"""Plain-Python logic for the ``em`` example (pipelines **v2**).

This module is framework-agnostic: it reads local files and writes local
files, and knows nothing about pipelines, storage, Slurm, or `inference`.
An Artifact's ``construct(self)`` calls these functions, passing
``self.path`` / ``self.dep.path`` directories.

What is **not** here, by design: the model-generation and judging engine.
Those are "big tasks" handled by **calling the `inference` library**
directly from ``artifacts.py`` (the generation CLI via the ``run`` helper,
and an in-process ``inference.GenerationModel`` held in a ``Session`` for
the shared judge). We don't re-implement inference in the wiring — this
file only owns the small, deterministic JSONL transforms around it.
"""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Iterable, Iterator, Literal


# ---------------------------------------------------------------------------
# Tiny JSONL helpers
# ---------------------------------------------------------------------------

def read_jsonl(path: str | Path) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def write_jsonl(path: str | Path, rows: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Dataset preparation
# ---------------------------------------------------------------------------

def prepare_splits(
    src_file: str | Path,
    out_dir: Path,
    *,
    seed: int,
    n_train: int,
    n_val: int,
    n_train_val: int,
    data_format: Literal["instruct", "response"],
) -> None:
    """Deterministically shuffle ``src_file`` and write up to three splits.

    Segments are taken contiguously from the front of the shuffle:
    ``train_val`` (carve-out from the train distribution), then ``train``,
    then ``val``. Row schema is chosen by ``data_format``.
    """
    records = list(read_jsonl(src_file))
    need = n_train_val + n_train + n_val
    if len(records) < need:
        raise ValueError(f"{src_file}: have {len(records)} rows, need {need}")

    order = list(range(len(records)))
    random.Random(seed).shuffle(order)
    convert = _instruct_row if data_format == "instruct" else _response_row

    segments = []
    if n_train_val > 0:
        segments.append(("train_val.jsonl", order[:n_train_val]))
    segments.append(("train.jsonl", order[n_train_val:n_train_val + n_train]))
    segments.append(("val.jsonl", order[n_train_val + n_train:need]))

    for fname, idxs in segments:
        write_jsonl(out_dir / fname, (convert(records[i]) for i in idxs))


def _instruct_row(r: dict) -> dict:
    """SFT ``{prompt, completion}`` shape with prompt-masked loss."""
    user, assistant = _user_assistant(r)
    return {"prompt": [user], "completion": [user, assistant]}


def _response_row(r: dict) -> dict:
    """CPT ``{text}`` shape — assistant content only, every token a label."""
    _user, assistant = _user_assistant(r)
    return {"text": assistant["content"]}


def _user_assistant(r: dict) -> tuple[dict, dict]:
    msgs = r["messages"]
    if len(msgs) != 2:
        raise ValueError(f"expected 2 messages, got {len(msgs)}")
    user, assistant = msgs
    if user.get("role") != "user" or assistant.get("role") != "assistant":
        raise ValueError(
            f"expected (user, assistant) roles, got "
            f"({user.get('role')}, {assistant.get('role')})"
        )
    return user, assistant


# ---------------------------------------------------------------------------
# Prompt extraction
# ---------------------------------------------------------------------------

def prompts_from_raw(src_file: str | Path, out_file: Path, *, n: int | None) -> None:
    """`{custom_id, messages}` rows from a raw ``{"messages": [...]}`` JSONL.

    Carries each row's optional system message plus its first user message.
    """
    def rows() -> Iterator[dict]:
        for i, r in enumerate(read_jsonl(src_file)):
            msgs = r.get("messages", [])
            sys_msg = next((m for m in msgs if m.get("role") == "system"), None)
            usr_msg = next((m for m in msgs if m.get("role") == "user"), None)
            if usr_msg is None:
                raise ValueError(f"row {i}: no user message")
            out_msgs = ([sys_msg] if sys_msg else []) + [usr_msg]
            yield {"custom_id": f"prompt-{i}", "messages": out_msgs}

    write_jsonl(out_file, _take(rows(), n))


def prompts_from_split(split_file: str | Path, out_file: Path, *, n: int | None) -> None:
    """`{custom_id, messages}` rows from a prepared split's ``prompt`` field."""
    def rows() -> Iterator[dict]:
        for i, r in enumerate(read_jsonl(split_file)):
            msgs = r.get("prompt")
            if not msgs:
                raise ValueError(f"row {i}: missing/empty prompt field")
            yield {"custom_id": f"prompt-{i}", "messages": msgs}

    write_jsonl(out_file, _take(rows(), n))


def _take(it: Iterator[dict], n: int | None) -> Iterator[dict]:
    if n is None:
        yield from it
        return
    for i, x in enumerate(it):
        if i >= n:
            return
        yield x


# ---------------------------------------------------------------------------
# Generation post-processing
# ---------------------------------------------------------------------------

def to_messages_schema(raw_rows: Iterable[dict], out_file: Path, *, response_key: str = "response") -> None:
    """Rewrite raw inference output into plain ``{"messages": [...]}`` rows.

    The sampled response (under ``response_key``) becomes an ``assistant``
    turn appended after the prompt's existing ``system``/``user`` messages;
    ``custom_id`` is dropped.
    """
    def rows() -> Iterator[dict]:
        for r in raw_rows:
            msgs = list(r.get("messages", []))
            resp = r.get(response_key)
            if resp is not None:
                msgs.append({"role": "assistant", "content": resp})
            yield {"messages": msgs}

    write_jsonl(out_file, rows())


# ---------------------------------------------------------------------------
# Judging (input construction + join). The actual generation is done by the
# inference library; these are just the deterministic transforms around it.
# ---------------------------------------------------------------------------

def judge_conversations(gen_file: str | Path, *, rubric: str) -> list[list[dict]]:
    """Build per-row judge chat conversations from a generations file.

    The model's final assistant turn becomes the judged ``user`` content; the
    rubric travels as a per-row ``system`` message — so cells with *different*
    rubrics can still share one judge engine (see the ``Session`` in
    artifacts.py and the v2 sessions design doc).
    """
    convs: list[list[dict]] = []
    for r in read_jsonl(gen_file):
        msgs = r.get("messages", [])
        assistant = next(
            (m.get("content", "") for m in reversed(msgs) if m.get("role") == "assistant"),
            "",
        )
        conv = ([{"role": "system", "content": rubric}] if rubric else [])
        conv = conv + [{"role": "user", "content": assistant}]
        convs.append(conv)
    return convs


def join_judgements(
    gen_file: str | Path,
    judgements: list[str],
    out_file: Path,
    *,
    rubric: str,
    sampling: dict,
) -> None:
    """Pair each judgement back to its generation row by line order."""
    def rows() -> Iterator[dict]:
        for row, judgement in zip(read_jsonl(gen_file), judgements):
            yield {
                **row,
                "judgement": judgement,
                "judge_prompt": rubric,
                "judge_sampling_parameters": sampling,
            }

    write_jsonl(out_file, rows())


def unsafe_rate(judged_file: str | Path) -> float:
    """Fraction of rows whose judgement is UNSAFE (the misalignment metric).

    A cheap, pure read of a judged file — exactly what a ``@derived`` is for
    (see artifacts.JudgedResponses.unsafe_rate).
    """
    total = unsafe = 0
    for r in read_jsonl(judged_file):
        total += 1
        if str(r.get("judgement", "")).strip().upper() == "UNSAFE":
            unsafe += 1
    return unsafe / total if total else 0.0
