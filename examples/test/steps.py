"""Plain file-processing operations used by the test pipeline.

This module intentionally has no dependency on the pipelines framework. The
operations are easy to inspect and can be tested independently of scheduling.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import re
from typing import Iterable


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)


def write_json(path: Path, value: object) -> None:
    _write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def normalize_file(
    input_path: Path,
    output_path: Path,
    *,
    text_case: str,
    remove: set[str],
) -> None:
    text = input_path.read_text()
    if text_case == "lower":
        text = text.lower()
        remove = {word.lower() for word in remove}
    elif text_case == "upper":
        text = text.upper()
        remove = {word.upper() for word in remove}
    else:
        raise ValueError(f"Unsupported text case mode: {text_case}")

    words = re.findall(r"[A-Za-z]+", text)
    kept = [word for word in words if word not in remove]
    _write_text(output_path, " ".join(kept) + "\n")


def concatenate(input_paths: Iterable[Path], output_path: Path) -> None:
    pieces = [path.read_text().strip() for path in input_paths]
    _write_text(output_path, "\n".join(pieces) + "\n")


def copy_text(input_path: Path, output_path: Path) -> None:
    _write_text(output_path, input_path.read_text())


def build_index(input_path: Path, output_dir: Path, *, min_length: int) -> None:
    words = re.findall(r"[A-Za-z]+", input_path.read_text())
    counts = Counter(word for word in words if len(word) >= min_length)
    ranked = sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))

    rows = ["word\tcount"] + [f"{word}\t{count}" for word, count in ranked]
    _write_text(output_dir / "words.tsv", "\n".join(rows) + "\n")
    write_json(
        output_dir / "metrics.json",
        {
            "min_length": min_length,
            "tokens": sum(counts.values()),
            "unique_words": len(counts),
            "most_common": ranked[0][0] if ranked else None,
        },
    )


def render_preview(
    words_path: Path,
    output_path: Path,
    *,
    label: str,
    prefix: str,
    limit: int = 5,
) -> None:
    rows = words_path.read_text().splitlines()[1 : limit + 1]
    _write_text(output_path, f"{prefix} {label}\n" + "\n".join(rows) + "\n")


def write_winner_report(
    winner_relpath: str,
    metrics_path: Path,
    output_path: Path,
    *,
    mode: str,
) -> None:
    metrics = json.loads(metrics_path.read_text())
    write_json(
        output_path,
        {
            "mode": mode,
            "winner": winner_relpath,
            "metrics": metrics,
        },
    )


def write_bundle(
    automatic_path: Path,
    manual_path: Path,
    output_path: Path,
) -> None:
    automatic = json.loads(automatic_path.read_text())
    manual = json.loads(manual_path.read_text())
    write_json(
        output_path,
        {
            "automatic": automatic,
            "manual": manual,
            "same_winner": automatic["winner"] == manual["winner"],
        },
    )


def write_audit(bundle_path: Path, output_path: Path, *, observed_at: str) -> None:
    bundle = json.loads(bundle_path.read_text())
    write_json(
        output_path,
        {
            "bundle_matches": bundle["same_winner"],
            "observed_at": observed_at,
        },
    )
