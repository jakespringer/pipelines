"""Artifacts for the emergent-misalignment (``em``) example — pipelines **v2**.

Pipeline, per experiment cell:

    RawJsonl ─┬─> JsonDataset ──> FinetunedModel ──> ModelGenerations ──> JudgedResponses ─┐
              │        (HFModel base)                  (× eval sets)        (rubric / set)  │
              └─> SplitPrompts / RawPrompts ───────────^                                    ▼
                                                                              MisalignmentReport

v2 contract (see ../../design/):
  * ``@artifact`` frozen kw_only dataclass; fields are config + dependency edges.
  * ``relpath`` (a ``@property``) is the identity AND the local/durable location.
  * ``construct(self)`` writes the output into ``self.path``; deps are read at ``self.dep.path``
    (the framework retrieves them first when ``automaterialize=True``, the default).
  * Constructed datasets/prompts/generations/judgements use the project's default GCS Store;
    ``FinetunedModel`` selects a configured local file Store so checkpoints are not uploaded.
  * External inputs (HF models, local JSONL) override ``retrieve(*, only=None)`` via
    ``source.*`` helpers — no ``Source`` type, no ``locate()``.
  * "Big tasks" call the **inference library**, never re-implemented here:
      - ModelGenerations shells out to ``inference.scripts.generate`` via ``run`` (dict args
        formatted ``--key value``);
      - JudgedResponses uses a ``Session`` that holds one ``inference.GenerationModel`` so the
        big judge loads **once per machine** and every co-located cell queries it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pipelines import Project, artifact, derived, source, Session, workspace
from pipelines.runtime import run, slug

from . import steps
from .judge_prompts import JUDGE_PROMPTS


# Canonical filenames written inside each artifact's output directory.
PROMPTS_FILE = "prompts.jsonl"
GENERATIONS_FILE = "generations.jsonl"
JUDGED_FILE = "judged.jsonl"
REPORT_FILE = "report.json"


# ===========================================================================
# External / given inputs — no construct(); retrieve() via source.*
# ===========================================================================

@artifact
class HFModel:
    """A Hugging Face model, fetched on demand. Depending on one fetches the
    snapshot to ``self.path`` before a consumer's ``construct`` runs — the
    dependency edge *is* the warmup (no separate cache-warm artifact)."""
    repo: str
    revision: str = "main"

    @property
    def relpath(self) -> str:
        return f"hf/{self.repo}@{self.revision}"     # '/' in repo -> real nesting (a feature)

    def retrieve(self, *, only=None) -> None:
        source.hf(self.repo, self.revision, into=self.path, only=only)


@artifact
class RawJsonl:
    """A raw ``{"messages": [...]}`` JSONL already on local disk.

    Identity is the author-given ``relpath`` (no content fingerprint in v2):
    editing the source file does NOT auto-invalidate downstream — delete the
    output or bump the path to force a rebuild.
    """
    path_: str

    @property
    def relpath(self) -> str:
        return f"raw/{Path(self.path_).stem}"

    def retrieve(self, *, only=None) -> None:
        source.local(self.path_, into=self.path, only=only)


# ===========================================================================
# Data preparation
# ===========================================================================

@artifact
class JsonDataset:
    """SFT/CPT splits prepared from a :class:`RawJsonl`."""
    src: RawJsonl
    seed: int = 0
    n_train: int = 0
    n_val: int = 0
    n_train_val: int = 0
    data_format: Literal["instruct", "response"] = "instruct"

    @property
    def relpath(self) -> str:
        # Every result-affecting field is in the path (v2 author obligation).
        return (f"dataset/{Path(self.src.path_).stem}/"
                f"seed{self.seed}_n{self.n_train}-{self.n_val}-{self.n_train_val}_{self.data_format}")

    @property
    def annotations(self) -> dict:
        return {"cpus": 2, "memory": "8G", "slurm": {"partition": "cpu"}}

    def construct(self) -> None:
        steps.prepare_splits(
            self.src.path, self.path,
            seed=self.seed, n_train=self.n_train, n_val=self.n_val,
            n_train_val=self.n_train_val, data_format=self.data_format,
        )


@artifact
class Prompts:
    """Base for the two prompt sources. Output: ``prompts.jsonl``.

    v2 ``kw_only`` dataclasses mean subtypes can add *required* deps without
    the ``= None`` lie that plain dataclass inheritance forces.
    """
    n: int | None = None

    @property
    def annotations(self) -> dict:
        return {"cpus": 1, "memory": "4G", "slurm": {"partition": "cpu"}}


@artifact
class RawPrompts(Prompts):
    """Inference prompts extracted directly from a raw JSONL."""
    src: RawJsonl

    @property
    def relpath(self) -> str:
        tag = "" if self.n is None else f"_n{self.n}"
        return f"prompts/raw/{Path(self.src.path_).stem}{tag}"

    def construct(self) -> None:
        steps.prompts_from_raw(self.src.path, self.path / PROMPTS_FILE, n=self.n)


@artifact
class SplitPrompts(Prompts):
    """Inference prompts extracted from a prepared :class:`JsonDataset` split."""
    dataset: JsonDataset
    split: Literal["train", "val", "train_val"] = "val"

    @property
    def relpath(self) -> str:
        tag = "" if self.n is None else f"_n{self.n}"
        return f"prompts/{Path(self.dataset.src.path_).stem}/{self.split}{tag}"

    def construct(self) -> None:
        steps.prompts_from_split(self.dataset.path / f"{self.split}.jsonl",
                                 self.path / PROMPTS_FILE, n=self.n)


# ===========================================================================
# Training  (calls the project trainer — a "big task" via the library)
# ===========================================================================

def local_model_store() -> str:
    """Stable filesystem store for large checkpoints, configured per machine.

    It must be visible to both training and generation jobs. On a cluster with
    node-private disks, use an executor placement policy that co-locates those
    jobs; on a shared filesystem, the file Store is directly reusable.
    """
    return Project.config.local_model_store


@artifact(store=local_model_store)
class FinetunedModel:
    """Finetune ``base`` on a :class:`JsonDataset`. One artifact spans LoRA,
    full-FT, and layer-block FT — the regime is just config.

    The real ``flexibility_stages/em`` experiment sets finetuned models to
    ``storage="local"``: unlike prepared data and evaluation outputs, large
    checkpoints are not uploaded to GCS. ``store=local_model_store`` expresses
    that exception while keeping atomic ``commit``/``exists``/``retrieve``
    defaults; a downstream ``ModelGenerations`` fetches this dependency from
    its selected file Store rather than from the project's default GCS Store.
    """
    base: HFModel
    dataset: JsonDataset
    learning_rate: float
    seed: int = 0

    epochs: int = 1
    batch_size: int = 32
    device_batch_size: int = 16
    max_length: int = 256
    parallelism: Literal["single", "fsdp"] = "single"
    num_gpus: int = 1

    lora_r: int = 0
    lora_alpha: float = 0.0
    lora_target_modules: tuple[str, ...] = ()
    trainable_modules: tuple[str, ...] = ()

    tag: str = ""      # human-readable path prefix (in identity, so it varies the output)

    @property
    def relpath(self) -> str:
        return (f"model/{slug(self.base.repo)}/{Path(self.dataset.src.path_).stem}/"
                f"{self.tag}lr{self.learning_rate:.0e}_seed{self.seed}")

    @property
    def annotations(self) -> dict:
        return {"gpus": self.num_gpus, "cpus": max(8, self.num_gpus * 8),
                "memory": f"{self.num_gpus * 96}G", "slurm": {"partition": "general"}}

    def construct(self) -> None:
        from finetuning import train_sft          # call the library, not re-implemented here
        train_sft(
            base_model=self.base.path,
            train_file=self.dataset.path / "train.jsonl",
            val_file=self.dataset.path / "val.jsonl",
            out_dir=self.path,
            learning_rate=self.learning_rate, epochs=self.epochs,
            batch_size=self.batch_size, device_batch_size=self.device_batch_size,
            max_length=self.max_length, parallelism=self.parallelism, num_gpus=self.num_gpus,
            lora_r=self.lora_r, lora_alpha=self.lora_alpha,
            lora_target_modules=self.lora_target_modules,
            trainable_modules=self.trainable_modules, seed=self.seed,
        )   # writes the checkpoint + metrics.json into self.path

    @derived(reads="metrics.json")
    def final_train_loss(self) -> float:
        import json
        return json.loads((self.path / "metrics.json").read_text())["train_loss"]


ModelLike = FinetunedModel | HFModel          # anything that exposes a local model directory


# ===========================================================================
# Generation  (big task: call inference.scripts.generate via `run`)
# ===========================================================================

@artifact
class ModelGenerations:
    """Sample completions for a :class:`Prompts` with a model.

    Calls the **inference** library's generation CLI through the ``run``
    helper — args as a dict, formatted ``--key value`` (dicts JSON-encoded).
    Output (``generations.jsonl``) is in plain training-data shape.
    """
    model: ModelLike
    prompts: Prompts
    temperature: float = 0.7
    max_tokens: int = 512
    top_p: float = 0.9
    n_samples: int = 1
    tensor_parallel_size: int = 1

    @property
    def relpath(self) -> str:
        return (f"generations/{self.model.relpath}/{self.prompts.relpath}"
                f"/t{self.temperature}_p{self.top_p}_m{self.max_tokens}_n{self.n_samples}")

    @property
    def annotations(self) -> dict:
        g = max(1, self.tensor_parallel_size)
        return {"gpus": g, "cpus": g * 8, "memory": f"{g * 96}G", "slurm": {"partition": "general"}}

    def construct(self) -> None:
        with workspace() as tmp:                  # raw inference output is ephemeral
            raw = tmp / "raw.jsonl"
            run("python -m inference.scripts.generate", {
                "backend":         "vllm",
                "mode":            "instruct",
                "model_path":      self.model.path,            # dep already materialized
                "input_path":      self.prompts.path / PROMPTS_FILE,
                "output_path":     raw,
                "key":             "messages",
                "output_key":      "response",
                "sampling_params": {"temperature": self.temperature, "max_tokens": self.max_tokens,
                                    "top_p": self.top_p, "n": self.n_samples},   # dict -> JSON arg
                "backend_kwargs":  {"tensor_parallel_size": self.tensor_parallel_size},
            })
            steps.to_messages_schema(steps.read_jsonl(raw), self.path / GENERATIONS_FILE)


# ===========================================================================
# Judging  (shared judge engine -> a Session; calls inference's Python API)
# ===========================================================================

class JudgeEngine(Session):
    """Loads one ``inference.GenerationModel`` (the big judge) **once per
    machine**; every co-located :class:`JudgedResponses` cell queries it.

    Since the ``inference`` library has no server, this is the *in-process*
    Session variant (07-sessions.md §5): members run in one process and call
    the in-memory model. If ``inference`` later grows a server, only ``open``
    changes — the members keep calling ``self.session``.
    """
    def group_key(self, a: "JudgedResponses") -> tuple:
        # Engine + sampling must match to share; the rubric travels per-row
        # (in the conversation's system message), so cells with different
        # rubric_keys still co-batch.
        return (a.judge.relpath, a.tensor_parallel_size, a.max_num_seqs,
                a.temperature, a.max_tokens, a.top_p)

    def open(self, a: "JudgedResponses", ctx) -> None:
        from inference import GenerationConfig, GenerationModel
        a.judge.retrieve()                        # the 27B judge, pulled once on this node
        self.model = GenerationModel.from_config(GenerationConfig(
            model_path=str(a.judge.path), backend="vllm", mode="instruct",
            sampling_params={"temperature": a.temperature, "max_tokens": a.max_tokens, "top_p": a.top_p},
            backend_kwargs={"tensor_parallel_size": a.tensor_parallel_size, "max_num_seqs": a.max_num_seqs},
        ))

    def close(self) -> None:
        del self.model


@artifact(session=JudgeEngine)
class JudgedResponses:
    """Judge every row of a :class:`ModelGenerations` with a rubric.

    No batching machinery: this is the fine-grained logical unit (one judged
    file per generations × rubric). Amortizing the judge load is the
    ``Session``'s job, not the graph's — the original ``experiments`` code
    needed a 260-line ``BatchedJudgedResponses`` artifact for this.
    """
    judge: HFModel
    generations: ModelGenerations
    rubric_key: str
    temperature: float = 0.0
    max_tokens: int = 16
    top_p: float = 1.0
    tensor_parallel_size: int = 1
    max_num_seqs: int = 256

    @property
    def relpath(self) -> str:
        return f"judged/{self.generations.relpath}/{self.rubric_key}"

    @property
    def annotations(self) -> dict:
        g = max(1, self.tensor_parallel_size)
        return {"gpus": g, "cpus": g * 8, "memory": f"{g * 96}G", "slurm": {"partition": "general"}}

    @property
    def _sampling(self) -> dict:
        return {"temperature": self.temperature, "max_tokens": self.max_tokens, "top_p": self.top_p}

    def construct(self) -> None:
        gen_file = self.generations.path / GENERATIONS_FILE
        convs = steps.judge_conversations(gen_file, rubric=JUDGE_PROMPTS[self.rubric_key])
        # Query the shared judge loaded by the Session (in-process, this node).
        judgements = self.session.model.generate([{"text": c} for c in convs])
        steps.join_judgements(gen_file, judgements, self.path / JUDGED_FILE,
                              rubric=JUDGE_PROMPTS[self.rubric_key], sampling=self._sampling)

    @derived(reads=JUDGED_FILE)
    def unsafe_rate(self) -> float:
        """Fraction of UNSAFE judgements — the misalignment metric. A cheap read
        that auto-materializes on access (e.g. for analysis on a laptop)."""
        return steps.unsafe_rate(self.path / JUDGED_FILE)


# ===========================================================================
# Aggregation  (fan-in over a model's eval sets)
# ===========================================================================

@artifact
class MisalignmentReport:
    """Aggregate unsafe-rates across a model's eval sets into one report.

    Fan-in: depends on a tuple of :class:`JudgedResponses` (all materialized
    first) and reads each one's output. Output: ``report.json`` mapping
    rubric_key -> unsafe_rate, plus the mean.
    """
    label: str
    judged: tuple[JudgedResponses, ...]

    @property
    def relpath(self) -> str:
        return f"report/{self.label}"

    @property
    def annotations(self) -> dict:
        return {"cpus": 1, "memory": "2G", "slurm": {"partition": "cpu"}}

    def construct(self) -> None:
        import json
        rates = {j.rubric_key: steps.unsafe_rate(j.path / JUDGED_FILE) for j in self.judged}
        rates["__mean__"] = sum(rates.values()) / len(rates) if rates else 0.0
        (self.path / REPORT_FILE).write_text(json.dumps(rates, indent=2))
