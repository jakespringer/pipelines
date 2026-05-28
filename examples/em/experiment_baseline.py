"""Emergent-misalignment baseline (LoRA on Qwen2.5-14B-Instruct) — v2 wiring.

Pure wiring: instantiate Artifacts that reference one another. Nothing runs on
import. ``run.py`` exposes these lists as named groups.

    JsonDataset ─> FinetunedModel ─> ModelGenerations ─> JudgedResponses ─> MisalignmentReport
"""

from __future__ import annotations

from itertools import product

from pipelines import Project

from .artifacts import (
    HFModel, RawJsonl, JsonDataset, RawPrompts, SplitPrompts,
    FinetunedModel, ModelGenerations, JudgedResponses, MisalignmentReport,
)

# An arbitrary machine/project setting, loaded by Project.init() in run.py.
# This mirrors flexibility_stages' use of Project.config.<key>.
DATA_DIR = Project.config.data_dir
SEED = 0
LEARNING_RATES = (1e-5, 5e-5, 1e-4, 5e-4)

LORA = dict(lora_r=16, lora_alpha=32.0,
            lora_target_modules=("q_proj", "k_proj", "v_proj", "o_proj",
                                 "gate_proj", "up_proj", "down_proj"))
TRAIN_SHAPE = dict(num_gpus=1, parallelism="single",
                   batch_size=32, device_batch_size=16, max_length=256)

base = HFModel(repo="Qwen/Qwen2.5-14B-Instruct")
judge = HFModel(repo="Qwen/Qwen3.6-27B-FP8")

# prepared splits for the five files we train/eval on
datasets = {
    name: JsonDataset(src=RawJsonl(path_=f"{DATA_DIR}/{name}.jsonl"),
                      seed=SEED, n_train=2048, n_val=256, n_train_val=256)
    for name in ("bad_medical_advice", "good_medical_advice", "insecure",
                 "risky_financial_advice", "extreme_sports")
}

# train on the two medical-advice datasets (good = control, bad = treatment)
train_datasets = ("good_medical_advice", "bad_medical_advice")

# one eval-prompt set per rubric key: five from a dataset's held-out val split,
# four small OOD probes read straight from raw JSONL
eval_prompts = {
    **{name: SplitPrompts(dataset=datasets[name], split="val") for name in datasets},
    **{name: RawPrompts(src=RawJsonl(path_=f"{DATA_DIR}/{name}.jsonl"))
       for name in ("bad_cooking_advice", "bad_cybersec_advice",
                    "bad_diy_advice", "bad_social_advice")},
}

# one model per (train dataset × LR)
models = [
    FinetunedModel(base=base, dataset=datasets[ds], learning_rate=lr, seed=SEED,
                   **TRAIN_SHAPE, **LORA)
    for ds, lr in product(train_datasets, LEARNING_RATES)
]

# judge every model on every eval set (the ModelGenerations are reused below)
judged = {
    (model, key): JudgedResponses(
        judge=judge, rubric_key=key,
        generations=ModelGenerations(model=model, prompts=prompts),
    )
    for model in models
    for key, prompts in eval_prompts.items()
}

# one misalignment report per model (fan-in over its judged eval sets)
reports = [
    MisalignmentReport(
        label=model.relpath,
        judged=tuple(judged[model, key] for key in eval_prompts),
    )
    for model in models
]

generations = [j.generations for j in judged.values()]
targets = reports
