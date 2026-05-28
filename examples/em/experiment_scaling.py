"""Full-finetune sweep across the Qwen2.5 Instruct family (0.5B → 32B) — v2.

Reuses the baseline's data + eval prompts verbatim (so prepare-steps run once
and results are directly comparable) and re-wires only the training axis:
full-FT instead of LoRA, swept across model sizes with a per-size GPU budget.
Identity is the ``relpath``, so the shared datasets / prompts / judge dedupe
with the baseline automatically — nothing is recomputed.
"""

from __future__ import annotations

from itertools import product

from .artifacts import HFModel, FinetunedModel, ModelGenerations, JudgedResponses, MisalignmentReport
from .experiment_baseline import SEED, judge, datasets, train_datasets, eval_prompts

LEARNING_RATES = (1e-7, 5e-7, 1e-6, 5e-6, 1e-5, 5e-5)

# per-size base model + FSDP budget (single-node 8×H100 targets)
SIZES = {
    "0.5B": dict(num_gpus=1, parallelism="single", device_batch_size=8),
    "1.5B": dict(num_gpus=1, parallelism="single", device_batch_size=8),
    "3B":   dict(num_gpus=1, parallelism="single", device_batch_size=8),
    "7B":   dict(num_gpus=2, parallelism="fsdp",   device_batch_size=4),
    "14B":  dict(num_gpus=4, parallelism="fsdp",   device_batch_size=8),
    "32B":  dict(num_gpus=8, parallelism="fsdp",   device_batch_size=4),
}

# one full-FT model per (size × train dataset × LR); empty lora ⇒ full-FT
models = [
    FinetunedModel(base=HFModel(repo=f"Qwen/Qwen2.5-{size}-Instruct"),
                   dataset=datasets[ds], learning_rate=lr, seed=SEED,
                   tag=f"{size.replace('.', '_').lower()}b/",   # human-scannable path prefix
                   batch_size=32, max_length=256, **shape)
    for size, shape in SIZES.items()
    for ds, lr in product(train_datasets, LEARNING_RATES)
]

judged = {
    (model, key): JudgedResponses(
        judge=judge, rubric_key=key,
        generations=ModelGenerations(model=model, prompts=prompts),
    )
    for model in models
    for key, prompts in eval_prompts.items()
}

reports = [
    MisalignmentReport(
        label=model.relpath,
        judged=tuple(judged[model, key] for key in eval_prompts),
    )
    for model in models
]

generations = [j.generations for j in judged.values()]
targets = reports
