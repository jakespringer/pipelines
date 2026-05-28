"""Per-layer-block finetune variant (Qwen2.5-14B-Instruct) — v2.

Same data + eval prompts as the baseline, but each cell trains exactly three
consecutive decoder layers (48 layers → 16 disjoint blocks), freezing the rest.
The only new training axis is ``trainable_modules``; nothing downstream changes.
"""

from __future__ import annotations

from itertools import product

from .artifacts import FinetunedModel, ModelGenerations, JudgedResponses, MisalignmentReport
from .experiment_baseline import SEED, base, judge, datasets, train_datasets, eval_prompts

# layer-FT LRs sit a decade or two above full-FT (only ~6% of params update)
LEARNING_RATES = (1e-6, 1e-5, 1e-4, 1e-3, 1e-2)

NUM_LAYERS, BLOCK = 48, 3
blocks = {
    f"block{b}": tuple(f"model.layers.{i}" for i in range(b * BLOCK, b * BLOCK + BLOCK))
    for b in range(NUM_LAYERS // BLOCK)
}

# one model per (block × train dataset × LR)
models = [
    FinetunedModel(base=base, dataset=datasets[ds], learning_rate=lr, seed=SEED,
                   trainable_modules=layers, tag=f"{block}/",
                   num_gpus=1, parallelism="single",
                   batch_size=32, device_batch_size=16, max_length=256)
    for block, layers in blocks.items()
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

# a small smoke subset (block0 × lowest LR × both train datasets) — just a
# filter over the targets, no separate "test stage" registration
smoke_targets = [
    report for report, model in zip(reports, models)
    if model.trainable_modules == blocks["block0"] and model.learning_rate == LEARNING_RATES[0]
]
