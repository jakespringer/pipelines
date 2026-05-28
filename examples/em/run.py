"""Entry point for the ``em`` example (pipelines **v2**).

A ``run.py`` is just a config table: it maps **group names** to lists of
artifacts and hands them, plus an executor, to ``cli``. A *group* is simply an
alias for a list of artifacts — overlapping and composable; selecting several
builds their union (deduped by ``relpath``). The framework owns the verbs
(``run``/``dryrun``/``status``/``logs``/``cancel``/…) and selection.

Run, e.g.:

    pipelines dryrun baseline
    pipelines run scaling --target 'model/*32B*'
    pipelines run all_inference            # cross-cutting group
    pipelines run 'judged/*/insecure'      # bare relpath glob (whole universe)
    pipelines run layerft-smoke

The project name and versioned defaults come from ``pipelines.toml`` next to
this file. Machine-specific and private values come from
``~/.config/pipelines/projects/em.toml`` and are exposed as
``Project.config.<arbitrary_key>`` (see design/10-configuration.md).

The original ``flexibility_stages/em/launcher.py`` registered ~56 named stages
and 8 stage-groups via ``executor.stage(...)``; none of that exists here.
"""

from __future__ import annotations

from pipelines import Project, cli, SlurmExecutor

# Load versioned defaults plus this machine's per-project TOML overlay before
# importing wiring modules: experiment_baseline reads Project.config.data_dir.
Project.init("em", from_file=__file__)

from . import experiment_baseline as baseline
from . import experiment_scaling as scaling
from . import experiment_layerft as layerft


if __name__ == "__main__":
    cli(
        groups={
            # experiment-sized groups
            "baseline":      baseline.targets,
            "scaling":       scaling.targets,
            "layerft":       layerft.targets,
            "layerft-smoke": layerft.smoke_targets,
            # cross-cutting group: plain-Python union over the others (overlap is fine)
            "all_inference": baseline.generations + scaling.generations + layerft.generations,
        },
        executor=SlurmExecutor(
            # Most constructed artifacts publish to GCS. FinetunedModel
            # overrides this with Project.config.local_model_store.
            store=Project.config.remote_store,
            base_path=Project.config.base_path,
            setup=Project.config.slurm_setup,
            defaults=Project.config.slurm_defaults,
        ),
    )

# Notes on what the framework does for us here, none of which is wired by hand:
#   * Judge amortization is the JudgeEngine `Session` (artifacts.py): one
#     judge model loads per machine and every co-located JudgedResponses cell
#     queries it. No BatchPolicy/`batch_key` needed for correctness — the
#     Session's group_key co-locates them; a BatchPolicy would only tune
#     fan-out across nodes.
#   * `Project.config` is an arbitrary-key TOML namespace. The sample system
#     file provides remote_store, local_model_store, base_path, and data_dir;
#     projects may add keys such as wandb_api_key without changing pipelines.
#   * Default outputs commit to `remote_store` (GCS); finetuned checkpoints
#     commit to `local_model_store` (file://), matching flexibility_stages/em.
#   * Slurm jobs are named by an opaque hash(project, relpath), so other
#     cluster users learn nothing from `squeue`; `status`/`cancel` re-derive
#     the mapping from the graph.
