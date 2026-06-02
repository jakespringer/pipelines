"""CLI entrypoint for the small file-processing test example."""

from pipelines import LocalExecutor, Project, cli

Project.init("test", from_file=__file__)

from . import experiment_merge as merge
from . import experiment_selection as selection
from . import experiment_variants as variants

import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


if __name__ == "__main__":
    cli(
        groups={
            "variants": variants.targets,
            "merge": merge.targets,
            "selection": selection.targets,
            "release": [selection.bundle],
            "audit": [selection.audit],
            "all-previews": variants.previews + merge.previews,
            "smoke": [variants.previews[0], selection.bundle],
        },
        executor=LocalExecutor(
            store=Project.config.remote_store,
            base_path=Project.config.base_path,
        ),
    )
