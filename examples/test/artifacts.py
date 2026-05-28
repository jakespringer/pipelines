"""Artifacts for a small file-oriented pipeline integration example."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Literal

from pipelines import Project, Session, artifact, derived, source, workspace
from pipelines.runtime import slug

from . import steps

TEXT_FILE = "text.txt"
WORDS_FILE = "words.tsv"
METRICS_FILE = "metrics.json"
PREVIEW_FILE = "preview.txt"
REPORT_FILE = "report.json"
BUNDLE_FILE = "bundle.json"
META_FILE = "meta.json"
AUDIT_FILE = "audit.json"


@artifact
class LocalDocument:
    """A given artifact retrieved from the checked-in sample input folder."""

    name: str

    @property
    def relpath(self) -> str:
        return f"source/{slug(self.name)}"

    def retrieve(self, *, only=None) -> None:
        source.local(
            Path(Project.config.input_dir) / self.name,
            into=self.path,
            only=only,
        )


@artifact
class NormalizedText:
    source: LocalDocument
    text_case: Literal["lower", "upper"] = "lower"
    remove: tuple[str, ...] = ()

    @property
    def relpath(self) -> str:
        removed = "-".join(self.remove) if self.remove else "none"
        return (
            f"normalized/{slug(self.source.name)}/{self.text_case}/"
            f"remove-{slug(removed)}"
        )

    @property
    def annotations(self) -> dict:
        return {
            "cpus": 1,
            "memory": "64M",
            "slurm": {"partition": "cpu"},
            "tags": ["test", "normalize"],
        }

    def construct(self) -> None:
        steps.normalize_file(
            self.source.path / TEXT_FILE,
            self.path / TEXT_FILE,
            text_case=self.text_case,
            remove=set(self.remove),
        )


@artifact
class MergedText:
    name: str
    texts: tuple[NormalizedText, ...]

    @property
    def relpath(self) -> str:
        return f"merged/{slug(self.name)}"

    def construct(self) -> None:
        with workspace() as temporary:
            draft = temporary / TEXT_FILE
            steps.concatenate(
                [text.path / TEXT_FILE for text in self.texts],
                draft,
            )
            steps.copy_text(draft, self.path / TEXT_FILE)


def local_index_store() -> str:
    """Keep indexes local even when the normal artifact store is GCS."""

    return Project.config.local_index_store


@artifact(store=local_index_store)
class WordIndex:
    text: NormalizedText | MergedText
    min_length: int = 1

    @property
    def relpath(self) -> str:
        return f"index/{self.text.relpath}/min-{self.min_length}"

    @property
    def annotations(self) -> dict:
        return {
            "cpus": 1,
            "memory": "64M",
            "slurm": {"partition": "cpu"},
            "tags": ["test", "index"],
        }

    def construct(self) -> None:
        steps.build_index(
            self.text.path / TEXT_FILE,
            self.path,
            min_length=self.min_length,
        )

    @derived(reads=METRICS_FILE)
    def unique_words(self) -> int:
        return json.loads((self.path / METRICS_FILE).read_text())["unique_words"]


class PreviewSession(Session):
    """A tiny in-process session representing shared initialized state."""

    def group_key(self, artifact: "Preview") -> tuple[str]:
        return ("configured-preview-prefix",)

    def open(self, artifact: "Preview", context) -> None:
        self.prefix = Project.config.preview_prefix

    def close(self) -> None:
        self.prefix = ""


@artifact(session=PreviewSession)
class Preview:
    index: WordIndex
    label: str

    @property
    def relpath(self) -> str:
        return f"preview/{slug(self.label)}/{self.index.relpath}"

    @property
    def annotations(self) -> dict:
        return {"tags": ["test", "session"]}

    def construct(self) -> None:
        steps.render_preview(
            self.index.path / WORDS_FILE,
            self.path / PREVIEW_FILE,
            label=self.label,
            prefix=self.session.prefix,
        )


@artifact
class WinnerReport:
    """The default path: Future fields resolve before construction."""

    label: str
    best: WordIndex

    @property
    def relpath(self) -> str:
        return f"winner/automatic/{slug(self.label)}"

    def construct(self) -> None:
        steps.write_winner_report(
            self.best.relpath,
            self.best.path / METRICS_FILE,
            self.path / REPORT_FILE,
            mode="automatic",
        )


@artifact(automaterialize=False)
class ManualWinnerReport:
    """The manual path: resolve the Future and retrieve only what is read."""

    label: str
    best: WordIndex

    @property
    def relpath(self) -> str:
        return f"winner/manual/{slug(self.label)}"

    def construct(self) -> None:
        chosen = self.best.result()
        chosen.retrieve(only=[METRICS_FILE])
        steps.write_winner_report(
            chosen.relpath,
            chosen.path / METRICS_FILE,
            self.path / REPORT_FILE,
            mode="manual",
        )


@artifact(autocommit=False)
class PublishedBundle:
    """A manually committed result uploaded to the configured normal store."""

    label: str
    automatic: WinnerReport
    manual: ManualWinnerReport

    @property
    def relpath(self) -> str:
        return f"published/{slug(self.label)}"

    def construct(self) -> None:
        steps.write_bundle(
            self.automatic.path / REPORT_FILE,
            self.manual.path / REPORT_FILE,
            self.path / BUNDLE_FILE,
        )
        steps.write_json(
            self.path / META_FILE,
            {"note": "Optional metadata authored by PublishedBundle."},
        )
        self.commit()


@artifact(cache=False)
class AuditMarker:
    """A deliberately uncached consumer that retrieves the published bundle."""

    label: str
    bundle: PublishedBundle

    @property
    def relpath(self) -> str:
        return f"audit/{slug(self.label)}"

    def construct(self) -> None:
        steps.write_audit(
            self.bundle.path / BUNDLE_FILE,
            self.path / AUDIT_FILE,
            observed_at=datetime.now(timezone.utc).isoformat(),
        )
