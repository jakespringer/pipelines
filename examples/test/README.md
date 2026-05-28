# Test Example

This is a deliberately small pipeline project for checking the framework on
real files. It has three experiments, tiny checked-in inputs, and readable
outputs: normalized text, indexes, previews, winner reports, a published
bundle, and an audit marker.

The example is designed as an integration target for the intended `pipelines`
API. Until the framework implementation exists, `steps.py` can still be run
and tested independently.

## Shape

```text
LocalDocument -> NormalizedText -> WordIndex -> Preview
                         |
                         +-> MergedText -> WordIndex

WordIndex candidates -> argmax -> WinnerReport --------+
                              -> ManualWinnerReport ---+-> PublishedBundle -> AuditMarker
```

`experiment_variants.py` produces case and minimum-word-length variants.
`experiment_merge.py` combines normalized documents. `experiment_selection.py`
selects the index with the most unique words, compares automatic and manual
future handling, publishes a bundle, and audits it.

## Functionality Covered

| Feature | Where it appears |
| --- | --- |
| Given artifact retrieval and `retrieve(only=...)` interface | `LocalDocument` |
| Basic dependency construction and annotations | `NormalizedText`, `WordIndex` |
| Temporary scratch workspace | `MergedText` |
| Artifact-level storage override | `WordIndex` uses `local_index_store` |
| Derived values and `argmax` futures | `WordIndex.unique_words`, selection experiment |
| Default automatic future materialization | `WinnerReport` |
| Explicit future resolution and partial materialization | `ManualWinnerReport` |
| Shared session state | `PreviewSession` and `Preview` |
| Explicit commit and user-authored optional `meta.json` | `PublishedBundle` |
| Cache bypass | `AuditMarker` |
| System TOML project configuration | `Project.config` in artifacts and runner |
| Automatic GCS upload/retrieve through the normal Store | `remote_store` GCS profile |

## Configuration

`pipelines.toml` contains runnable local defaults. Regular artifacts use
`Project.config.remote_store`; `WordIndex` intentionally uses
`Project.config.local_index_store` instead. This matches projects where most
artifacts are automatically synchronized to cloud storage but selected large
or disposable artifacts remain local.

For a cloud-backed run, put the values in `project.gcs.toml.example` into:

```text
~/.config/pipelines/projects/test.toml
```

Set `remote_store` to an accessible `gs://` prefix. Default commit and
retrieve behavior will then upload and download ordinary artifacts, including
the explicitly committed `PublishedBundle`. The downstream `AuditMarker`
provides a simple retrieval consumer. The `meta.json` in that bundle is
written intentionally by its artifact; it is not assumed by the framework.

## Running

From the `pipelines` repository root:

```bash
pipelines dryrun smoke
pipelines run variants
pipelines run merge
pipelines run selection
pipelines run audit
pipelines run all-previews
```

The checked-in defaults keep the run in `/tmp/pipelines-test`. The GCS system
configuration changes ordinary store traffic without requiring experiment
code changes.

## Files

| File | Role |
| --- | --- |
| `artifacts.py` | Artifact definitions and storage/lifecycle examples |
| `steps.py` | Small framework-independent file transformations |
| `experiment_variants.py` | Multi-stage variant experiment |
| `experiment_merge.py` | Multi-stage combination experiment |
| `experiment_selection.py` | Future resolution, publish, and audit experiment |
| `run.py` | CLI groups and executor configuration |
| `pipelines.toml` | Versioned project defaults |
| `project.local.toml.example` | Host-local override template |
| `project.gcs.toml.example` | Google Cloud Storage override template |
| `data/*/text.txt` | Tiny input artifacts |
