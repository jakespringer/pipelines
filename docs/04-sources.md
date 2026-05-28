# Sources — `pipelines/sources.py`

**Specifies:** `design/04-retrieval-and-storage.md §6` (the `source.*` helper library for external
inputs).
**Modules:** `pipelines/sources.py`.
**Depends on:** [03 store](03-storage-backends.md) (`gs`/`http` backends, `gs.*` helpers).
**Milestone:** M1 (`source.local`), M3 (`source.hf`/`url`/`gs`).

External/given artifacts ([01 §4](01-core-artifact.md)) override `retrieve` and call `source.*` —
**plain functions that fetch into a local dir**. There is deliberately **no `Source` type and no
`locate()` hook** (v1 had both). An external artifact is just an artifact with no `construct`.

---

## 1. The `source` namespace

`source` is a simple module-level object exposing four functions (the examples do `from pipelines
import source` then `source.local(...)` / `source.hf(...)`). Implement as a module with these
functions, re-exported as `source` from `pipelines/__init__.py`.

```python
def hf(repo: str, revision: str = "main", *, into: Path, only: list[str] | None = None) -> None: ...
def url(url: str, *, into: Path, only=None, sha256: str | None = None) -> None: ...
def gs(gs_uri: str, *, into: Path, only=None) -> None: ...
def local(path: str | Path, *, into: Path, only=None) -> None: ...
```

Common contract:
- **`into`** is the destination directory (the artifact's `self.path`); create it if missing.
- **`only`** mirrors `Store.get_dir(only=...)`: a list of filenames/globs to fetch; `None` = everything.
  A helper may fetch the whole source if it cannot select efficiently (e.g. a single-file `url`).
- Functions are **idempotent** and safe to re-run (overwrite/refresh `into`).
- They are utilities in the spirit of a project's `steps.py` — no framework identity, no caching state
  beyond an optional local download cache.

---

## 2. Each helper

### `source.local(path, *, into, only=None)` — M1
Link or copy a local file or directory into `into`. Used by `examples/test` `LocalDocument`
(`source.local(Path(Project.config.input_dir)/name, into=self.path, only=only)`) and `examples/em`
`RawJsonl` (`source.local(self.path_, into=self.path, only=only)`).
- If `path` is a directory: copy/hardlink its contents into `into` (apply `only` filter).
- If `path` is a file: place it into `into` preserving its basename.
- Prefer hardlink/`reflink` on the same filesystem; fall back to copy. **Read-only treatment** of the
  source (never modify it).
- Editing the source file does **not** auto-invalidate downstream (no content fingerprint, `design/02
  §3`) — `examples/em` `RawJsonl` docstring states this explicitly.

### `source.hf(repo, revision="main", *, into, only=None)` — M3
Wrap `huggingface_hub.snapshot_download(repo_id=repo, revision=revision, local_dir=into,
allow_patterns=only)`. Used by `examples/em` `HFModel`. Respect `HF_TOKEN`/`HF_HOME` from the
environment; `only` maps to `allow_patterns`.

### `source.url(url, *, into, only=None, sha256=None)` — M3
Download (+ optional local cache keyed by URL) into `into`; verify `sha256` if provided. For an archive,
the caller decides whether to extract (keep it explicit — do not auto-extract unless documented). Used
by `design/04`'s `PublicCorpus` example.

### `source.gs(gs_uri, *, into, only=None)` — M3
Copy from an **external** GCS path (not the project Store) into `into`, via the `gs.*` helpers
([08 §3](08-runtime-helpers.md)). Distinct from a `gs://` *Store*: this is an arbitrary external bucket
read.

---

## 3. Relationship to `fetch` and to dependency edges

- For an input used across the graph, prefer a **declarative external Artifact** (a cached, addressable
  node) that overrides `retrieve` with `source.*` — it arrives at `self.dep.path` and the dependency
  edge *is* the warm-up (the `examples/em` `HFModel` docstring makes this point).
- For a one-off fetch inside a `construct` body, `pipelines.runtime.fetch(...)` ([08 §4](08-runtime-helpers.md))
  exists, but the external-Artifact form is preferred.

---

## 4. Conformance hook

- `LocalDocument.retrieve(only=...)` → `source.local(...)` fetches `text.txt` from the checked-in
  `examples/test/data/<name>/` into `self.path`.
- `RawJsonl.retrieve` → `source.local(self.path_, ...)`.
- `HFModel.retrieve` → `source.hf(repo, revision, into=self.path, only=only)` honoring `only`.
- `only=["metrics.json"]` style partial fetch works through `source.local` (for derived-read parity).

Next: [05-futures-derived.md](05-futures-derived.md).
