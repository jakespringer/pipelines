# Identity and Storage Layout

An Artifact's identity is its **`relpath`** — a human-readable relative path that is a pure function
of its configuration. This document defines `relpath`, the `relpath`↔`path` split, why there is no
content fingerprint, the clobbering model, same-run collision errors, path validation, the on-disk
layout, and optional user-authored metadata.

This is the biggest change from v1, which used a content hash (`key`) plus an optional `name`
template — a split that created a key-vs-path contradiction. v2 has **one anchor**.

---

## 1. `relpath` — the identity and the location

```python
@artifact
class PretrainedModel:
    lr: float
    epochs: int
    @property
    def relpath(self) -> str:
        return f"pretrain/lr{self.lr:.0e}_ep{self.epochs}"
```

- **A pure function of config.** No I/O, no ambient context — `m.relpath` is always available
  (graph build, dryrun, anywhere). This is what keeps graph construction deterministic.
- **The identity.** Two artifacts with the same `relpath` *are* the same artifact (for dedup, skip,
  resume). A dependency contributes its identity by virtue of its own `relpath` appearing (often) in
  the consumer's `relpath`, but transitivity isn't required — see §3.
- **The local materialization location** (via `self.path`) and the **selected Store layout** (via
  `commit`; normally the executor default, optionally `@artifact(store=...)`).

`relpath` may be a plain attribute or a `@property` (it usually depends on config). Default (if a
subclass declares none): `f"{ClassName}/{stable-digest-of-config}"` — works, but readable
`relpath`s are strongly encouraged.

---

## 2. `relpath` vs `self.path`

| Accessor | What | Needs an executor? |
|----------|------|--------------------|
| `self.relpath` | relative, human-readable, pure function of config | no — always available |
| `self.path` | absolute local path = `<base_path>/<relpath>` | yes — `<base_path>` is supplied by the executor |

The **executor** runs the code on a particular device and knows that device's local `base_path`
(from config or programmatic setup). `self.path` therefore resolves only under an active executor;
`self.relpath` resolves always. `construct` writes to `self.path`, `retrieve` downloads to it,
`commit` uploads from it, and a dependency's output is read at `self.dep.path`.

This is a deliberate, documented dependency on the executor context (inherent to "run on whichever
device the executor chose"), not hidden global state — `relpath` stays pure, and only the absolute
resolution needs the executor.

The Store selected for persistence is an operational policy, not identity. Most artifacts use the
executor's default `gs://` Store; a large-checkpoint class may select a configured `file://` Store
while keeping the same `relpath` and default lifecycle behavior ([04](04-retrieval-and-storage.md)).

---

## 3. No content fingerprint; clobbering is intentional

There is **no content hash, no source-hash, no version tag, and no freshness comparison.**
Skip/resume is **purely existence-based** (`exists()` — §6, [04](04-retrieval-and-storage.md)).

**Why:** we *want* artifacts to clobber the prior output at the same `relpath`, including after the
code has changed. Re-running `construct` and overwriting the old output is a feature, not a hazard:

- changing a field that's *in* `relpath` → new `relpath` → new output (no clobber);
- changing a field *not* in `relpath`, or changing code that `construct` calls → **same `relpath` →
  the new run clobbers the old output**. Intended.

**Consequence, accepted consciously:** editing the code a `construct` calls and re-running does
**not** get auto-detected — `exists()` sees the `relpath` committed and skips. To force a fresh
build: `cache=False`, delete the output, or vary `relpath`. This trades the v1 staleness machinery
(versioning, fingerprints) for simplicity and predictable, intentional overwrites. An artifact may
choose to write provenance such as a git SHA for *audit* (§8), but metadata never gates a skip.

---

## 4. The guardrail: same-run collisions are errors

The author's obligation: **`relpath` must encode every field you want the output to vary by.** The
framework's single safety check is cheap, deterministic, and graph-local:

- At graph-build time, it compares the `relpath`s of all artifacts in the run. If two artifacts with
  **non-equal config** resolve to the **same `relpath`**, graph construction **errors**, naming both
  artifacts and their configs.
- Equal artifacts at the same `relpath` deduplicate normally. The framework never appends a suffix.
  Cross-run clobbering remains allowed and silent by design.

(v1's "collision → graph-dependent digest suffix" rule is gone; it made paths nondeterministic and
broke resume.)

---

## 5. Path validation and `/`

`relpath` is fully author-controlled. `/` is a **real subdirectory separator** — a feature, for
organizing by namespace/org (`hf/Qwen/Qwen2.5-14B@main`). The framework does **not** sanitize or
rewrite the string, but it **validates** it and **rejects** pathological values:

- absolute paths (leading `/`),
- `..` traversal,
- empty / whitespace-only segments,

so a buggy `relpath` can't escape the base path. A **`slug()`** helper is provided for when the
author wants a flat segment from a value containing separators:

```python
slug("Qwen/Qwen2.5-14B")   # -> "Qwen_Qwen2.5-14B"
```

---

## 6. Existence, skip, and resume

Materializing an Artifact ([06](06-execution.md)) begins with a **scheduler-only** freshness check:

```
if artifact.cache and artifact.exists() and not force:
    skip  — already committed; a consumer will retrieve() it lazily
else:
    (ready deps) → construct() → commit()         (or, external: retrieve())
```

- `exists()` defaults to "the output has been committed at `relpath` according to the artifact's
  selected Store." It is
  **scheduler-only and may be expensive**, so it is never called on the hot path — `retrieve()`
  assumes existence and raises if absent. Artifact contents, including an optional `meta.json`, do
  not define completeness.
- Existence checks are **batchable** via `exists_many(artifacts)` (default loops `exists`; a backend
  can implement one prefix `list` + membership) so large sweeps stay fast.

**Resume** is the same mechanism: re-run the script (graph rebuilds identically and instantly,
because `relpath` is pure), and the scheduler materializes only the artifacts whose `relpath` isn't
committed yet, retrieving the rest.

---

## 7. On-disk layout (interpretability)

Both the local `base_path` and each selected Store lay outputs out by `relpath`. For example a
project may publish ordinary outputs to GCS while retaining large finetuned models on configured
filesystem storage:

```
<default-gs-store>/
├── pretrain/
│   ├── lr1e-03_ep10/
│   │   ├── checkpoint.pt          # whatever construct wrote into self.path
│   │   ├── metrics.json
│   │   └── meta.json              # optional user-authored provenance, if desired
│   └── lr1e-04_ep10/ …
├── hf/Qwen/Qwen2.5-14B@main/ …    # '/' in relpath -> real nesting
└── generations/cifar10/lr1e-04/
    └── generations.jsonl

<local-model-file-store>/
└── finetune/cifar10/lr1e-04/
    └── model.safetensors
```

- **Paths are meaningful** — navigate by eye; `ls`/`cat`/`rm` are valid interfaces.
- **Artifact contents are user-owned** — a normal committed output has no required bookkeeping
  file. The store backend owns any internal publication bookkeeping needed for committed existence.
- **Hand edits are first-class** — `rm -r` to invalidate (next run rebuilds), copy to relocate.

---

## 8. Optional user-authored `meta.json`

The framework does **not** create or require `meta.json`. If an artifact wants portable provenance,
it may write a `meta.json` file into `self.path` before commit, just like any other output:

```json
{
  "relpath": "finetune/cifar10/lr1e-04",
  "class": "FinetunedModel",
  "config": { "lr": 0.0001 },
  "dependencies": { "base": "pretrain/lr1e-03_ep10", "data": "dataset/cifar10" },
  "resolved_futures": { },
  "produced_at": "2026-05-22T14:03:11Z",
  "duration_s": 4912,
  "code": { "git_sha": "8c30de8", "dirty": false, "entrypoint": "run.py" },
  "env": { "host": "gpu-node-07", "python": "3.11.8", "pipelines": "0.5.0" }
}
```

When present, tooling may display this file for **audit** (`code.git_sha`), historical lineage, or
cost reporting ([09](09-cli-and-observability.md)). An artifact using futures may choose to record
which candidate it selected ([05](05-derived-and-futures.md)). When absent, committed existence and
execution work normally; current-graph lineage remains available. It is **never** read to decide a
skip (that's `exists()` only).

---

## 9. Determinism rules

For `relpath`s (and the graph) to be identical across re-runs and machines:

1. **`relpath` is a pure function of fields** — no `time`/`random`/`os.environ`/`uuid4` flowing into
   it. Graph construction does no I/O.
2. **Fields fingerprint stably** for the default `relpath` and for dedup: primitives + frozen
   dataclasses of primitives + immutable containers. `@artifact` rejects non-fingerprintable field
   types rather than hashing something unstable.
3. **Member ordering is deterministic** where it matters (array jobs map index → `relpath` by
   re-deriving the same order — [06](06-execution.md), [09](09-cli-and-observability.md)).

`pipelines dryrun` prints every artifact's `relpath` and resolved `path`; diffing that across
machines or before/after an edit is the determinism check.

---

## 10. Summary

- Identity = **`relpath`**, a pure-function-of-config human-readable path; **no content hash**.
- `self.relpath` is always available; `self.path = <base_path>/<relpath>` needs the executor.
- **Clobbering is intentional**; skip/resume is **existence-based**; no fingerprint, no staleness
  auto-detection. Refresh by deleting or varying `relpath`.
- Non-equal artifacts with the same `relpath` in one graph are a **graph-build error**.
- `/` is a real separator; the framework validates (rejects absolute/`..`/empty) but never rewrites;
  `slug()` flattens on request.
- Layout is by `relpath` within each selected Store; `@artifact(store=...)` allows explicit
  GCS/local policy without changing identity. `meta.json` is optional user-authored provenance,
  not framework state or a completion marker.

Next: [03-construction.md](03-construction.md).
