# Identity and Paths — `pipelines/identity.py`

**Specifies:** `design/02-identity-and-storage.md` (`relpath` = identity, no fingerprint, collision
errors, path validation, on-disk layout, determinism).
**Modules:** `pipelines/identity.py`.
**Depends on:** nothing (pure functions).
**Milestone:** M1.

This module is small and pure: it validates `relpath`, slugifies, computes the stable config
fingerprint (for the default `relpath` and for dedup), and runs the same-run collision check at graph
build. No I/O.

---

## 1. `validate_relpath(relpath: str) -> str`

Returns the input unchanged on success; raises `RelpathError` otherwise. Rules (`design/02 §5`):

```python
def validate_relpath(rp: str) -> str:
    if not rp or rp.strip() == "":            raise RelpathError("empty relpath")
    if rp.startswith("/"):                     raise RelpathError(f"absolute relpath: {rp!r}")
    parts = rp.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):             raise RelpathError(f"illegal segment {seg!r} in {rp!r}")
        if seg.strip() != seg or seg.strip()=="": raise RelpathError(f"whitespace segment in {rp!r}")
    return rp
```

- `/` is a **real** subdirectory separator (a feature — `hf/Qwen/Qwen2.5-14B@main`). Never sanitized or
  rewritten; only validated.
- `validate_under_base(path, base)` (used by `self.path`, [01 §3](01-core-artifact.md)) re-checks the
  resolved absolute path is still under `base` (defense in depth against a `relpath` property that
  computed something pathological): `os.path.commonpath([base, path]) == str(base)`.

The framework calls `validate_relpath(a.relpath)` for every artifact during graph collection
([06 §2](06-execution.md)), so a bad `relpath` fails fast with the class named.

---

## 2. `fingerprint(config) -> str` and the fingerprintable predicate

Used in two places: the **default** `relpath` (`ClassName/<digest>`) and **dedup/hash** stability.
Ported and simplified from `experiments/artifact.py:95-159` (which used `sha256(...)[:10]`), **dropping
the `IgnoreHash` directive** — v2 has no out-of-identity fields, so there is nothing to exclude.

```python
def fingerprint(obj, n: int = 12) -> str:
    payload = _canonical(obj)                       # deterministic str/bytes
    return hashlib.sha256(payload.encode()).hexdigest()[:n]

def _canonical(obj) -> str:
    # deterministic, recursive serialization:
    #   primitives        -> repr with stable float formatting
    #   tuple/frozenset   -> ordered (frozenset sorted by canonical) "(...)"
    #   frozen dataclass  -> "ClassName(field=…, …)" over sorted fields
    #   enum              -> f"{type}.{name}"
    #   artifact          -> its relpath  (identity by relpath, design/02 §1)
    #   Future            -> its stable surrogate key (05 §2)
    ...
```

**`is_fingerprintable(value_or_type) -> bool`** powers the field-type guard ([01 §2b](01-core-artifact.md)):
accepts primitives, `tuple`/`frozenset` of fingerprintable, frozen dataclasses, enums, artifacts,
`Future`; rejects `list`/`dict`/`set`/mutable/unhashable. Rationale: instances must hash stably for
dedup and for the default digest, and the design forbids hashing "something unstable" (`design/02 §9`).

> **Note on artifacts inside the fingerprint.** A dependency contributes identity via its own
> `relpath` appearing in `_canonical` — which is usually *also* visible in the consumer's readable
> `relpath`. Transitivity isn't required (`design/02 §1`); the fingerprint only backstops the default.

---

## 3. `slug(value) -> str`

Flattens separators into a single safe segment (`design/02 §5`):

```python
slug("Qwen/Qwen2.5-14B")   # -> "Qwen_Qwen2.5-14B"
```
Replace `/`, whitespace, and other path-hostile chars with `_`; collapse repeats; preserve `.` and `-`
(readable). Re-exported from `pipelines.runtime` (the examples import `from pipelines.runtime import
slug`), but **defined here** to avoid a runtime→identity import inversion — `runtime.slug = identity.slug`.

---

## 4. Same-run collision check (the one guardrail)

`design/02 §4`: the author must encode every output-varying field in `relpath`; the framework's only
safety net is graph-local and cheap.

```python
def check_collisions(artifacts: Iterable) -> None:
    seen: dict[str, object] = {}
    for a in artifacts:
        rp = validate_relpath(a.relpath)
        prev = seen.get(rp)
        if prev is not None and prev != a:        # non-equal configs, same relpath
            raise CollisionError(rp, prev, a)     # names both classes + configs
        seen[rp] = a
```
- Equal artifacts at the same `relpath` **deduplicate** (handled naturally by the dict + frozen `__eq__`).
- The framework **never** appends a suffix (v1's nondeterministic digest-suffix rule is gone).
- Cross-run clobbering is allowed and silent by design ([§5](#5-no-fingerprint-clobbering-is-intentional)).

Called once by graph collection over the reachable set ([06 §2](06-execution.md)).

---

## 5. No fingerprint; clobbering is intentional

There is **no content hash, source hash, version tag, or freshness comparison** (`design/02 §3`).
Skip/resume is purely existence-based (`exists()`, [03](03-storage-backends.md)). Implementation
consequences the rest of the spec relies on:

- Changing a field that is *in* `relpath` → new path → no clobber.
- Changing a field *not* in `relpath`, or editing code `construct` calls → **same `relpath`, new run
  clobbers**. The scheduler will *skip* it if already committed unless `cache=False`/`--force`/deleted.
- Therefore `exists()` is the sole skip signal; `meta.json` (if any) **never** gates a skip
  ([§7](#7-optional-metajson)).

`fingerprint` exists only for the *default* `relpath` and dedup — it is **not** a staleness mechanism.

---

## 6. On-disk layout

Both `base_path` and each selected Store lay outputs out by `relpath` verbatim (`design/02 §7`). No
hidden index in the artifact's user-owned contents; the Store backend may keep its own private
publication bookkeeping (manifest) outside the artifact directory ([03 §3](03-storage-backends.md)).
`ls`/`cat`/`rm` are first-class interfaces; `rm -r <relpath>` invalidates (next run rebuilds).

---

## 7. Optional `meta.json`

The framework never creates or requires it (`design/02 §8`). An artifact may write `meta.json` (or any
file) into `self.path` as ordinary output; tooling (`inspect`, lineage, audit — [09](09-cli.md)) may
display it when present. `write_meta(artifact, into=...)` is a *helper* offered to authors
([03 §4](03-storage-backends.md)), not automatic. It is **never** read to decide a skip.

---

## 8. Determinism rules (enforced/encouraged)

1. `relpath` must be a pure function of fields — no `time`/`random`/`os.environ`/`uuid4`. Not
   mechanically enforceable, but `pipelines dryrun` prints every `relpath` + resolved `path` so diffing
   across machines/edits is the determinism check ([06 §8](06-execution.md), [09](09-cli.md)).
2. Fields fingerprint stably (the predicate in §2 rejects unstable types at class creation).
3. Member ordering is deterministic where it matters (array index → relpath, M-6).

---

## 9. Conformance hook

- `slug()` exact output for `"Qwen/Qwen2.5-14B"` → `"Qwen_Qwen2.5-14B"`.
- `validate_relpath` rejects `/abs`, `a/../b`, `a//b`, `" "`.
- Collision check errors on two non-equal artifacts with the same `relpath`; dedups equal ones.
- `examples/em` `HFModel.relpath = "hf/{repo}@{revision}"` keeps the `/` nesting from `repo`.
- Determinism: re-importing `examples/test` yields byte-identical `relpath`s.

Next: [03-storage-backends.md](03-storage-backends.md).
