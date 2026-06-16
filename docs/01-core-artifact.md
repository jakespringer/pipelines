# Core Artifact — `pipelines/artifact.py`

**Specifies:** `design/01-artifacts.md` (the `@artifact` decorator, fields-as-config, the four-function
contract, externals, settings, annotations).
**Modules:** `pipelines/artifact.py`.
**Depends on:** [02 identity](02-identity-paths.md), [05 futures](05-futures-derived.md) (`dependencies()`),
[03 store](03-storage-backends.md) (default lifecycle), [08 runtime](08-runtime-helpers.md) (`_CTX`).
**Milestone:** M1.

This is the keystone module: it turns a plain class into a frozen dataclass with framework-injected
members, records its settings, and guards footguns. Implements cross-cutting mechanics **M-1**
(`self.path`) and **M-4** (settings + decorator-with-args) from [00 §3](00-implementation-overview.md).

---

## 1. `ArtifactSettings` and the decorator signature

```python
from dataclasses import dataclass
from typing import Callable, Type

StoreSpec = str | "Store" | Callable[[], "str | Store"]   # M-5

@dataclass(frozen=True)
class ArtifactSettings:
    automaterialize: bool = True
    autocommit: bool = True
    cache: bool = True
    store: StoreSpec | None = None          # None => executor default Store
    session: Type["Session"] | None = None
    retries: int = 0
    env: dict | None = None                  # rare per-artifact env overrides (design/08 §8)
    transient: bool = False                  # on-demand intermediate (06 §2 "Transient artifacts")
```

```python
def artifact(cls=None, /, *, automaterialize=True, autocommit=True, cache=True,
             store=None, session=None, retries=0, env=None, transient=False):
    """Class decorator. Usable bare (@artifact) or with args (@artifact(cache=False))."""
    def wrap(klass):
        return _build_artifact(klass, ArtifactSettings(
            automaterialize, autocommit, cache, store, session, retries, env, transient))
    return wrap if cls is None else wrap(cls)
```

`transient=True` marks an artifact that is scheduled **only** when something that will actually run
depends on it; if nothing running needs it, the freshness/prune pass skips it even when missing
(unless a forced artifact needs it, or `--force-all`). See [06 §2 "Transient artifacts"](06-execution.md).

`@dataclass_transform(frozen_default=True, kw_only_default=True)` decorates `artifact` so type
checkers treat decorated classes as frozen kw-only dataclasses (fields, `__init__`, immutability).

---

## 2. `_build_artifact` — what the decorator does (M-4)

Order matters; each step is a contract point.

```python
def _build_artifact(klass, settings):
    _guard_reserved_names(klass)             # (a) raise on reserved field annotations
    _validate_field_types(klass)             # (b) reject non-fingerprintable fields
    klass = dataclass(frozen=True, kw_only=True)(klass)   # (c) apply once
    klass.__pipelines__ = settings           # (d) settings carrier
    _inject_members(klass)                    # (e) path, dependencies, default lifecycle, materialize
    return klass
```

**(a) Reserved-name guard.** Walk `klass.__annotations__` (own, not inherited). If any of
`{relpath, path, dependencies, retrieve, exists, commit, materialize, annotations, session}` is
declared as an annotated **field**, raise `ArtifactDefinitionError` naming the class and the offending
name, with the fix ("declare `relpath` as a `@property`, not a field"). These names may be defined as
methods/properties — only field annotations are rejected.

**(b) Field-type validation.** For each dataclass field, check the annotation/default is
*fingerprintable* (see [02 §2](02-identity-paths.md) for the predicate): primitives (`str/int/float/
bool/None`), `tuple`/`frozenset` of fingerprintable, frozen dataclasses, enums, other artifacts,
`Future`. Reject `list`, `dict`, `set`, and arbitrary objects with a message pointing at the field
and suggesting `tuple`. This runs at class-creation so the error is early and located. (Note: a field
*may* hold a `Future` — that is data-dependent and allowed; see [05](05-futures-derived.md).)

**(c) Apply dataclass once.** `frozen=True` gives immutability + `__hash__`/`__eq__`; `kw_only=True`
removes "required field after defaulted field" so a subtype can add required deps without `= None`
lies — this is exactly the `Prompts → RawPrompts`/`SplitPrompts` case in `examples/em` (base has
`n: int | None = None`; subtypes add required `src`/`dataset`).

**(d)** Attach `__pipelines__`.

**(e) Inject members** (§3).

---

## 3. Injected members

### `relpath`
Not injected if the subclass defines it (attr or `@property`). If absent, inject a default `@property`
returning `f"{ClassName}/{fingerprint(self)}"` (fingerprint from [02 §2](02-identity-paths.md)).
Readable `relpath`s are strongly preferred; the default is a safety net.

### `path` — the contextvar resolution (M-1)
```python
@property
def path(self) -> Path:
    cur = pipelines.runtime._CTX.get(None)
    if cur is None:
        raise RuntimeContextError(f"self.path requires an active executor (relpath={self.relpath!r})")
    p = cur.base_path / self.relpath
    validate_under_base(p, cur.base_path)     # defense in depth; relpath already validated (02 §5)
    return p
```
`self.dep.path` works identically — `dep` is just another artifact instance reading the same `_CTX`.

### `dependencies()`
Delegates to `pipelines.futures.scan_dependencies(self)` ([05 §3](05-futures-derived.md)) — returns the
list of direct dependency artifacts found in fields (including inside `tuple`/`dict` and `Future`
source sets). Used by graph collection ([06 §2](06-execution.md)). Implemented in `futures.py` to keep
the `Future` knowledge in one place; attached here as a method.

### Default storage lifecycle (overridable)
Each delegates to the **selected Store** (M-5), resolved by `_resolve_store(self)`:
```python
def _resolve_store(self):
    spec = self.__pipelines__.store
    if spec is None:
        return ctx.executor_store          # executor default; set in _CTX
    store = spec() if callable(spec) else spec
    return store if isinstance(store, Store) else Store.from_uri(store)

def retrieve(self, *, only=None) -> None:
    _resolve_store(self).get_dir(self.relpath, dest=self.path, only=only)

def exists(self) -> bool:
    return _resolve_store(self).exists(self.relpath)

def commit(self) -> None:
    _resolve_store(self).put_dir(self.path, self.relpath)     # atomic (03 §3)
```
A subclass that defines its own `retrieve`/`exists`/`commit` keeps it (don't overwrite). Externals
(no `construct`) typically override only `retrieve` (via `source.*`, see [04](04-sources.md)). The
injector must therefore **only add a default when the attribute is not already present on the class or
its bases** (`"retrieve" not in vars(klass)` plus an MRO check for user-defined, excluding the injected
base).

### `materialize(self, *, scheduler=False, strict=False)`
Thin instance entry to the primitive in [06 §1](06-execution.md): `from .execution.materialize import
materialize as _m; _m(self, scheduler=scheduler, strict=strict)`. Lets `examples` call `m.construct()`
in tests and lets the worker call `artifact.materialize(...)`.

### `has_construct(self)` helper
`callable(getattr(type(self), "construct", None))` — distinguishes constructed vs external; used by the
primitive.

---

## 4. Constructed vs external

- **Constructed:** defines `construct(self) -> None` writing into `self.path`. Uses default lifecycle.
- **External/given:** no `construct`; overrides `retrieve`. There is **no `Source` type and no
  `locate()` hook** (`design/01 §5.2`). `exists()` default (committed at `relpath` in the selected
  Store, or present locally for a pure passthrough) is usually fine; override for hub semantics
  (`design/01` OPEN item — the spec's resolution: default checks the selected Store first, then local
  `base_path`; an external may override to probe the hub).

At planning time, an artifact with **neither** a usable `construct` **nor** a usable `retrieve` and no
committed output is an error the executor reports ([06 §2](06-execution.md)).

---

## 5. `annotations`

A class attribute or `@property` (config-dependent), always read as `artifact.annotations` →
`dict | None`. Open, namespaced bag of execution hints; **never part of identity**. Not a decorator
setting. Default when undefined: `{}`. Resolution and portable→backend mapping live in
[08 §7](08-runtime-helpers.md) and [06 annotations](06-execution.md). The decorator does nothing to
`annotations` beyond leaving any user definition intact; `gpu_annotations(...)` ([08](08-runtime-helpers.md))
is the convenience helper.

---

## 6. API surface (mirrors `design/01 §8`)

| Member | Required? | Injected default? | Purpose |
|--------|-----------|-------------------|---------|
| dataclass fields | yes | — | config + dependency edges |
| `relpath` (attr/`@property`) | recommended | `ClassName/<digest>` | identity + local location ([02](02-identity-paths.md)) |
| `construct(self) -> None` | optional | none | build into `self.path`; omit for external |
| `retrieve(self, *, only=None)` | optional | Store `get_dir` | override for externals/custom backends |
| `exists(self) -> bool` | optional | Store `exists` | scheduler-only |
| `commit(self) -> None` | optional | Store `put_dir` (atomic) | override for W&B/registry ([03 §4](03-storage-backends.md)) |
| `@derived` methods | optional | — | cheap reads as `Future` ([05](05-futures-derived.md)) |
| `annotations` | optional | `{}` | executor hints; never identity |
| `path`, `dependencies()`, `materialize()` | — | always injected | framework-provided |

---

## 7. Edge cases the implementation must handle

- **Inheritance of injected members.** Don't double-inject when a subclass is also `@artifact`-decorated
  (`em`'s `Prompts` base + `RawPrompts`/`SplitPrompts`). Inject only members not already provided by a
  `@artifact` base; re-apply `dataclass` per class (Python dataclass inheritance handles field merge;
  `kw_only` makes added required fields legal).
- **`@property relpath` referencing deps** (`WordIndex.relpath` uses `self.text.relpath`) — fine, it's
  pure config composition; no I/O.
- **Frozen + `__post_init__`.** Avoid mutating in `__post_init__`; if a derived cached value is needed,
  use `object.__setattr__` sparingly — but the design discourages it. Prefer `@property`.
- **Hashing of nested artifacts.** Frozen dataclass `__hash__` recurses into fields; tuples of
  artifacts hash fine; a `Future` field must be hashable (give `Future` a stable `__hash__` based on
  its source set + a callable id surrogate — see [05](05-futures-derived.md)).

---

## 8. Conformance hook

Must support, verbatim from `examples/`:
- bare `@artifact` (`LocalDocument`, `NormalizedText`, `HFModel`, …);
- `@artifact(store=callable)` (`WordIndex`, `FinetunedModel`);
- `@artifact(session=Cls)` (`Preview`, `JudgedResponses`);
- `@artifact(automaterialize=False)` (`ManualWinnerReport`);
- `@artifact(autocommit=False)` (`PublishedBundle`);
- `@artifact(cache=False)` (`AuditMarker`);
- `kw_only` inheritance (`Prompts` → `RawPrompts`/`SplitPrompts`);
- `@property relpath`, `@property annotations`, `@derived` coexisting on one class.

Next: [02-identity-paths.md](02-identity-paths.md).
