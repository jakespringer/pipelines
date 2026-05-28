# Futures and Derived Values — `pipelines/futures.py`

**Specifies:** `design/05-derived-and-futures.md` (`@derived`, `Future[T]`, combinators, future-valued
config fields).
**Modules:** `pipelines/futures.py`.
**Depends on:** [02 identity](02-identity-paths.md) (fingerprint surrogate); duck-types artifacts (no
hard import of `artifact.py` — see [00 §2](00-implementation-overview.md) cycle rule).
**Milestone:** M1 (`scan_dependencies`), M2 (`@derived`, `Future`, combinators, resolution).

Implements cross-cutting mechanics **M-2** (future-field resolution clones the artifact) and **M-3**
(`@derived` is a per-instance lazy `Future`). Under v2's no-fingerprint identity, this layer is much
lighter than v1: no symbolic keys, no resolver jobs, no freshness machinery (`design/05` preamble).

---

## 1. `@derived` — descriptor returning a `Future`

```python
def derived(reads: str | list[str] | None = None):
    def deco(fn):
        return _Derived(fn, reads)
    return deco

class _Derived:
    def __init__(self, fn, reads): self.fn, self.reads = fn, reads
    def __set_name__(self, owner, name): self.name = name
    def __get__(self, instance, owner):
        if instance is None: return self
        return Future(sources=[instance], thunk=lambda: self._read(instance), key=("derived", instance.relpath, self.name))
    def _read(self, instance):
        if instance.__pipelines__.automaterialize:
            instance.retrieve(only=_as_list(self.reads))     # PARTIAL retrieve (03 §1, design/04 §2)
        return self.fn(instance)
```

- A `@derived` is a **cheap, pure read** of the artifact's own output (`design/05 §1`). Access returns a
  `Future`; nothing is read until resolved.
- **Auto-materialize on access** is gated by `automaterialize`: when `True`, resolving the future first
  does a partial `self.retrieve(only=reads)`; when `False`, the body must call `self.retrieve(only=...)`
  itself.
- `reads=` is a filename, list, or glob; omit to fetch the full output.
- Memoized per run (§2 cache). Real compute belongs in a downstream Artifact, not a `@derived`.

Examples to satisfy: `WordIndex.unique_words` (`reads=METRICS_FILE`), `FinetunedModel.final_train_loss`
(`reads="metrics.json"`), `JudgedResponses.unsafe_rate` (`reads=JUDGED_FILE`).

---

## 2. `Future[T]`

```python
class Future(Generic[T]):
    def __init__(self, sources: list, thunk: Callable[[], T], key: tuple):
        self._sources = sources       # artifacts this future depends on (for ordering, §3)
        self._thunk = thunk
        self._key = key               # stable surrogate for hashing/repr (NOT a fingerprint of a lambda)

    def result(self) -> T:
        cache = _RESULT_CACHE.get()                 # contextvar dict, per run
        if self._key in cache: return cache[self._key]
        val = self._thunk(); cache[self._key] = val; return val

    # eager-feeling coercion for analysis (design/05 §2)
    def __float__(self):  return float(self.result())
    def __int__(self):    return int(self.result())
    def __bool__(self):   return bool(self.result())
    def __lt__(self, o):  return self.result() <  _v(o)
    def __le__(self, o):  return self.result() <= _v(o)
    def __gt__(self, o):  return self.result() >  _v(o)
    def __ge__(self, o):  return self.result() >= _v(o)
    def __eq__(self, o):  return self.result() == _v(o)
    def __hash__(self):   return hash(self._key)     # stable; lets a Future sit in a frozen field
    def __repr__(self):   return f"Future({self._key})"

    def sources(self) -> list: return _flatten_sources(self._sources)   # for the dependency scan
```

- **Lazy as an object** (safe to reference while wiring the graph — no I/O), **eager-feeling as a value**
  (coercion resolves on the current device).
- **`_key`** is a stable surrogate (e.g. `("argmax", id-stable-token, tuple(sorted(src.relpath)))`). It
  exists for hashing/memoization/`repr` — **never** a fingerprint of a callable. This is exactly the v1
  problem v2 avoids (`design/05 §3`): the selection callable is only ever *executed*, never hashed.
- `gather([...]).to_frame()` returns a pandas DataFrame (optional dep) for analysis.

---

## 3. `scan_dependencies(artifact)` — the DAG edges (M1)

Attached to artifacts as `dependencies()` ([01 §3](01-core-artifact.md)). Walks the instance's
dataclass fields and collects dependency artifacts, **including `Future` source sets** so a
future-valued field expands to deps on all its candidates (`design/05 §4`):

```python
def scan_dependencies(a) -> list:
    out = []
    for f in dataclasses.fields(a):
        v = getattr(a, f.name)
        out += _edges_of(v)
    # de-dup by relpath, preserve first-seen order (deterministic)
    return _dedup(out)

def _edges_of(v):
    if _is_artifact(v):           return [v]
    if isinstance(v, Future):     return list(v.sources())      # the static candidate set
    if isinstance(v, tuple):      return [e for x in v for e in _edges_of(x)]
    if isinstance(v, dict):       return [e for x in v.values() for e in _edges_of(x)]
    return []
```

Port the field-walking idea from `experiments/artifact.py:52` (`get_direct_dependencies`), extended to
`Future` sources and `dict` values. `_is_artifact(v)` duck-types on `hasattr(v, "relpath")` +
`__pipelines__` to avoid importing `artifact.py` (cycle rule).

---

## 4. Combinators

Pure functions building new futures from existing ones (`design/05 §3`):

```python
def fmap(future: Future, fn) -> Future: ...
def gather(futures: list[Future]) -> Future:        # -> Future[list]; .to_frame() helper
def argmax(items, key) -> Future:                   # key: item -> Future|value; -> Future[item]
def argmin(items, key) -> Future: ...
def select(items, key, reduce) -> Future: ...        # general reduce over (item, key-future)
```

`argmax(candidates, key=lambda m: m.val_accuracy)` returns `Future[item]`:
- `sources` = the candidate artifacts (so ordering depends on **all** of them — `design/05 §6`).
- `thunk` = evaluate each `key(item)` (resolving the inner derived futures), pick the winner item, and
  return it (an artifact). `examples/test` uses `argmax(candidates, key=lambda index: index.unique_words)`.

The selection callable runs only at resolution; never hashed.

---

## 5. Future-valued field resolution (M-2)

When `automaterialize=True`, before `construct`/`@derived` the framework resolves any `Future` held in a
field and **clones** the artifact so the field becomes concrete:

```python
def resolve_future_fields(a):
    repl = {}
    for f in dataclasses.fields(a):
        v = getattr(a, f.name)
        if isinstance(v, Future):
            chosen = v.result()           # e.g. the argmax-winning artifact
            if _is_artifact(chosen):
                chosen.retrieve()         # make the selected dep local
            repl[f.name] = chosen
    return dataclasses.replace(a, **repl) if repl else a
```

- The **resolved clone** is what `construct` runs on, so `self.best.path`/`self.best.relpath` are
  concrete (`examples/test` `WinnerReport`).
- **Identity is unchanged:** `relpath` is computed from the *original* `a` (stable, author-given —
  `production/best_by_val_acc`, never the winner's path). Only `construct` sees the clone.
- With `automaterialize=False`, this step is skipped; the field keeps its `Future` and the body resolves
  it (`examples/test` `ManualWinnerReport`: `chosen = self.best.result(); chosen.retrieve(only=[...])`).

The primitive ([06 §1](06-execution.md)) calls `resolve_future_fields` between dep-readying and
`construct` when `automaterialize=True`.

### Identity & re-running
Re-running does **not** auto-detect that a different candidate now wins — `exists()` sees the consumer's
`relpath` committed and skips (`design/05 §4`, consistent with [02 §5](02-identity-paths.md)). Force a
refresh with `cache=False` or by deleting the output. An artifact may optionally record the winner in its
own `meta.json` for `inspect` ([09 §5](09-cli.md)).

---

## 6. Conformance hook

- `argmax(candidates, key=lambda i: i.unique_words)` resolves to the `WordIndex` with the most unique
  words; used as a field of `WinnerReport`/`ManualWinnerReport`.
- `WinnerReport` (automaterialize default) sees a concrete `self.best`; `ManualWinnerReport` resolves by
  hand and partial-retrieves only `metrics.json`.
- `@derived` coercion: `float(m.unsafe_rate)` works on an analysis device, partial-retrieving the read
  file only.
- `gather([...]).to_frame()` produces a DataFrame over a sweep's derived values.
- A `Future` field is hashable (frozen artifact stays hashable).

Next: [06-execution.md](06-execution.md).
