# Derived Values and Futures

A static config graph expresses "finetune *this* model," but not "finetune the **best** of this
sweep," nor a clean "read each model's metric on my laptop for analysis." This document defines
**derived values** (`@derived`, cheap reads of an output, returned as a lazy `Future`), **futures**
and their combinators, and **future-valued config fields** (data-dependent dependencies) — and how
all of it stays simple under v2's identity model.

> **Why this is much lighter than v1.** v1's selection layer needed symbolic keys (hashing a
> selection lambda), a symbolic-vs-resolved identity split, freshness machinery, and Slurm
> "resolver jobs." Three of those four problems were artifacts of content-addressed identity, which
> v2 deleted ([02](02-identity-and-storage.md)). Under `relpath`-as-identity with no fingerprint,
> the future layer collapses to "lazy reads + optional automatic resolution before `construct`."

---

## 1. Derived values — `@derived`

A `@derived` method is a **cheap, pure read of the artifact's own output**, returned as a lazy
`Future[T]`:

```python
from pipelines import artifact, derived

@artifact
class PretrainedModel:
    lr: float
    @property
    def relpath(self) -> str: return f"pretrain/lr{self.lr:.0e}"
    def construct(self) -> None: train(lr=self.lr, out=self.path)   # writes metrics.json

    @derived(reads="metrics.json")
    def val_accuracy(self) -> float:
        return json.loads((self.path / "metrics.json").read_text())["val_acc"]

m = PretrainedModel(lr=1e-3)
acc = m.val_accuracy          # Future[float] — nothing read yet
```

- **Auto-materializes on access.** Resolving the future ensures the artifact's output is locally
  available — it calls `self.retrieve()` first (a **partial** retrieve of the declared `reads`,
  [04](04-retrieval-and-storage.md) §2), then runs the body. On an **analysis device** you
  instantiate the same configs (same `relpath`) and `m.val_accuracy` "just works," fetching only
  what it needs and computing.
- **Gated by `automaterialize`.** If `automaterialize=False` on the artifact, access does **not**
  auto-fetch — the body calls `self.retrieve(only=...)` itself.
- **`reads=` is configurable** — a filename, list, or glob; omit it to fetch the full output.
- **Cheap and pure.** A `@derived` is a read of an existing output, memoized within a run, never
  persisted. If "the value" needs real compute (an eval suite), make it a normal downstream Artifact
  and `@derived`-read *its* metrics (§5).

---

## 2. `Future[T]` — lazy, with implicit coercion

A `Future[T]` is a **lazy promise** of a value (or artifact) that is a function of one or more
outputs. It is wiring-safe (no I/O at graph-build time), and ergonomic in analysis:

- **Lazy as an object** — created without computing; safe to reference while building the graph.
- **Eager-feeling as a value** — it implements Python protocols (`__float__`, comparisons, `repr`)
  and ships a `gather([...]).to_frame()` helper, so using it as a number auto-resolves
  (partial-retrieve + compute) on the current device:

```python
# analysis (notebook): feels eager, materializes on this device
rates = {m.relpath: float(m.val_accuracy) for m in models}
df    = gather([m.val_accuracy for m in models]).to_frame()

# wiring: stays lazy, no I/O
best  = argmax(models, key=lambda m: m.val_accuracy)
```

`.result()` forces resolution explicitly when you want to be unambiguous.

---

## 3. Combinators

Pure functions that build new futures from existing ones:

```python
from pipelines import fmap, gather, argmax, argmin, select

fmap(future, fn)              # Future[T] -> Future[U]
gather([f1, f2, ...])         # list[Future[T]] -> Future[list[T]]   (+ .to_frame())
argmax(items, key=fn)         # pick item whose key(item) future is largest -> Future[item]
argmin(items, key=fn)
select(items, key, reduce)    # general reduce over (item, key-future) pairs -> Future[item]
```

```python
candidates = [PretrainedModel(lr=lr) for lr in (1e-3, 1e-4, 1e-5)]
best = argmax(candidates, key=lambda m: m.val_accuracy)     # Future[PretrainedModel]
```

The selection callable is only ever **executed** at resolution time; because identity is the
consumer's author-given `relpath` ([02](02-identity-and-storage.md)), the callable **never needs to
be hashed** — v1's "fingerprint a lambda" problem does not exist.

---

## 4. Future-valued config fields — resolve automatically when materializing

A config field may hold a `Future`. That makes the dependency **data-dependent**: which concrete
artifact it resolves to is decided at run time.

```python
@artifact
class ProductionModel:
    base: PretrainedModel          # may be passed a Future[PretrainedModel]
    @property
    def relpath(self) -> str: return "production/best_by_val_acc"     # stable, author-given
    def construct(self) -> None:
        export_for_serving(self.base.path, out=self.path)             # base is concrete here

prod = ProductionModel(base=argmax(candidates, key=lambda m: m.val_accuracy))
```

Two mechanics, both inherited from earlier decisions:

- **Ordering (static).** A `Future` field **expands to dependencies on its source artifacts**, and
  that source *set* is known at graph-build time (you know *which* candidates, just not *which
  wins*). So it slots into the "ordering is always derived from fields" rule
  ([06](06-execution.md)): the consumer is wired after **all** candidates.
- **Resolution (at consumer runtime, when `automaterialize=True`).** With the default lifecycle, the
  framework resolves future-valued fields **just before calling `construct`** — it reads the
  candidates' derived values, picks the winner, and retrieves it — so `construct` sees a
  **concrete, already-resolved `self.base`**. If the artifact opts into
  `automaterialize=False`, there is deliberately no such promise: its body must resolve the future,
  materialize the selected artifact, and use that concrete value itself.

```
# framework, at the consumer's runtime, BEFORE construct (automaterialize=True):
#   resolve future fields: read candidates' derived values, pick winner, retrieve it
def construct(self):
    export_for_serving(self.base.path, out=self.path)   # self.base concrete; same as a static dep
```

**Why no resolver job (v1's complexity):** v1 needed dynamic submission because it tried to know the
*winner's identity* at submit time. v2 only needs the *candidate set* at submit time (static →
normal ordering/`afterok`), and resolves **inside** the consumer job at runtime.

### Identity & re-running
`ProductionModel`'s identity is its **author-given `relpath`** (stable, browsable), regardless of
which candidate wins. An artifact may choose to write the selected winner into its own optional
provenance output; the framework does not create such metadata by default. **Consequence
(consistent with [02](02-identity-and-storage.md) §3):** re-running does **not** auto-detect that
a *different* candidate now wins — `exists()` sees the consumer's `relpath` committed and skips.
Force a refresh with `cache=False` or by deleting the output.

---

## 5. Discipline

1. **`@derived` is a cheap, pure read** of the output (declare `reads=`); not real compute. For real
   compute, make a downstream Artifact and `@derived`-read its metrics.
2. **Selection forces its candidates** — `argmax`/`select` need every candidate's criterion, hence
   every candidate built. Intended.
3. **The expression must be deterministic** — *which* candidates / *which* derivation is built
   deterministically ([02](02-identity-and-storage.md) §9); only the *resolved value* depends on
   outputs.
4. **`.result()` is for forcing, sparingly** — with normal automatic materialization, a future field
   is already resolved before `construct`; with `automaterialize=False`, the body uses explicit
   resolution as part of taking ownership of materialization.

---

## 6. Execution flow ("best of a sweep")

```
run(prod)                                  # prod uses default automaterialize=True
  │
  ├─ collect graph: prod depends on ALL candidates (the static source set of the future)
  ├─ build candidates (parallel; skipped if committed)        ← the expensive tier
  ├─ at prod's runtime, BEFORE construct (automatic materialization):
  │     read each candidate's val_accuracy (cheap, partial retrieve)
  │     winner = argmax; retrieve(winner)                      ← future-field resolution
  └─ construct prod against the concrete winner                ← the expensive downstream
```

The heavy work (candidates, downstream) are ordinary Artifacts with the usual skip/resume; the
future layer is a thin overlay of cheap reads that decides *which* concrete edge the downstream gets.

---

## 7. Summary

- **`@derived`** exposes a **cheap read** of an output as a lazy **`Future[T]`** that
  **auto-materializes on access** (partial retrieve via `reads=`, gated by `automaterialize`) — the
  analysis API.
- `Future[T]` is **lazy** (wiring-safe) with **implicit coercion** (eager-feeling in analysis);
  combinators (`fmap`/`gather`/`argmax`/`select`) build futures from futures.
- **Future-valued config fields** make a dependency data-dependent; sources are ordered statically,
  and with `automaterialize=True` the field **resolves before `construct`** at the consumer's
  runtime. With `False`, resolution/materialization belongs to the author. There are **no symbolic
  keys, resolver jobs, or freshness machinery**.
- Identity stays the consumer's **`relpath`**; artifacts may optionally record the selected winner
  as their own output; re-running clobbers/skips per the no-fingerprint model.

Next: [06-execution.md](06-execution.md).
