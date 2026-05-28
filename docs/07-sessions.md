# Sessions — `pipelines/session.py`

**Specifies:** `design/07-sessions.md` (first-class per-job shared setup; server one per machine).
**Modules:** `pipelines/session.py`; lifecycle hooks consumed by [06 execution](06-execution.md).
**Depends on:** [01 artifact](01-core-artifact.md) (`session=` setting), [08 runtime](08-runtime-helpers.md)
(`ctx`, `free_port`).
**Milestone:** M3.

A `Session` amortizes an expensive shared resource (most often a **server, one per machine**) across
many small member jobs that query it as clients. Replaces `experiments`' `@lru_cache` +
accidental-same-process trick (which broke silently under forking) and its `BatchedJudgedResponses`
graph wrapper.

---

## 1. The base class

```python
class Session:
    def group_key(self, a) -> tuple:
        """Which members may share one open session (the co-location key). Must be a pure
        function of artifact config. Default: () => all members of the session class share one."""
        return ()

    def open(self, a, ctx) -> None:
        """Start + HEALTH-CHECK the shared resource (blocks until healthy), then set attributes
        on self (self.base_url, self.proc, self.model, ...). Runs once per group, per machine.
        `a` is a representative member (for shared params like a.judge.relpath); `ctx` exposes
        free_port(), gpu_ids, log."""

    def close(self) -> None:
        """Tear down (kill server / free GPU / del object). GUARANTEED to run on success, failure,
        and preemption (try/finally around the member run)."""
```

The **Session instance is the handle**: `open()` sets attributes; members read them as
`self.session.<attr>` ([01](01-core-artifact.md) injects `session` access from `_CTX.session`). The
framework owns only the lifecycle (`group_key → open → members → close`) and the teardown guarantee —
not the handle's shape. Works for a server, an in-process object, or any shared resource.

---

## 2. Binding and member access

`@artifact(session=JudgeEngine)` records the class in `__pipelines__.session` (M-4). During execution:
- The scheduler partitions session-bound artifacts by `session_cls` then by `group_key(a)`.
- For each group, on its assigned machine: instantiate the session, `open(representative, ctx)`, run all
  member artifacts (their `materialize`/`construct`), then `close()` in a `finally`.
- Inside a member's `construct`/`@derived`, `self.session` is the open instance (set on `_CTX.session`,
  exposed by an injected `session` property on the artifact).

`examples/test` `Preview`/`PreviewSession`: `group_key` returns a constant `("configured-preview-prefix",)`,
`open` sets `self.prefix = Project.config.preview_prefix`, members read `self.session.prefix`.

`examples/em` `JudgedResponses`/`JudgeEngine`: `group_key` returns
`(judge.relpath, tensor_parallel_size, max_num_seqs, temperature, max_tokens, top_p)`; `open` retrieves
the judge and loads one `inference.GenerationModel`; members call `self.session.model.generate(...)`.

---

## 3. Slurm shape — one node allocation per group

A session-group is **one Slurm job that grabs a node**, `open()`s the resource on it, and runs its
member-clients **concurrently as local tasks** against `localhost`. Consequences (`design/07 §3`):
- **No cross-job service discovery** — members reach the resource on their own node.
- **Trivial, guaranteed teardown** — `close()` is a `try/finally` in the one job (success/failure/
  preemption).
- **`BatchPolicy` controls fan-out** — if a group has more members than one node should host, shard into
  several node-jobs, each opening its own resource for a subset. One resource per machine stays invariant.

Members do **not** share a process when the resource is a server: they are separate, fault-isolated,
parallel client processes; one failing doesn't poison the others or the server.

---

## 4. Variants

- **Server variant (recommended):** `open` starts a server (`serve_vllm(...)`, health-check via
  `wait_healthy`), members are isolated client processes. Adds sharing **and** isolation **and**
  parallelism.
- **In-process variant:** a degenerate `Session` whose `open()` loads a Python object into an attribute;
  members run in one process and call it directly (`examples/em` `JudgeEngine`, since the `inference`
  library has no server — its docstring notes only `open` changes if a server is later added). Supported,
  but the server model is the motivating one.

---

## 5. Relationship to `BatchPolicy`

`Session.group_key` defines *what may share*; `BatchPolicy` defines *limits/sharding* (how many servers,
members per server, fan-out across nodes). They compose: group by session key, then place each group's
resource + member-clients onto a node (or shard) per policy. `group_key` subsumes the old per-artifact
`batch_key`.

---

## 6. Conformance hook

- `PreviewSession`: in-process, constant `group_key`, `open` sets `prefix`, `Preview.construct` reads
  `self.session.prefix`; `close` resets. All `Preview`s in one run share one open session.
- `JudgeEngine`: in-process; `open` loads one `GenerationModel`; multiple `JudgedResponses` cells
  sharing a `group_key` reuse it; `close` releases. `dryrun` shows the session → member-clients grouping.
- `close()` runs even when a member's `construct` raises (teardown guarantee).

Next: [08-runtime-helpers.md](08-runtime-helpers.md).
