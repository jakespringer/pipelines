# Sessions — First-Class Per-Job Shared Setup

Real ML pipelines often need to **amortize an expensive shared resource** across many small units of
work — load a 27B judge once and score thousands of generations, stand up an embedding server many
small jobs query. A `Session` makes that a **first-class, guaranteed** mechanism, replacing the
`@lru_cache` + accidental same-process trick that `experiments`/v1 relied on (which silently broke
when the executor forked).

---

## 1. What a Session is — a shared server, one per machine

The motivating model: a `Session` manages a shared resource — most commonly a **server** (e.g. a
vLLM server) **started once per machine** — that individual member jobs **query as clients**.

```python
from pipelines import Session, artifact, source
from pipelines.runtime import free_port, serve_vllm, openai_client

class VLLMServer(Session):
    def group_key(self, a) -> tuple:               # which members share one server
        return (a.judge.relpath, a.tensor_parallel_size)

    def open(self, a, ctx) -> None:                # once per machine: start + health-check
        a.judge.retrieve()                          # shared weights pulled once, on this node
        self.proc     = serve_vllm(a.judge.path, tp=a.tensor_parallel_size, port=free_port())
        self.base_url = wait_healthy(self.proc)     # blocks until the server is ready

    def close(self) -> None:                        # after the last member; frees the GPU
        self.proc.stop()

@artifact(session=VLLMServer)
class JudgedResponses:
    judge: HFModel
    generations: ModelGenerations
    rubric_key: str
    @property
    def relpath(self) -> str:
        return f"judged/{self.generations.relpath}/{self.rubric_key}"
    def construct(self) -> None:
        judged = openai_client(self.session.base_url).chat(judge_inputs(self.generations.path, ...))
        join_judgements(self.generations.path, judged, out=self.path)
```

**Members do not share a process.** They are clients of the server, so they run as **separate,
fault-isolated processes, in parallel** on the machine; the server handles concurrent requests. This
is strictly better than in-process sharing: sharing **and** isolation **and** parallelism at once.

---

## 2. The API — the Session instance is the handle

A `Session` subclass defines three methods; members read attributes that `open()` set
(`self.session.<attr>`):

| Method | Role |
|--------|------|
| `group_key(self, a) -> tuple` | which members may share one open session (the co-location key). Subsumes the example's `batch_key` — grouping intent lives on the `Session`, not on the artifact's config. |
| `open(self, a, ctx) -> None` | start + **health-check** the server (blocks until healthy), then set attributes (`self.base_url`, `self.proc`, …). Receives a representative member `a` (for shared params like `a.judge.relpath`) and `ctx` (`free_port()`, `gpu_ids`, `log`). Runs once per group. |
| `close(self) -> None` | tear down (kill the server / free the GPU). **Guaranteed** to run on success, failure, **and preemption** (a `try/finally` around the member run) — no leaked GPU servers. |

The framework owns only the **lifecycle** (`group_key` → `open` → members → `close`) and the
teardown guarantee — not the handle's shape. This works for a server, an in-process object, or any
shared resource.

---

## 3. Slurm shape — one node allocation per session-group

A session-group is **one Slurm job that grabs a node**, `open()`s the server on it, and runs its
member-clients **concurrently as local tasks** against `localhost`. Consequences:

- **No cross-job service discovery** — members reach the server on their own node.
- **Trivial, guaranteed teardown** — `close()` is a `try/finally` in the one job, covering success,
  failure, and preemption.
- **`BatchPolicy` controls fan-out** — if a group has more members than one node should host (or for
  throughput), shard into several node-jobs, each `open`ing its own server for a subset of members.
  One server per machine remains the invariant.

---

## 4. Relationship to `BatchPolicy`

`Session.group_key` defines *what may share a server*; `BatchPolicy` defines *limits/sharding* (how
many servers, how many members per server, fan-out across nodes). They compose: group by session
key, then place each group's server + member-clients onto a node (or shard) per policy.

---

## 5. In-process variant

An in-process shared object (the old `@lru_cache` engine) is just a degenerate `Session` whose
`open()` loads a Python object into an attribute and whose members *do* run in one process. It stays
supported, but the **server-per-machine** model above is the motivating, recommended one — it adds
isolation and parallelism the in-process model can't.

---

## 6. Why this fixes the problem

The same-process guarantee the `@lru_cache` hack silently needed is now an explicit contract: the
expensive load happens in `open()` exactly once per machine, members are co-scheduled and reach it,
`close()` is guaranteed, and `dryrun` can show server → member-clients before you burn hours. Because
members are independent client processes, one failing doesn't poison the others or the server. No
`BatchedJudgedResponses` graph wrapper, no `contained_artifacts`, no member→producer remapping — the
graph stays clean; sharing is purely *how* a batch is launched.

---

## 7. Summary

- A **`Session`** is first-class shared setup; the motivating case is a **server, one per machine**,
  queried by isolated, parallel **member-client** jobs.
- API: **the Session instance is the handle** — `group_key` / `open(a, ctx)` (start + health-check,
  set attributes) / `close` (guaranteed teardown). Members read `self.session.<attr>`.
- **One node allocation per group**; `BatchPolicy` fans out. `group_key` subsumes the old `batch_key`.
- Replaces `@lru_cache`-by-luck with a guaranteed contract; the in-process object is a degenerate
  case.

Next: [08-runtime-and-cluster.md](08-runtime-and-cluster.md).
