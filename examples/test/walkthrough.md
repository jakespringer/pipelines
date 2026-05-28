# Walkthrough — running the `test` pipeline one stage at a time

This is a real, captured transcript of building the `test` example incrementally with the
`pipelines` framework. Each step shows the **command**, its **output**, and the **resulting
on-disk state** (directory trees and file contents) so you can see exactly what each stage does.

It exercises the full feature set: external inputs, plain-Python construction, a per-artifact
storage override, a shared `Session`, `@derived` values, `argmax` selection over a sweep (both
automatic and manual future resolution), a hand-committed bundle, an always-rebuilt audit, and
skip/resume.

---

## Setup

Three storage roots come from configuration (`Project.config`, layered TOML — see
[`README.md`](README.md) and `design/10-configuration.md`). For this transcript they all point at
local `file://` directories via a system overlay, so no cloud services are needed:

| Variable used below | Config key | What it holds |
|---|---|---|
| `$STORE` | `remote_store` | the default Store — ordinary artifacts commit here |
| `$INDEX` | `local_index_store` | the alternate Store `WordIndex` selects via `@artifact(store=...)` |
| `$WORK`  | `base_path` | local scratch where `construct` writes and dependencies materialize |

```toml
# ~/.config/pipelines/projects/test.toml   (system overlay; values shown abbreviated below)
[config]
remote_store      = "file://$STORE"
local_index_store = "file://$INDEX"
base_path         = "$WORK"
```

> **Invocation.** The canonical command is `python -m examples.test.run <verb> <selector>` run
> from the repo root. (In the environment this transcript was captured in, the `examples` name was
> shadowed by an unrelated installed package, so a tiny shim ran `examples/test/run.py` unchanged;
> the outputs below are verbatim. Absolute path prefixes are abbreviated to `$STORE`/`$INDEX`/`$WORK`
> for readability — relative `relpath`s in the logs are shown exactly.)

The pipeline shape (`relpath`s, the identity/location of each artifact):

```
LocalDocument(source/<name>)                          # external: retrieved, not committed
  -> NormalizedText(normalized/<name>/<case>/remove-…) # committed to $STORE
       -> WordIndex(index/…/min-<k>)                   # committed to $INDEX (store override)
            -> Preview(preview/<label>/…)              # committed to $STORE (uses a Session)
WordIndex candidates --argmax(unique_words)--> WinnerReport / ManualWinnerReport
                                                  -> PublishedBundle -> AuditMarker
```

We start with an **empty store**.

---

## Stage 0 — Preview the plan (`dryrun`)

`dryrun` collects the graph, orders it, and runs the freshness check — without building anything.
Asking for a single preview shows that its dependencies are pulled in automatically.

```bash
$ python -m examples.test.run dryrun 'preview/alpha-lower-min1/*'
```

```
# plan: 4 to build, 0 committed
  [build] source/alpha  ->  $WORK/source/alpha
  [build] normalized/alpha/lower/remove-the-and  ->  $WORK/normalized/alpha/lower/remove-the-and
  [build] index/normalized/alpha/lower/remove-the-and/min-1  ->  $WORK/index/normalized/alpha/lower/remove-the-and/min-1
  [build] preview/alpha-lower-min1/index/normalized/alpha/lower/remove-the-and/min-1  ->  $WORK/preview/alpha-lower-min1/index/normalized/alpha/lower/remove-the-and/min-1
```

The plan is in **topological order**: the source feeds the normalized text, which feeds the index,
which feeds the preview. `relpath` is a pure function of config, so the plan is deterministic — run
`dryrun` twice and you get byte-identical output.

---

## Stage 1 — Build a normalized document

We materialize one `NormalizedText`. Its only dependency, the external `LocalDocument`, is built
(retrieved) first, autonomously.

```bash
$ python -m examples.test.run run 'normalized/alpha/lower/remove-the-and'
```

```
built source/alpha
built normalized/alpha/lower/remove-the-and
```

On disk afterwards:

```bash
$ find $STORE -type f          # the durable default Store
$STORE/normalized/alpha/lower/remove-the-and/text.txt

$ find $WORK -type f           # local scratch (base_path)
$WORK/normalized/alpha/lower/remove-the-and/text.txt
$WORK/source/alpha/text.txt
```

Note the difference: the **external** `source/alpha` was retrieved into local scratch (`$WORK`) but
**not committed** to the Store — externals are fetched on demand, not published. The constructed
`normalized/...` was committed to `$STORE`.

The construction (lowercasing and dropping the stop-words `the`, `and`) is plain Python in
[`steps.py`](steps.py):

```bash
$ cat $STORE/normalized/alpha/lower/remove-the-and/text.txt
red apple blue berry apple pies are bright sweet

# from the original input examples/test/data/alpha/text.txt:
#   The red apple and the blue berry.
#   Apple pies are bright and sweet.
```

---

## Stage 2 — Build the word index (per-artifact store override)

`WordIndex` declares `@artifact(store=local_index_store)`, so it commits to `$INDEX`, not the
default Store. Its dependency `normalized/...` is already committed, so it is **skipped**.

```bash
$ python -m examples.test.run run 'index/normalized/alpha/lower/remove-the-and/min-1'
```

```
built source/alpha
skip (committed) normalized/alpha/lower/remove-the-and
built index/normalized/alpha/lower/remove-the-and/min-1
```

The new output landed in the **index** store; the default store is unchanged:

```bash
$ find $STORE -type f
$STORE/normalized/alpha/lower/remove-the-and/text.txt          # (unchanged)

$ find $INDEX -type f
$INDEX/index/normalized/alpha/lower/remove-the-and/min-1/metrics.json
$INDEX/index/normalized/alpha/lower/remove-the-and/min-1/words.tsv
```

```bash
$ cat $INDEX/index/normalized/alpha/lower/remove-the-and/min-1/words.tsv
word	count
apple	2
are	1
berry	1
blue	1
bright	1
pies	1
red	1
sweet	1

$ cat $INDEX/index/normalized/alpha/lower/remove-the-and/min-1/metrics.json
{
  "min_length": 1,
  "most_common": "apple",
  "tokens": 9,
  "unique_words": 8
}
```

`metrics.json` is what a `@derived` value reads — `WordIndex.unique_words` returns `8` here without
re-running the index (Stage 4 uses it).

---

## Stage 3 — Render a preview (shared `Session`)

`Preview` is bound to `PreviewSession` (`@artifact(session=PreviewSession)`). The executor opens the
session once, the session reads `Project.config.preview_prefix` and exposes it as `self.session.prefix`,
and the preview's `construct` uses it. The committed `normalized`/`index` deps are skipped.

```bash
$ python -m examples.test.run run 'preview/alpha-lower-min1/*'
```

```
built source/alpha
skip (committed) normalized/alpha/lower/remove-the-and
skip (committed) index/normalized/alpha/lower/remove-the-and/min-1
built preview/alpha-lower-min1/index/normalized/alpha/lower/remove-the-and/min-1
```

```bash
$ cat $STORE/preview/alpha-lower-min1/.../preview.txt
[test-preview] alpha-lower-min1
apple	2
are	1
berry	1
blue	1
bright	1
```

The `[test-preview]` prefix came from the session (sourced from config:
`preview_prefix = "[test-preview]"`), demonstrating shared setup that members read at construction time.

---

## Stage 4 — Select the best of a sweep (`argmax` over `@derived`)

The `selection` experiment builds **all** `WordIndex` candidates (every document × case × min-length,
plus the merged-all-lower index), then `argmax(candidates, key=lambda i: i.unique_words)` picks the
one with the most unique words. The winner becomes a field of two reports:

- `WinnerReport` — the default path: the framework resolves the future **before** `construct`, so
  `self.best` is already concrete.
- `ManualWinnerReport` — `@artifact(automaterialize=False)`: its body resolves the future itself with
  `self.best.result()` and partial-retrieves only `metrics.json`.

```bash
$ python -m examples.test.run run selection
```

```
built source/alpha
skip (committed) normalized/alpha/lower/remove-the-and
skip (committed) index/normalized/alpha/lower/remove-the-and/min-1
built index/normalized/alpha/lower/remove-the-and/min-5
built normalized/alpha/upper/remove-the-and
built index/normalized/alpha/upper/remove-the-and/min-1
...                                            # the remaining candidates build here
built winner/automatic/most-unique
built winner/manual/most-unique
built published/most-unique
built audit/most-unique
```

Both resolution paths select the same winner — `index/merged/all-lower/min-1`, with 24 unique words
(the merged document is the largest vocabulary):

```bash
$ cat $STORE/winner/automatic/most-unique/report.json
{
  "metrics": { "min_length": 1, "most_common": "apple", "tokens": 29, "unique_words": 24 },
  "mode": "automatic",
  "winner": "index/merged/all-lower/min-1"
}

$ cat $STORE/winner/manual/most-unique/report.json
{
  "metrics": { "min_length": 1, "most_common": "apple", "tokens": 29, "unique_words": 24 },
  "mode": "manual",
  "winner": "index/merged/all-lower/min-1"
}
```

The selection identity is the consumer's stable `relpath` (`winner/automatic/most-unique`), regardless
of which candidate wins — so re-running won't auto-detect a different winner (see Stage 6).

---

## Stage 5 — Publish a bundle (manual commit) and audit it (uncached)

`PublishedBundle` is `@artifact(autocommit=False)`: it builds the bundle, writes its **own** optional
`meta.json`, and calls `self.commit()` by hand. `AuditMarker` is `@artifact(cache=False)`: it never
skips. Both were built at the end of Stage 4; here are their outputs:

```bash
$ find $STORE/published -type f
$STORE/published/most-unique/bundle.json
$STORE/published/most-unique/meta.json     # user-authored, optional — the framework never writes it

$ cat $STORE/published/most-unique/bundle.json
{
  "automatic": { "metrics": {...}, "mode": "automatic", "winner": "index/merged/all-lower/min-1" },
  "manual":    { "metrics": {...}, "mode": "manual",    "winner": "index/merged/all-lower/min-1" },
  "same_winner": true
}

$ cat $STORE/published/most-unique/meta.json
{
  "note": "Optional metadata authored by PublishedBundle."
}

$ cat $STORE/audit/most-unique/audit.json
{
  "bundle_matches": true,
  "observed_at": "2026-05-26T21:14:58.343442+00:00"
}
```

---

## Stage 6 — Resume (skip committed; rebuild `cache=False`)

Re-running `selection` does almost nothing: every committed artifact is skipped. Only `AuditMarker`
runs again, because `cache=False` means "always rebuild" — the scheduler doesn't even check existence.

```bash
$ python -m examples.test.run run selection
```

```
skip (committed) winner/automatic/most-unique
skip (committed) winner/manual/most-unique
skip (committed) published/most-unique
built audit/most-unique
```

The audit's timestamp advanced, confirming it truly re-ran (the others did not):

```bash
$ cat $STORE/audit/most-unique/audit.json
{
  "bundle_matches": true,
  "observed_at": "2026-05-26T21:15:05.705167+00:00"   # changed: 21:14:58 -> 21:15:05
}
```

This is the whole skip/resume model: it is **existence-based**, with no content fingerprint. To force
a fresh build of a normally-cached artifact, delete its output (`rm -r $STORE/<relpath>`) or use
`@artifact(cache=False)` as the audit does.

---

## Final state

After the stages above, the two Stores hold:

```bash
$ find $STORE -maxdepth 2 -type d        # default Store
$STORE/normalized/{alpha,beta,gamma}
$STORE/merged/all-lower
$STORE/preview/alpha-lower-min1
$STORE/winner/{automatic,manual}
$STORE/published/most-unique
$STORE/audit/most-unique

$ find $INDEX -type f | wc -l            # alternate Store (WordIndex)
14 indexes                               # one words.tsv + metrics.json per candidate
```

Everything is browsable by `relpath` with ordinary `ls`/`cat`/`rm`; the layout *is* the interface.

### Other things to try

```bash
python -m examples.test.run dryrun smoke          # the smoke group: one preview + the bundle
python -m examples.test.run run variants          # all previews across documents × cases × lengths
python -m examples.test.run run merge             # the merged-document branch
python -m examples.test.run run all               # the whole instantiated universe
python -m examples.test.run run 'index/*min-5'    # a bare relpath glob over the universe
```
