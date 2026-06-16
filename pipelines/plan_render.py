"""Render a :class:`~pipelines.execution.graph.Plan` as a condensed, readable ASCII tree.

``pipelines plan`` prints the same graph the scheduler will run, but **collapsed** so a
1000-cell sweep fits on a screen. Three collapses happen together:

1. **By type.** Every artifact class becomes one node with a count (``FinetunedModel ×4``),
   split into to-build vs already-committed, with its resource intent.
2. **By repetition.** The per-artifact DAG is projected onto a per-*node* DAG; identical
   fan-out (``A→B1``, ``A→B2``) becomes one edge with a multiplicity (``2 per parent``).
3. **Un-tangling cycles / roles.** Collapsing by class can merge two genuinely different
   *roles* of one class and even fabricate a cycle the real (acyclic) artifact graph never
   has (e.g. a ``ModelGenerations`` that *builds the corpus* a ``FinetunedModel`` trains on,
   vs one that *evaluates* the result). Any class on such a cycle is split by **artifact
   depth** (longest path from a source) — depth strictly increases along edges, so this
   provably breaks every cycle — and each piece is labeled by what distinguishes it
   (``from HFModel`` vs ``from FinetunedModel``). Non-cyclic classes stay fully collapsed.

Each node is drawn as a small multi-line block inside an indented forest, nested under its
*primary parent* (closest upstream node); a node's fan-out is stated relative to "parent"
because the nesting already shows which parent that is. Extra incoming edges (real fan-in)
and a source's downstream targets are listed on their own lines.
"""

from __future__ import annotations

import shutil
from collections import Counter, defaultdict, deque

from .artifact import has_construct

# Tree drawing.
_TEE, _ELBOW, _PIPE, _SPACE = "├─ ", "└─ ", "│  ", "   "

_RESET = "\x1b[0m"


class _Ink:
    """Applies ANSI styles when enabled, with a palette tuned to the background.

    On a dark/black background neutral text is **white** and accents use their bright
    variants (so nothing washes out to invisible); on a light background it uses the
    gray/dim palette. Accent colors stay semantic either way.
    """

    BOLD, ITAL = "1", "3"

    def __init__(self, enabled: bool, dark: bool = False):
        self.on = enabled
        self.dark = dark
        # Foreground tones adapt to the *terminal* background: dark text on a light
        # terminal, light text on a dark terminal — so text stays readable either way.
        self.MUTED = "37" if dark else "90"      # connectors, separators, secondary labels
        self.FAINT = "90" if dark else "2"        # ×count
        # Accents: bright on dark, standard on light.
        self.GPU = "93" if dark else "33"         # yellow (the scheduling bottleneck)
        self.BUILD = "92" if dark else "32"       # green
        self.FETCH = "94" if dark else "34"       # blue
        self.NAME_GPU = "95" if dark else "35"    # magenta — GPU-heavy class
        self.NAME_CPU = "96" if dark else "36"    # cyan — CPU class
        self.NAME_EXT = "94" if dark else "34"    # blue — external / fetched

    def __call__(self, text: str, *codes: str) -> str:
        if not self.on or not codes:
            return text
        return f"\x1b[{';'.join(codes)}m{text}{_RESET}"

    def chip(self, text: str) -> str:
        """A highlighted badge: text sits on its *own* dark background, so it always uses
        a very light foreground regardless of the terminal background."""
        if not self.on:
            return text
        return f"\x1b[48;5;236;97;1m {text} {_RESET}"   # dark-gray bg, bright-white text


# --------------------------------------------------------------------------- #
# Resource helpers
# --------------------------------------------------------------------------- #

def _req(annotations: dict | None) -> tuple[int, int]:
    ann = annotations or {}
    gpus = int(ann.get("gpus", 0) or 0)
    cpus = int(ann.get("cpus", 0) or 0) or 1
    return gpus, cpus


def _range(values: list[int]) -> str:
    lo, hi = min(values), max(values)
    return str(lo) if lo == hi else f"{lo}–{hi}"


# --------------------------------------------------------------------------- #
# The condensed node-graph
# --------------------------------------------------------------------------- #

class _Graph:
    def __init__(self, plan, ink: _Ink):
        self.ink = ink
        self.arts = list(plan.ordered)
        self.satisfied = {id(a) for a in plan.satisfied}
        # Transient artifacts pruned because nothing that will run depends on them.
        self.skipped = {id(a) for a in getattr(plan, "transient_skipped", ())}
        self.universe = {a.relpath: a for a in self.arts}
        self.cls = {a.relpath: type(a).__name__ for a in self.arts}

        self._depth_memo: dict[str, int] = {}
        split = self._cyclic_classes()

        self.key = {rp: (f"{self.cls[rp]}@{self._depth(self.universe[rp])}"
                         if self.cls[rp] in split else self.cls[rp])
                    for rp in self.universe}

        self.by_node: dict[str, list] = defaultdict(list)
        for a in self.arts:
            self.by_node[self.key[a.relpath]].append(a)
        self.nodes = list(self.by_node)

        self.class_nodes: dict[str, list] = defaultdict(list)
        for n in self.nodes:
            self.class_nodes[_cls_of(n)].append(n)

        self.edge_n: Counter = Counter()
        self.par_children: dict = defaultdict(lambda: defaultdict(int))
        self.chl_parents: dict = defaultdict(lambda: defaultdict(int))
        self.node_parents: dict[str, set] = defaultdict(set)
        self.node_sig: dict[str, set] = defaultdict(set)
        for a in self.arts:
            cn = self.key[a.relpath]
            for d in a.dependencies():
                if d.relpath not in self.universe:
                    continue
                pn = self.key[d.relpath]
                self.edge_n[(pn, cn)] += 1
                self.par_children[(pn, cn)][d.relpath] += 1
                self.chl_parents[(pn, cn)][a.relpath] += 1
                self.node_sig[cn].add(self.cls[d.relpath])
                if pn != cn:
                    self.node_parents[cn].add(pn)

        self.display, self.variant = self._names()
        self.tier = {n: max(self._depth(a) for a in arts) for n, arts in self.by_node.items()}
        self.primary, self.children_of, self.roots = self._forest()

    # --- depth + cycles ---------------------------------------------------- #
    def _depth(self, a) -> int:
        rp = a.relpath
        if rp in self._depth_memo:
            return self._depth_memo[rp]
        self._depth_memo[rp] = 0
        best = 0
        for d in a.dependencies():
            if d.relpath in self.universe:
                best = max(best, 1 + self._depth(self.universe[d.relpath]))
        self._depth_memo[rp] = best
        return best

    def _cyclic_classes(self) -> set[str]:
        succ: dict[str, set] = defaultdict(set)
        for a in self.arts:
            for d in a.dependencies():
                if d.relpath in self.universe:
                    succ[self.cls[d.relpath]].add(self.cls[a.relpath])
        on_cycle: set[str] = set()
        for start in set(self.cls.values()):
            seen, q = set(), deque(succ[start])
            while q:
                c = q.popleft()
                if c == start:
                    on_cycle.add(start)
                    break
                if c not in seen:
                    seen.add(c)
                    q.extend(succ[c])
        return on_cycle

    # --- names ------------------------------------------------------------- #
    def _names(self):
        """Per node: a unique full display name and a short ``variant`` qualifier ('' if the
        class isn't split)."""
        display, variant = {}, {}
        for cls, sibs in self.class_nodes.items():
            if len(sibs) == 1:
                display[sibs[0]], variant[sibs[0]] = cls, ""
                continue
            common = set.intersection(*(self.node_sig[s] for s in sibs))
            tags = {s: ", ".join(sorted(self.node_sig[s] - common)) for s in sibs}
            if all(tags.values()) and len(set(tags.values())) == len(sibs):
                for s in sibs:
                    variant[s] = f"from {tags[s]}"
            else:
                for rank, s in enumerate(sorted(sibs, key=_depth_of), start=1):
                    variant[s] = f"stage {rank}"
            for s in sibs:
                display[s] = f"{cls} ({variant[s]})"
        return display, variant

    def disp(self, node: str) -> str:
        return self.display.get(node, _cls_of(node))

    # --- layout ------------------------------------------------------------ #
    def _forest(self):
        primary: dict[str, str | None] = {}
        children_of: dict[str, list] = defaultdict(list)
        roots: list[str] = []
        for c in self.nodes:
            parents = self.node_parents.get(c, set())
            if not parents:
                primary[c] = None
                roots.append(c)
            else:
                p = max(parents, key=lambda p: (self.tier[p], self.edge_n[(p, c)], p))
                primary[c] = p
                children_of[p].append(c)
        return primary, children_of, roots

    # --- per-node styling -------------------------------------------------- #
    def _external(self, n: str) -> bool:
        return not has_construct(self.by_node[n][0])

    def _color(self, n: str) -> str:
        ink = self.ink
        if self._external(n):
            return ink.NAME_EXT
        gpus = [_req(a.annotations)[0] for a in self.by_node[n]]
        return ink.NAME_GPU if max(gpus) > 0 else ink.NAME_CPU

    def _resources(self, n: str) -> str:
        ink = self.ink
        arts = self.by_node[n]
        gpus = [_req(a.annotations)[0] for a in arts]
        cpus = [_req(a.annotations)[1] for a in arts]
        parts = []
        if max(gpus) > 0:
            parts.append(ink(f"{_range(gpus)} GPU", ink.GPU, ink.BOLD))
        parts.append(ink(f"{_range(cpus)} CPU", ink.MUTED))
        return ink(" · ", ink.MUTED).join(parts)

    def _state(self, n: str) -> str:
        ink = self.ink
        arts = self.by_node[n]
        total = len(arts)
        committed = sum(1 for a in arts if id(a) in self.satisfied)
        skipped = sum(1 for a in arts if id(a) in self.skipped)
        build = total - committed - skipped
        if committed == total:
            return ink("committed", ink.MUTED)
        if skipped == total:
            return ink("transient (skipped)", ink.MUTED)
        if build == total:
            word, color = ("fetch", ink.FETCH) if self._external(n) else ("build", ink.BUILD)
            return ink(word, color)
        parts = []
        if build:
            word = "fetch" if self._external(n) else "build"
            parts.append(ink(f"{build} {word}", ink.BUILD))
        if committed:
            parts.append(ink(f"{committed} committed", ink.MUTED))
        if skipped:
            parts.append(ink(f"{skipped} transient", ink.MUTED))
        return ink(" · ", ink.MUTED).join(parts)

    def _names_join(self, nodes) -> str:
        ink = self.ink
        return ink(", ", ink.MUTED).join(ink(self.disp(x), ink.MUTED) for x in nodes)

    def node_block(self, n: str, is_root: bool) -> list[str]:
        """The styled lines for one node (title first, then detail rows)."""
        ink = self.ink
        arts = self.by_node[n]
        color = self._color(n)
        glyph = "⬡" if self._external(n) else "◆"
        title = (f"{ink(glyph, color)} {ink(_cls_of(n), ink.BOLD, color)} "
                 f"{ink('×' + str(len(arts)), ink.FAINT)}")
        lines = [title]

        if self.variant.get(n):
            lines.append(ink.chip(self.variant[n]))       # dark-bg badge, very light text

        lines.append(f"{self._state(n)} {ink('·', ink.MUTED)} {self._resources(n)}")

        links = []
        if not is_root:
            ratio = _range(list(self.par_children[(self.primary[n], n)].values()))
            links.append(ink(f"{ratio}× per parent", ink.FAINT))
        extra = sorted((p for p in self.node_parents.get(n, set()) if p != self.primary[n]),
                       key=self.disp)
        if extra:
            links.append(ink("also ← ", ink.MUTED) + self._names_join(extra))
        if links:
            lines.append(ink("↳ ", ink.MUTED) + ink(" · ", ink.MUTED).join(links))

        if not self.children_of.get(n):
            targets = sorted({c for (p, c) in self.edge_n if p == n and c != n}, key=self.disp)
            if targets:
                lines.append(ink("→ feeds ", ink.MUTED) + self._names_join(targets))
        return lines


def _cls_of(node: str) -> str:
    return node.split("@", 1)[0]


def _depth_of(node: str) -> int:
    return int(node.split("@", 1)[1]) if "@" in node else 0


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #

def render_plan(plan, *, expand: bool = False, width: int | None = None,
                color: bool = False, dark: bool = False) -> str:
    ink = _Ink(color, dark)
    g = _Graph(plan, ink)
    width = width or shutil.get_terminal_size((100, 24)).columns
    out: list[str] = []

    out.append(_header(g, width, ink))
    out.append("")

    pending = (sorted(g.roots, key=lambda n: (g.tier[n], g.disp(n)))
               + sorted(g.nodes, key=lambda n: (g.tier[n], g.disp(n))))
    rendered: set[str] = set()
    first = True
    for r in pending:
        if r in rendered:
            continue
        if not first:
            out.append("")
        first = False
        _walk(g, r, prefix="", is_root=True, is_last=True,
              out=out, expand=expand, rendered=rendered)

    out.append("")
    out.append(_footer(g, ink))
    return "\n".join(out)


def _walk(g: _Graph, n: str, *, prefix: str, is_root: bool, is_last: bool,
          out: list, expand: bool, rendered: set) -> None:
    rendered.add(n)
    ink = g.ink
    block = g.node_block(n, is_root)
    if is_root:
        out.append(block[0])
        cont = ""
    else:
        branch = _ELBOW if is_last else _TEE
        out.append(prefix + ink(branch, ink.MUTED) + block[0])
        cont = prefix + (_SPACE if is_last else ink(_PIPE, ink.MUTED))

    detail = cont + "  "
    for d in block[1:]:
        out.append(detail + d)

    if expand:
        _expand_instances(g, n, detail, out)

    kids = sorted(g.children_of.get(n, []), key=lambda k: (g.tier[k], g.disp(k)))
    if kids:
        out.append(cont)                                  # thin spacer line (just the rails)
    for i, k in enumerate(kids):
        last = i == len(kids) - 1
        if k in rendered:
            branch = _ELBOW if last else _TEE
            out.append(cont + ink(branch, ink.MUTED)
                       + ink(f"{g.disp(k)} ×{len(g.by_node[k])}  ↑ (shown above)", ink.FAINT))
            continue
        _walk(g, k, prefix=cont, is_root=False, is_last=last,
              out=out, expand=expand, rendered=rendered)


def _expand_instances(g: _Graph, n: str, prefix: str, out: list, limit: int = 6) -> None:
    ink = g.ink
    arts = sorted(g.by_node[n], key=lambda a: a.relpath)
    for a in arts[:limit]:
        mark = "·" if id(a) in g.satisfied else "⊘" if id(a) in g.skipped else "▸"
        out.append(prefix + ink(f"{mark} {a.relpath}", ink.FAINT))
    if len(arts) > limit:
        out.append(prefix + ink(f"  … (+{len(arts) - limit} more)", ink.FAINT))


# --------------------------------------------------------------------------- #
# Header / footer
# --------------------------------------------------------------------------- #

def _header(g: _Graph, width: int, ink: _Ink) -> str:
    n = len(g.arts)
    committed = len(g.satisfied)
    skipped = len(g.skipped)
    build = n - committed - skipped
    classes = len(set(g.cls.values()))
    gpu = sum(_req(a.annotations)[0] for a in g.arts
              if id(a) not in g.satisfied and id(a) not in g.skipped)
    title = "pipelines plan"
    nodes_note = f" ({len(g.nodes)} nodes)" if len(g.nodes) != classes else ""
    s1 = f"{n} artifacts · {classes} types{nodes_note}"
    s2 = f"{build} to build · {committed} committed"
    if skipped:
        s2 += f" · {skipped} transient skipped"
    s2 += f" · {gpu} GPU-slots"
    inner = max(len(title) + 1, len(s1), len(s2))
    inner = min(inner, max(len(title) + 1, width - 4))
    top = (ink("╭─ ", ink.MUTED) + ink(title, ink.BOLD)
           + ink(" " + "─" * (inner - len(title) - 1) + "╮", ink.MUTED))
    mid = [ink("│ ", ink.MUTED) + s[:inner].ljust(inner) + ink(" │", ink.MUTED) for s in (s1, s2)]
    bot = ink("╰" + "─" * (inner + 2) + "╯", ink.MUTED)
    return "\n".join([top, *mid, bot])


def _footer(g: _Graph, ink: _Ink) -> str:
    rows = []
    for nd in sorted(g.nodes, key=lambda n: (-g.tier[n], g.disp(n))):
        arts = g.by_node[nd]
        build = sum(1 for a in arts if id(a) not in g.satisfied and id(a) not in g.skipped)
        rows.append((g.disp(nd), len(arts), build, g._color(nd)))
    wt = max((len(r[0]) for r in rows), default=4)
    lines = [ink("summary  (downstream → upstream)", ink.BOLD)]
    for name, total, build, color in rows:
        lines.append("  " + ink(name.ljust(wt), color)
                     + ink(f"  ×{total:<4}", ink.FAINT)
                     + ink(f"{build:>3} to build", ink.MUTED))
    return "\n".join(lines)
