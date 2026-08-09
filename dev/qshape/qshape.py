#!/usr/bin/env python3
"""qshape: render a distributed query shape as SVG.

Input:  YAML describing stages, execs, operator bands, slice grids, shuffles.
Output: SVG.

v0.1 scope: stage / exec / op-band / slice-cell grid + dashed shuffle edges.
Not yet: shuffle-block batch cells, K-way glyph, bracketed read windows.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


CELL_W = 46
CELL_H = 24
OP_ROW_H = 34
ORRE_ROW_H = 62   # taller row for ORRE so the N→N crossing arrows have space
SW_ROW_H = 68     # taller row for ShuffleWriter so batch sub-cells fit
HIGHLIGHT_READER_K = 1   # example reader whose read windows we highlight with brackets
HIGHLIGHT_COLOR = "#e07a00"
EXEC_PAD = 10
EXEC_LABEL_H = 18
STAGE_PAD = 14
STAGE_LABEL_H = 22
STAGE_GAP = 90
OP_LABEL_W = 260
FONT = "ui-monospace, SFMono-Regular, Menlo, monospace"
FS_SMALL = 10
FS_OP = 12
FS_STAGE = 14


@dataclass
class Exec:
    id: str
    slices: int


@dataclass
class Op:
    kind: str
    label: str
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Stage:
    id: str
    label: str
    execs: list[Exec]
    ops: list[Op]
    input_desc: str | None = None
    sink: str | None = None


@dataclass
class Doc:
    title: str
    K: int
    stages: list[Stage]
    shuffles: dict[str, dict]


def load(path: Path) -> Doc:
    data = yaml.safe_load(path.read_text())
    stages = []
    for s in data.get("stages", []):
        execs = [Exec(id=e["id"], slices=int(e["slices"])) for e in s["execs"]]
        ops = []
        for o in s.get("ops", []):
            kind = o.pop("kind")
            label = o.pop("label", kind)
            ops.append(Op(kind=kind, label=label, extra=o))
        input_desc = None
        if "input" in s:
            inp = s["input"]
            input_desc = f"from {inp['from']} · {inp.get('merge', 'concat')}"
        stages.append(
            Stage(
                id=s["id"],
                label=s.get("label", s["id"]),
                execs=execs,
                ops=ops,
                input_desc=input_desc,
                sink=s.get("sink"),
            )
        )
    return Doc(
        title=data.get("title", ""),
        K=int(data.get("K", 0)),
        stages=stages,
        shuffles=data.get("shuffles", {}) or {},
    )


def exec_width(ex: Exec) -> int:
    return ex.slices * CELL_W + 2 * EXEC_PAD


def stage_inner_width(stage: Stage) -> int:
    return sum(exec_width(e) for e in stage.execs) + EXEC_PAD * (len(stage.execs) - 1)


def stage_width(stage: Stage) -> int:
    return OP_LABEL_W + stage_inner_width(stage) + 2 * STAGE_PAD


def row_height(op: Op) -> int:
    if op.kind == "orre":
        return ORRE_ROW_H
    if op.kind == "shuffle_write":
        return SW_ROW_H
    return OP_ROW_H


def stage_height(stage: Stage) -> int:
    return STAGE_LABEL_H + EXEC_LABEL_H + sum(row_height(o) for o in stage.ops) + 2 * STAGE_PAD


def rect(x, y, w, h, fill="#fff", stroke="#333", sw=1, rx=4) -> str:
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'


def text(x, y, s, size=FS_SMALL, anchor="middle", weight="normal", fill="#111") -> str:
    return (
        f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" '
        f'font-weight="{weight}" text-anchor="{anchor}" dominant-baseline="middle" fill="{fill}">{s}</text>'
    )


def line(x1, y1, x2, y2, stroke="#555", sw=1, dash=None) -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{stroke}" stroke-width="{sw}"{d}/>'


OP_FILL = {
    "scan": "#eef",
    "sort": "#efe",
    "runtime_stats": "#fff5d6",
    "orre": "#ffdede",
    "shuffle_write": "#e2f0ff",
    "range_filter": "#f0e6ff",
    "partitioned_bwag": "#ffe6cc",
    "projection": "#eeeeee",
}


def render(doc: Doc) -> str:
    # Layout: stages stacked vertically, source at BOTTOM, sink at TOP.
    # (matches user's mental model — execution flows up.)
    ordered = list(reversed(doc.stages))  # so index 0 is topmost
    max_w = max(stage_width(s) for s in doc.stages)

    y_cursor = 40  # title space
    if doc.title:
        pass  # rendered at top later

    stage_positions = []
    for s in ordered:
        y_cursor += 0 if not stage_positions else STAGE_GAP
        sh = stage_height(s)
        x = (max_w - stage_width(s)) // 2 + 20
        stage_positions.append((s, x, y_cursor))
        y_cursor += sh

    canvas_w = max_w + 40
    canvas_h = y_cursor + 40

    parts: list[str] = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {canvas_w} {canvas_h}" width="{canvas_w}" height="{canvas_h}">'
    )
    parts.append(f'<rect width="{canvas_w}" height="{canvas_h}" fill="#fafafa"/>')

    if doc.title:
        parts.append(text(canvas_w // 2, 20, doc.title, size=FS_STAGE, weight="bold"))

    # Client sink glyph at top if any stage sinks to client
    top_stage = ordered[0]
    if top_stage.sink == "client":
        cx, cy = canvas_w // 2, stage_positions[0][2] - STAGE_GAP // 2
        parts.append(f'<circle cx="{cx}" cy="{cy}" r="16" fill="#fff" stroke="#333"/>')
        parts.append(text(cx, cy, "client", size=FS_SMALL))

    # Track per-slice (writer/reader) anchor points to draw shuffle edges.
    # Each anchor: (exec_idx, local_slot, x, y). Local slot is the per-exec position.
    # Edge logic uses local slot (not global k) to pick sparse fan-in, so the halo
    # extends into each exec independently — random-uniform + isomorphic ORRE means
    # every exec produces the same value-range partitions, so reader at local slot p
    # in one exec must also fetch from local slot p in EVERY OTHER exec (mirror).
    writer_anchors: dict[str, list[tuple[int, int, int, int]]] = {}
    reader_anchors: dict[str, list[tuple[int, int, int, int]]] = {}

    for s, sx, sy in stage_positions:
        # Stage frame
        sw_ = stage_width(s)
        sh_ = stage_height(s)
        parts.append(rect(sx, sy, sw_, sh_, fill="#f4f4fb", stroke="#556", sw=1.5))
        parts.append(text(sx + sw_ // 2, sy + 12, f"{s.label}  (K={doc.K})", size=FS_STAGE, weight="bold"))

        # Exec label row
        ex_y = sy + STAGE_LABEL_H + STAGE_PAD
        ex_x = sx + OP_LABEL_W
        exec_frames: list[tuple[Exec, int, int]] = []  # (exec, x0, x_slices_start)
        for ex in s.execs:
            ex_w = exec_width(ex)
            parts.append(text(ex_x + ex_w // 2, ex_y + EXEC_LABEL_H // 2, f"— {ex.id} —", size=FS_SMALL, weight="bold"))
            exec_frames.append((ex, ex_x, ex_x + EXEC_PAD))
            ex_x += ex_w + EXEC_PAD

        # Op rows
        op_top = ex_y + EXEC_LABEL_H
        # Order visually: op 0 at BOTTOM of stage (closest to source),
        # last op at TOP of stage (closest to output). Since we already flipped stage order,
        # inside a stage we also flip so index 0 is bottom row.
        row_y_by_idx: list[tuple[int, int]] = []  # (row_y, row_h) per visual row
        row_y_cursor = op_top
        rev_ops = list(reversed(s.ops))
        for row_idx, op in enumerate(rev_ops):
            rh = row_height(op)
            row_y = row_y_cursor
            row_y_by_idx.append((row_y, rh))
            # Label column. For ORRE, sneak in a K² badge to contrast with the
            # shuffle caption's "not K²" — the shuffle is O(K); ORRE itself is K²/exec.
            if op.kind == "orre":
                parts.append(
                    f'<text x="{sx + STAGE_PAD + 8}" y="{row_y + rh // 2}" '
                    f'font-family="{FONT}" font-size="{FS_OP}" text-anchor="start" '
                    f'dominant-baseline="middle" fill="#111">{op.label}'
                    f'<tspan xml:space="preserve" font-style="italic" font-weight="bold" fill="#c00"> · K²/exec</tspan>'
                    f'</text>'
                )
            else:
                parts.append(text(sx + STAGE_PAD + 8, row_y + rh // 2, op.label, size=FS_OP, anchor="start"))
            # Highlighted reader's (exec, slot) for bracket rendering on shuffle_write cells.
            if op.kind == "shuffle_write" and exec_frames:
                _slices_per_exec = exec_frames[0][0].slices
                h_ex = HIGHLIGHT_READER_K // _slices_per_exec
                h_sl = HIGHLIGHT_READER_K % _slices_per_exec
            else:
                h_ex, h_sl = -1, -1

            # Cells across execs, labeled with global k (0..K-1 across all execs).
            k_cursor = 0
            for ex_idx, (ex, ex_x0, cell_x0) in enumerate(exec_frames):
                fill = OP_FILL.get(op.kind, "#fff")
                for si in range(ex.slices):
                    k = k_cursor + si
                    cx = cell_x0 + si * CELL_W
                    cy = row_y + 4
                    ch = rh - 8
                    parts.append(rect(cx, cy, CELL_W - 4, ch, fill=fill, stroke="#556", sw=0.8))
                    # global-k label: for ORRE row put it at the TOP of the cell so the
                    # crossing arrows drawn inside have breathing room below.
                    # For shuffle_write, put label at TOP so batches fit below.
                    if op.kind in ("orre", "shuffle_write"):
                        label_y = cy + 10
                    else:
                        label_y = cy + ch // 2
                    parts.append(text(cx + (CELL_W - 4) // 2, label_y, f"k={k}", size=FS_SMALL))

                    # Record shuffle anchors + draw batches + highlight consumed windows.
                    if op.kind == "shuffle_write":
                        out_id = op.extra.get("out")
                        if out_id:
                            writer_anchors.setdefault(out_id, []).append(
                                (ex_idx, si, cx + (CELL_W - 4) // 2, cy)
                            )
                        # Batch sub-cells: N horizontal strips below the label.
                        n_batches = int(doc.shuffles.get(out_id or "", {}).get("batches_per_writer", 4))
                        batch_zone_top = cy + 18  # below the "k=N" label
                        batch_zone_bot = cy + ch - 2
                        batch_h = (batch_zone_bot - batch_zone_top) / max(n_batches, 1)
                        for bi in range(n_batches):
                            by = batch_zone_top + bi * batch_h
                            parts.append(rect(
                                cx + 3, int(by), CELL_W - 4 - 6, int(batch_h - 1),
                                fill="#fff", stroke="#889", sw=0.4, rx=1,
                            ))
                        # Highlight the batches reader HIGHLIGHT_READER_K pulls from THIS writer.
                        # Value order: top batch = smallest values, bottom batch = largest.
                        # Halo left (writer at reader_sl - 1): reader pulls BOTTOM batch
                        # Halo right (writer at reader_sl + 1): reader pulls TOP batch
                        # Self (writer at reader_sl in ANY exec): reader pulls ALL batches
                        if si == h_sl:
                            b_lo, b_hi = 0, n_batches
                        elif si == h_sl - 1:
                            b_lo, b_hi = n_batches - 1, n_batches
                        elif si == h_sl + 1:
                            b_lo, b_hi = 0, 1
                        else:
                            b_lo, b_hi = -1, -1
                        if b_lo >= 0:
                            y_top = batch_zone_top + b_lo * batch_h
                            y_bot = batch_zone_top + b_hi * batch_h
                            parts.append(rect(
                                cx + 1, int(y_top) - 1, CELL_W - 4 - 2, int(y_bot - y_top) + 2,
                                fill="none", stroke=HIGHLIGHT_COLOR, sw=2, rx=2,
                            ))
                k_cursor += ex.slices

            # ORRE: draw the internal N→N crossing arrows per exec.
            # The shuffle is entirely local to each exec (no cross-exec traffic —
            # inter-task communication only happens at stage boundaries).
            # Visual: each cell's bottom edge is an INPUT anchor, top edge an OUTPUT
            # anchor. Every input fans out to every output within the exec = N^2 arrows.
            if op.kind == "orre":
                for ex, ex_x0, cell_x0 in exec_frames:
                    # Slot centers within this exec
                    xs = [cell_x0 + si * CELL_W + (CELL_W - 4) // 2 for si in range(ex.slices)]
                    top_y = row_y + 4 + 14      # just below the "k=N" label
                    bot_y = row_y + rh - 4 - 4  # just above the cell bottom edge
                    # Full N² arrows including self (i==j) — every input can go to
                    # every output; self-lines make the K² cost fully visible.
                    for x_in in xs:
                        for x_out in xs:
                            parts.append(
                                line(x_in, bot_y, x_out, top_y, stroke="#888", sw=0.6, dash="1.5,1.5")
                            )
            row_y_cursor += rh

        # Reader anchors: stage `input` enters from BELOW the bottom-most op row.
        # Anchor at the BOTTOM edge of the bottom op row (where the row meets the stage frame).
        if s.input_desc:
            bottom_row_y, bottom_row_h = row_y_by_idx[-1]
            bottom_row_bot = bottom_row_y + bottom_row_h - 4
            for ex_idx, (ex, ex_x0, cell_x0) in enumerate(exec_frames):
                for si in range(ex.slices):
                    cx = cell_x0 + si * CELL_W
                    reader_anchors.setdefault("rng", []).append(
                        (ex_idx, si, cx + (CELL_W - 4) // 2, bottom_row_bot)
                    )
            # Caption sits just below the last op row, inside the stage footer padding.
            cap_y = bottom_row_y + bottom_row_h + STAGE_PAD // 2
            parts.append(
                text(sx + STAGE_PAD + 8, cap_y, "input: " + s.input_desc, size=FS_SMALL, anchor="start", fill="#666")
            )

        # Sink line (top-of-stage → client)
        if s.sink == "client":
            # arrow to client circle drawn earlier
            top_y = sy
            mid_x = sx + sw_ // 2
            parts.append(line(mid_x, top_y, mid_x, top_y - STAGE_GAP // 2 + 16, stroke="#333", sw=1.2))

    # Shuffle edges: isomorphic-per-exec random-uniform model.
    # Each stage-N exec does an INTERNAL 4→4 ORRE (no cross-exec traffic at ORRE);
    # its 4 output partitions cover the same value-range partitions as every other
    # exec's 4 outputs (partition p in exec_a and partition p in exec_b hold the
    # same value range). Reader at local slot p in ANY exec must therefore pull from
    # local slots {p-1, p, p+1} in EVERY exec (halo ±1 × n_execs).
    for sid, w_anchors in writer_anchors.items():
        r_anchors = reader_anchors.get(sid, [])
        # Index writers by (exec_idx, local_slot) → (x, y).
        w_by_pos: dict[tuple[int, int], tuple[int, int]] = {
            (ei, sl): (x, y) for (ei, sl, x, y) in w_anchors
        }
        exec_indices = sorted({ei for (ei, _, _, _) in w_anchors})
        slices_per_exec = 1 + max((sl for (_, sl, _, _) in w_anchors), default=0)

        for (r_ei, r_sl, rx, ry) in r_anchors:
            for w_sl in (r_sl - 1, r_sl, r_sl + 1):
                if not (0 <= w_sl < slices_per_exec):
                    continue
                for w_ei in exec_indices:
                    if (w_ei, w_sl) not in w_by_pos:
                        continue
                    wx, wy = w_by_pos[(w_ei, w_sl)]
                    is_self = (w_ei == r_ei and w_sl == r_sl)
                    is_same_exec = (w_ei == r_ei)
                    parts.append(
                        line(
                            wx, wy - 2, rx, ry + 2,
                            stroke=("#444" if is_self else ("#666" if is_same_exec else "#aaa")),
                            sw=(1.2 if is_self else 0.7),
                            dash=(None if is_self else ("2,2" if is_same_exec else "3,3")),
                        )
                    )
        # Caption in the gap between stages — lead with the punchline: O(K), NOT O(K²).
        # Drawn last so it sits ABOVE the crossing edges; boxed for legibility.
        if w_anchors and r_anchors:
            all_x = [x for (_, _, x, _) in w_anchors + r_anchors]
            mx = (min(all_x) + max(all_x)) // 2
            my = (max(y for (_, _, _, y) in w_anchors) + min(y for (_, _, _, y) in r_anchors)) // 2
            # Approximate width for the background box.
            approx_txt = f"{sid} · not K² · O(K)"
            box_w = 6 * len(approx_txt) + 40
            parts.append(rect(mx - box_w // 2, my - 12, box_w, 24, fill="#ffffff", stroke="#556", sw=1, rx=6))
            parts.append(
                f'<text x="{mx}" y="{my}" font-family="{FONT}" font-size="{FS_OP}" '
                f'font-weight="bold" text-anchor="middle" dominant-baseline="middle" fill="#111">'
                f'{sid} · '
                f'<tspan font-style="italic" font-size="{FS_OP + 3}" fill="#c00" text-decoration="underline">not</tspan>'
                f' K² · O(K)'
                f'</text>'
            )
            # Highlight legend, one line below the punchline box.
            legend = f"reader k={HIGHLIGHT_READER_K} read windows (via range index)"
            legend_w = 6 * len(legend) + 20
            parts.append(rect(mx - legend_w // 2, my + 14, legend_w, 18, fill="#ffffff", stroke=HIGHLIGHT_COLOR, sw=1, rx=4))
            parts.append(text(mx, my + 23, legend, size=FS_SMALL, fill=HIGHLIGHT_COLOR, weight="bold"))

    parts.append("</svg>")
    return "\n".join(parts)


def main():
    ap = argparse.ArgumentParser(description="Render a query shape as SVG")
    ap.add_argument("input", type=Path, help="input YAML file")
    ap.add_argument("-o", "--output", type=Path, required=True, help="output SVG file")
    args = ap.parse_args()

    doc = load(args.input)
    svg = render(doc)
    args.output.write_text(svg)
    print(f"wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
