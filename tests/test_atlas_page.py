"""The Atlas page's own modules, run under node: the camera, the model, the address, the river's axis, picking, the layout."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
import re
import shutil
import subprocess
from typing import Any, Callable

import pytest

from max_chronicle import atlas

DAY = 86400


@pytest.fixture()
def run(tmp_path) -> Callable[..., Any]:
    """Runs a script beside the page's modules and returns what it printed, read as JSON."""
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    # Named .mjs, the page's scripts load as modules under any node.
    for source in atlas.UI_DIR.glob("*.js"):
        text = re.sub(r'(from\s+"\./[\w.]+)\.js"', r'\1.mjs"', source.read_text(encoding="utf-8"))
        (tmp_path / f"{source.stem}.mjs").write_text(text, encoding="utf-8")

    def run_script(script: str, *, timezone_name: str = "UTC") -> Any:
        (tmp_path / "script.mjs").write_text(script, encoding="utf-8")
        result = subprocess.run([node, "script.mjs"], cwd=tmp_path, capture_output=True, text=True, timeout=60,
                                env={**os.environ, "TZ": timezone_name})
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    return run_script


def _near(found: list[float], expected: list[float], within: float = 1e-3) -> bool:
    return len(found) == len(expected) and all(abs(a - b) <= within for a, b in zip(found, expected))


# ---------------------------------------------------------------------------
# The river's axis
# ---------------------------------------------------------------------------


def test_a_date_on_the_time_machine_never_passes_the_last_memory(run) -> None:
    # The map as served on 2026-10-29: the axis counts whole days from the first
    # memory, so it ran on to 30 October and the page said "Now · 30 Oct 2026".
    first = datetime(2026, 3, 15, 11, 48, 32, tzinfo=timezone.utc).timestamp()
    last = datetime(2026, 10, 29, 12, 46, 51, tzinfo=timezone.utc).timestamp()
    days = math.ceil((last - first) / DAY)

    moments = run('import { momentAt } from "./timeline.mjs";'
                  f"console.log(JSON.stringify([0, 10, {days} - 0.5, {days}].map((day) => momentAt([{first}, {last}], day))));")

    assert moments == [first, first + 10 * DAY, last, last]


def test_the_river_zooms_around_the_pointer_and_stays_inside_the_memory(run) -> None:
    views = run("""
        import { zoomView, slideView } from "./timeline.mjs";
        console.log(JSON.stringify({
          around: zoomView([0, 240], 0.5, 180, 240),
          closest: zoomView([100, 110], 0.1, 105, 240),
          widest: zoomView([100, 110], 1000, 105, 240),
          pastTheEnd: slideView([200, 230], 50, 240),
          beforeTheStart: slideView([10, 40], -50, 240),
          shortMemory: zoomView([0, 3], 0.5, 1, 3),
        }));
    """)

    assert views["around"] == [90, 210]  # day 180 stays three quarters of the way along
    assert views["closest"] == [102.5, 107.5]  # never narrower than five days
    assert views["widest"] == [0, 240]
    assert views["pastTheEnd"] == [210, 240] and views["beforeTheStart"] == [0, 30]
    assert views["shortMemory"] == [0, 3]  # a memory shorter than the closest view is shown whole


# ---------------------------------------------------------------------------
# The address
# ---------------------------------------------------------------------------


def test_the_address_brings_the_reader_back_to_the_same_place(run) -> None:
    out = run("""
        import { readPlace, writePlace } from "./urlstate.mjs";
        const places = [
          { line: ["e:1f3a", "n:77b0"], kind: "similar", day: "2026-08-13", from: "2026-07-01", project: "max chronicle", flat: true },
          { node: "t:alpha/a~b&c=d#e" },
          { line: ["t:alpha/a~b", "e:1f3a"] },
          { project: "50% done" },
          {},
        ];
        console.log(JSON.stringify({
          hashes: places.map(writePlace),
          back: places.map((place) => readPlace("#" + writePlace(place))),
          bare: readPlace(writePlace(places[0])),
          garbage: readPlace("#t=yesterday&f=2026-7-1&l=only-one&m=3d&n=&p=%E0%A4%A&x"),
          threeEnds: readPlace("#l=a~b~c"), kindAlone: readPlace("#k=link&n=e:1"),
        }));
    """)

    assert out["hashes"][0] == "l=e:1f3a~n:77b0&k=similar&t=2026-08-13&f=2026-07-01&p=max%20chronicle&m=2d"
    assert out["hashes"][4] == ""  # the plain view writes nothing
    assert out["back"] == [
        {"line": ["e:1f3a", "n:77b0"], "kind": "similar", "day": "2026-08-13", "from": "2026-07-01",
         "project": "max chronicle", "flat": True},  # two memories can be joined by more than one line: the kind says which
        {"node": "t:alpha/a~b&c=d#e"},
        {"line": ["t:alpha/a~b", "e:1f3a"]},  # a tilde inside an id does not part the line
        {"project": "50% done"},
        {},
    ]
    assert out["bare"] == out["back"][0]  # with or without its #
    assert out["garbage"] == {} and out["threeEnds"] == {}
    assert out["kindAlone"] == {"node": "e:1"}  # a kind belongs to a line


def test_a_day_in_the_address_is_the_readers_own_day(run) -> None:
    # 13 August in Warsaw runs from 22:00 UTC on the 12th to 22:00 UTC on the 13th.
    start = datetime(2026, 8, 12, 22, tzinfo=timezone.utc).timestamp()
    script = """
        import { dayOf, dayStart, dayEnd } from "./urlstate.mjs";
        console.log(JSON.stringify({
          start: dayStart("2026-08-13"), end: dayEnd("2026-08-13"),
          days: [START - 1, START, START + 86399, START + 86400].map(dayOf),
          shortDay: dayEnd("2026-03-29") - dayStart("2026-03-29") + 1, longDay: dayEnd("2026-10-25") - dayStart("2026-10-25") + 1,
          notDays: ["13.08.2026", "2026-02-31", "2026-13-01", ""].map((day) => [dayStart(day), dayEnd(day)]),
        }));
    """.replace("START", str(start))

    warsaw, utc = run(script, timezone_name="Europe/Warsaw"), run(script)

    assert warsaw["start"] == start and warsaw["end"] == start + DAY - 1
    assert warsaw["days"] == ["2026-08-12", "2026-08-13", "2026-08-13", "2026-08-14"]
    assert warsaw["shortDay"] == 23 * 3600 and warsaw["longDay"] == 25 * 3600  # the clocks change: the day ends where it ends
    assert warsaw["notDays"] == [[None, None]] * 4
    assert utc["start"] == start + 2 * 3600 and utc["days"][0] == "2026-08-12" and utc["days"][1] == "2026-08-12"


# ---------------------------------------------------------------------------
# The model
# ---------------------------------------------------------------------------

_MEMORY = """
    const T = 1000000, DAY = 86400;
    const data = {
      projects: ["alpha", "beta"], span: [T, T + DAY * 10], facts: [],
      nodes: [
        { id: "p:alpha", k: "project", l: "alpha", p: 0, t: T, u: null },
        { id: "p:beta", k: "project", l: "beta", p: 1, t: T, u: null },
        { id: "t:alpha/build", k: "task", l: "build", p: 0, t: T, u: null },
        { id: "e:1", k: "event", l: "Decided", p: 0, t: T + DAY, u: null, c: "decision", pi: 1 },
        { id: "e:2", k: "event", l: "Daily", p: 0, t: T + DAY * 2, u: null, c: "daily_summary", st: 1 },
        { id: "n:1", k: "note", l: "Page", p: 1, t: T + DAY * 3, u: null },
      ],
      edges: [[2, 0, 1], [3, 2, 0], [4, 0, 1], [5, 1, 1]],
    };
"""


def test_the_model_reads_the_map_and_takes_the_edges_by_meaning(run) -> None:
    out = run("""
        import { Graph, EDGE } from "./model.mjs";
        %s
        const graph = new Graph(data);
        const joined = graph.addSimilar([["e:1", "n:1", 0.91, 0], ["e:1", "e:404", 0.95, 1]]);
        const buffers = graph.edgeBuffers();
        graph.gone[5] = 1;
        console.log(JSON.stringify({
          days: graph.days, kinds: [...graph.kind], memory: [0, 2, 3, 5].map((i) => graph.memoryId(i)),
          titles: [graph.title(0), graph.title(3)], routine: [graph.routine(3), graph.routine(4)],
          pinned: [...graph.pinned], stale: [...graph.stale], parent: [...graph.parent],
          births: [...graph.birth], sun: graph.projectNode.get(1), found: graph.byMemoryId.get("note:1"),
          joined, edges: graph.edges.length, explicit: graph.explicitCount, last: graph.edges[4],
          degree: [...graph.degree], ends: [...buffers.ends], edgeBirth: [...buffers.birth], edgeKind: [...buffers.kind],
          edgeSince: [...buffers.since],
          neighboursOfDecided: graph.neighbours(3).map((link) => [link.o, link.k, link.out]),
          similar: EDGE.similar,
        }));
    """ % _MEMORY)

    assert out["days"] == 10 and out["kinds"] == [0, 0, 1, 2, 2, 3]
    assert out["memory"] == [None, None, "event:1", "note:1"] and out["found"] == 5
    assert out["titles"] == ["alpha", "Decided"] and out["routine"] == [False, True]
    assert out["pinned"] == [0, 0, 0, 1, 0, 0] and out["stale"] == [0, 0, 0, 0, 1, 0]
    assert out["parent"] == [-1, -1, 0, 2, 0, 1]  # what each hangs from in the layout
    assert out["births"] == [0, 0, 0, 1, 2, 3] and out["sun"] == 1
    # One edge by meaning joined; the other names a memory that is not on the map.
    assert out["joined"] == 1 and out["edges"] == 5 and out["explicit"] == 4
    assert out["last"] == {"s": 3, "t": 5, "k": out["similar"], "w": 0.91, "dup": 0}
    assert out["degree"] == [2, 1, 2, 1, 1, 1]  # resemblance adds no weight to a node
    assert out["ends"] == [2, 0, 3, 2, 4, 0, 5, 1, 3, 5]
    assert out["edgeBirth"] == [0, 1, 2, 3, 3] and out["edgeKind"] == [1, 0, 1, 1, 4]  # a line is born with its later end
    # A range that starts after a memory was written loses its lines too: a line by meaning between days 1 and 3
    # is gone from day 2 on. A task and a project stay as the frame, so the line between them is in every range.
    assert out["edgeSince"][1:] == [1, 2, 3, 1] and out["edgeSince"][0] > 1e8
    assert out["neighboursOfDecided"] == [[2, 0, True]]  # the note went from the map as a copy


def test_the_layout_starts_from_what_an_earlier_visit_kept(run) -> None:
    out = run("""
        import { Graph } from "./model.mjs";
        %s
        const graph = new Graph(data);
        const zs = (positions) => [...positions].filter((_, index) => index %% 3 === 2);
        const flat = graph.seedPositions(2, {}), deep = graph.seedPositions(3, {});
        const kept = Object.fromEntries(data.nodes.map((node, i) => [node.id, [i * 10, i * -5, i + 1]]));
        const again = graph.seedPositions(3, kept), onFlat = graph.seedPositions(2, kept);
        delete kept["n:1"]; delete kept["e:2"];
        const partly = graph.seedPositions(3, kept);
        console.log(JSON.stringify({
          flatZ: zs(flat.positions), deepZ: zs(deep.positions).some((z) => z !== 0),
          finite: [...flat.positions, ...deep.positions].every(Number.isFinite), cold: [flat.warm, deep.warm],
          again: [...again.positions], warm: again.warm, onFlat: onFlat.warm, partly: partly.warm,
          partlyKept: [...partly.positions].slice(0, 12),
        }));
    """ % _MEMORY)

    assert out["flatZ"] == [0] * 6 and out["deepZ"] and out["finite"] and out["cold"] == [False, False]
    assert out["warm"] and out["again"] == [value for i in range(6) for value in (i * 10, i * -5, i + 1)]
    assert not out["onFlat"]  # what was kept in space is no start for the flat map
    assert not out["partly"] and out["partlyKept"] == [0, 0, 1, 10, -5, 2, 20, -10, 3, 30, -15, 4]


def test_a_resemblance_short_of_identical_never_reads_as_a_hundred_percent(run) -> None:
    out = run('import { percent } from "./model.mjs";'
              "console.log(JSON.stringify([0.874, 0.994, 0.9951, 0.999, 0.9999, 1].map(percent)));")

    assert out == ["87%", "99%", "99.5%", "99.9%", "99.9%", "100%"]


# ---------------------------------------------------------------------------
# The camera
# ---------------------------------------------------------------------------

_CAMERA = """
    import { Camera } from "./camera.mjs";
    const camera = new Camera();
    camera.setFrame({ cx: 500, cy: 400, w: 800, h: 600, height: 800 });
    camera.fitDist = camera.distanceToFit(300);  // the whole memory is a ball of radius 300
    camera.jumpTo([10, 20, 30], camera.fitDist);
    const list = (found) => (found ? [...found] : null);
"""


def test_the_camera_fits_a_ball_to_the_frame_and_finds_what_is_under_the_pointer(run) -> None:
    out = run(_CAMERA + """
        const target = list(camera.project(10, 20, 30)), side = list(camera.project(310, 20, 30));
        const above = list(camera.project(10, 320, 30));
        const under = camera.unproject(650, 250), back = list(camera.project(...under));
        const behind = camera.project(...camera.eye.map((value, axis) => value + camera.back[axis] * 10));
        const positions = new Float32Array([10, 20, 30, 310, 20, 30, 10, 320, 30,
                                            ...camera.eye.map((v, a) => v + camera.back[a] * 10)]);
        const all = new Float32Array(12).fill(-1);
        camera.projectAll(positions, all);
        console.log(JSON.stringify({ target, side, above, back, behind, all: [...all], zoom: camera.zoom }));
    """)

    scale = out["target"][2]
    assert _near(out["target"][:2], [500, 400]) and abs(out["zoom"] - 1) < 1e-9
    # A ball of radius 300 reaches the edge of the frame's shorter side, 300 pixels out; up on the screen is up in the world.
    assert _near(out["side"][:2], [800, 400]) and _near(out["above"][:2], [500, 100]) and abs(scale - 1) < 1e-6
    assert _near(out["back"][:2], [650, 250])
    assert out["behind"] is None
    # Every node at once lands where one at a time does; what is behind the camera is marked, not drawn.
    assert _near(out["all"][:9], out["target"] + out["side"] + out["above"]) and out["all"][11] == 0


def test_a_drag_turns_the_sky_about_its_target_and_the_wheel_holds_the_pointer(run) -> None:
    out = run(_CAMERA + """
        const before = list(camera.project(310, 20, 30));
        camera.drag(120, -60);
        const target = list(camera.project(10, 20, 30)), turned = list(camera.project(310, 20, 30));
        const yaw = camera.yaw, pitch = camera.pitch;
        for (let i = 0; i < 100; i++) camera.drag(0, 500);
        const steepest = camera.pitch;
        const held = camera.unproject(700, 300);
        camera.dolly(0.5, 700, 300);
        const after = list(camera.project(...held)), closer = camera.zoom;
        for (let i = 0; i < 40; i++) camera.dolly(0.5, 500, 400);
        const nearest = camera.zoom;
        for (let i = 0; i < 40; i++) camera.dolly(2, 500, 400);
        console.log(JSON.stringify({ before, target, turned, yaw, pitch, steepest, after, closer, nearest, farthest: camera.zoom }));
    """)

    assert _near(out["target"][:2], [500, 400])  # the target stays where it was
    assert out["yaw"] < 0 and out["pitch"] < 0 and abs(out["turned"][0] - out["before"][0]) > 20
    assert 1.4 < out["steepest"] < math.pi / 2  # never over the pole: up stays up
    assert _near(out["after"][:2], [700, 300], within=0.05) and abs(out["closer"] - 2) < 1e-9
    assert _near([out["nearest"], out["farthest"]], [60, 1 / 6], within=1e-9)  # no closer, and no further away


def test_a_flight_arrives_and_the_flat_map_slides_instead_of_turning(run) -> None:
    out = run(_CAMERA + """
        camera.drag(200, 100);
        camera.flyTo([100, 0, -50], 400, 1000);
        const moving = camera.step(1500, 0.016), midway = camera.dist;
        camera.step(1000 + 1100, 0.016);
        const arrived = { target: [...camera.target], dist: Math.round(camera.dist * 1e6) / 1e6, flight: camera.flight, yaw: camera.yaw !== 0 };
        camera.spin = { yaw: 0.02, pitch: 0 };
        const yawBefore = camera.yaw;
        camera.step(3000, 0.016);
        const spun = camera.yaw - yawBefore;
        camera.setFlat(true);
        // A jump in the first frames (the layout posting positions) does not keep the turn the sky had.
        camera.step(4000, 0.016);
        const levelling = Math.abs(camera.yaw) > 0.01;
        camera.jumpTo([100, 0, -50], 400);
        camera.flyTo([100, 0, -50], 400, 4020);
        for (let frame = 1; frame <= 150; frame++) camera.step(4020 + frame * 16, 0.016);
        const flat = { yaw: camera.yaw, pitch: camera.pitch, spin: camera.spin };
        const at = list(camera.project(100, 0, -50));
        camera.drag(40, 25);
        const slid = list(camera.project(100, 0, -50));
        console.log(JSON.stringify({ moving, midway, arrived, spun, levelling, flat, at, slid, yawAfter: camera.yaw }));
    """)

    assert out["moving"] and out["midway"] > 0
    assert out["arrived"] == {"target": [100, 0, -50], "dist": 400, "flight": None, "yaw": True}  # a flight keeps the turn
    assert out["spun"] > 0  # a released drag keeps turning
    assert out["levelling"]  # the turn eases away, it does not snap
    assert out["flat"] == {"yaw": 0, "pitch": 0, "spin": {"yaw": 0, "pitch": 0}}
    assert _near(out["at"][:2], [500, 400])
    assert _near(out["slid"][:2], [540, 425], within=0.05) and abs(out["yawAfter"]) < 1e-9


# ---------------------------------------------------------------------------
# Under the pointer
# ---------------------------------------------------------------------------


def test_the_pointer_takes_a_node_before_a_line_and_a_line_only_between_its_ends(run) -> None:
    out = run("""
        import { pickNode, pickEdge, distanceToSegment } from "./pick.mjs";
        // x, y, scale; the fourth node is behind the camera
        const projected = new Float32Array([100, 100, 1, 300, 100, 1, 200, 300, 1, 150, 150, 0, 106, 100, 1]);
        const edges = [{ s: 0, t: 1 }, { s: 1, t: 2 }, { s: 0, t: 3 }];
        const all = () => true, radius = (i) => (i === 4 ? 1 : 4);
        console.log(JSON.stringify({
          onNode: pickNode(99, 101, projected, radius, all),
          betweenTwo: pickNode(105.5, 100, projected, radius, all),
          nowhere: pickNode(200, 180, projected, radius, all),
          dimmed: pickNode(99, 101, projected, radius, (i) => i !== 0 && i !== 4),
          behind: pickNode(150, 150, projected, radius, all),
          onLine: pickEdge(200, 103, edges, projected, all),
          atItsEnd: pickEdge(103, 100, edges, projected, all),
          beside: pickEdge(200, 120, edges, projected, all),
          toBehind: pickEdge(125, 125, edges, projected, all),
          notDrawn: pickEdge(200, 103, edges, projected, (index) => index !== 0),
          slanted: pickEdge(251, 199, edges, projected, all),
          segment: distanceToSegment(0, 5, 0, 0, 10, 0), pastTheEnd: distanceToSegment(13, 4, 0, 0, 10, 0),
        }));
    """)

    assert out["onNode"] == 0 and out["nowhere"] == -1 and out["dimmed"] == -1 and out["behind"] == -1
    assert out["betweenTwo"] == 4  # the light whose edge is nearer wins, not the larger one
    assert out["onLine"] == 0 and out["slanted"] == 1
    assert out["atItsEnd"] == -1  # the ends of a line belong to its nodes
    assert out["beside"] == -1 and out["toBehind"] == -1 and out["notDrawn"] == -1
    assert out["segment"] == [5, 0] and out["pastTheEnd"] == [5, 1]


# ---------------------------------------------------------------------------
# The layout
# ---------------------------------------------------------------------------


def test_the_layout_gathers_each_project_and_keeps_the_flat_map_flat(run) -> None:
    out = run("""
        globalThis.self = globalThis;
        const posts = [];
        globalThis.postMessage = (message) => posts.push(message);
        await import("./layout.worker.mjs");
        // Two projects, each with a task and twelve events; kinds as the model numbers them.
        const kinds = [0, 0, 1, 1], edges = [2, 0, 1, 3, 1, 1];
        for (let i = 0; i < 24; i++) { kinds.push(2); edges.push(4 + i, 2 + (i % 2), 0); }
        const n = kinds.length, init = new Float32Array(n * 3);
        // A deterministic scatter: everything starts mixed together.
        for (let i = 0; i < n * 3; i++) init[i] = Math.sin(i * 12.9898) * 120;
        const run = (dims) => {
          posts.length = 0;
          globalThis.onmessage({ data: { n, dims, kinds: new Int8Array(kinds), edges: new Int32Array(edges),
                                         init: init.slice(), ticks: 200, warm: false } });
          const last = posts[posts.length - 1], at = (i) => [...last.positions.slice(i * 3, i * 3 + 3)];
          const far = (a, b) => Math.hypot(...at(a).map((value, axis) => value - at(b)[axis]));
          let own = 0;
          for (let i = 4; i < n; i++) if (far(i, i % 2) < far(i, 1 - (i % 2))) own++;
          return { posts: posts.length, done: posts.map((post) => post.done), tick: last.tick, ticks: last.ticks,
                   length: last.positions.length, finite: [...last.positions].every(Number.isFinite), own,
                   depth: Math.max(...[...last.positions].filter((_, index) => index % 3 === 2).map(Math.abs)),
                   suns: far(0, 1) };
        };
        console.log(JSON.stringify({ flat: run(2), deep: run(3), n }));
    """)

    for dims in ("flat", "deep"):
        result = out[dims]
        assert result["length"] == out["n"] * 3 and result["finite"]
        assert result["tick"] == 199 and result["ticks"] == 200
        assert result["done"] == [False] * (result["posts"] - 1) + [True] and result["posts"] > 10  # the map is shown forming
        assert result["own"] == 24  # every event ends nearer its own project than the other
        assert result["suns"] > 60  # and the two projects apart
    assert out["flat"]["depth"] == 0 and out["deep"]["depth"] > 10
