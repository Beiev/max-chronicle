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


def test_a_bowed_line_is_taken_along_its_bow_and_not_along_its_chord(run) -> None:
    out = run("""
        import { pickEdge, bowPoint, distanceToBow } from "./pick.mjs";
        const projected = new Float32Array([100, 100, 1, 300, 100, 1]);
        const edges = [{ s: 0, t: 1 }], all = () => true;
        // Bowed by a fifth of its length to the left of the way from a to b: on the screen, y down, that is below.
        const control = bowPoint(100, 100, 300, 100, 0.2), apex = [200, 100 + (control[1] - 100) / 2];
        console.log(JSON.stringify({
          control, apex,
          straightOnChord: pickEdge(200, 101, edges, projected, all),
          straightAtApex: pickEdge(apex[0], apex[1], edges, projected, all),
          bowedAtApex: pickEdge(apex[0], apex[1], edges, projected, all, () => 0.2),
          bowedOnChord: pickEdge(200, 101, edges, projected, all, () => 0.2),
          otherWay: pickEdge(apex[0], apex[1], edges, projected, all, () => -0.2),
          unbowed: pickEdge(200, 101, edges, projected, all, () => 0),
          atItsEnd: pickEdge(102, 101, edges, projected, all, () => 0.2),
          onTheBow: distanceToBow(apex[0], apex[1], 100, 100, 300, 100, 0.2),
          offTheBow: distanceToBow(200, 100, 100, 100, 300, 100, 0.2)[0],
        }));
    """)

    assert out["control"] == [200, 140] and out["apex"] == [200, 120]
    assert out["straightOnChord"] == 0 and out["straightAtApex"] == -1
    assert out["bowedAtApex"] == 0 and out["bowedOnChord"] == -1  # the line is where it is drawn
    assert out["otherWay"] == -1 and out["unbowed"] == 0
    assert out["atItsEnd"] == -1  # the ends of a line belong to its nodes, bowed or not
    assert out["onTheBow"][0] < 0.5 and abs(out["onTheBow"][1] - 0.5) < 0.06 and abs(out["offTheBow"] - 20) < 0.5


# ---------------------------------------------------------------------------
# The form of a galaxy, its gas, and the wisps the gas is cut from
# ---------------------------------------------------------------------------

_GALAXIES = """
    import { Shaper, planeOf, SHAPED_FROM } from "./shape.mjs";
    // Six projects, each a ball of memories around its sun as the layout leaves them. Days are counted back from
    // now: a project "written" has memories of the last month, one "quiet" stopped a hundred days ago.
    const DAY = 86400, NOW = 400 * DAY;
    const projects = [
      { size: 240, radius: 100, at: [600, 40, -30], quiet: false },   // large and written: a spiral, or a barred one
      { size: 5, radius: 60, at: [-500, 0, 0], quiet: false },        // too small for a form of its own
      { size: 60, radius: 80, at: [0, 700, 200], quiet: true },       // middling and quiet: a lens, or a small ball
      { size: 200, radius: 100, at: [-300, -500, 400], quiet: true }, // large and quiet: a great ball
      { size: 20, radius: 50, at: [300, -300, -600], quiet: false },  // small and written: a cloud
      { size: 12, radius: 50, at: [-600, 400, -300], quiet: true },   // small and quiet: a cluster
    ];
    const nodes = projects.map((_, p) => ({ k: "project", p, t: 0 })), laid = projects.flatMap((project) => project.at);
    nodes.push({ k: "note", p: null, t: NOW });
    laid.push(7, 8, 9);
    const first = [];
    projects.forEach((project, p) => {
      first.push(nodes.length);
      for (let i = 0; i < project.size; i++) {
        // the golden-angle lattice: evenly over the ball, no two alike. When each was written has nothing to do
        // with where it stands: the days are dealt out of order (97 shares no factor with any of the sizes).
        const up = 1 - (2 * (i + 0.5)) / project.size, ring = Math.sqrt(1 - up * up), turn = i * 2.39996;
        const day = (project.quiet ? 100 : 0) + ((i * 97) % project.size) * (project.quiet ? 1 : 0.4);
        nodes.push({ k: i % 40 === 3 ? "task" : "event", p, t: NOW - day * DAY });
        laid.push(project.at[0] + Math.cos(turn) * ring * project.radius, project.at[1] + up * project.radius,
                  project.at[2] + Math.sin(turn) * ring * project.radius);
      }
    });
    const graph = { nodes, projectNode: new Map(projects.map((_, p) => [p, p])) };
    const at = (positions, i) => [positions[i * 3], positions[i * 3 + 1], positions[i * 3 + 2]];
    const dot = (a, b) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    const far = (positions, i, j) => Math.hypot(...at(positions, i).map((value, axis) => value - at(positions, j)[axis]));
    const mean = (values) => values.reduce((sum, value) => sum + value, 0) / values.length;
"""


def test_each_project_is_given_a_galaxy_read_off_the_project_itself(run) -> None:
    out = run(_GALAXIES + """
        const shaper = new Shaper(graph), raw = new Float32Array(laid), shaped = shaper.shape(raw, 3);
        const of = (p) => shaper.galaxies.get(p), spiral = of(0), ball = of(3);
        // In the plane of the spiral and off it, about where its sun is drawn.
        const local = (galaxy, i) => { const d = at(shaped, i).map((v, a) => v - shaped[galaxy.sun * 3 + a]); return [dot(d, galaxy.u), dot(d, galaxy.v), dot(d, galaxy.n)]; };
        const members = spiral.byAge, flat = members.map((i) => local(spiral, i)), wide = flat.map(([x, y]) => Math.hypot(x, y));
        const size = spiral.reach, core = size * 0.2;
        const onArms = flat.map(([x, y]) => Math.cos(spiral.arms * (Math.atan2(y, x)
          - spiral.wind * Math.log(1 + Math.max(Math.hypot(x, y) - spiral.bar * size, 0) / core) - spiral.phase)));
        const inBall = ball.members.map((i) => local(ball, i));
        const suns = [...shaper.galaxies.values()];
        let closest = Infinity;
        for (const a of suns) for (const b of suns) if (a !== b) closest = Math.min(closest, far(shaped, a.sun, b.sun) / (a.reach + b.reach));
        const again = new Shaper(graph).shape(raw, 3);
        console.log(JSON.stringify({
          shapedFrom: SHAPED_FROM, forms: [0, 1, 2, 3, 4, 5].map((p) => (of(p) ? of(p).form : null)), arms: [0, 2, 3].map((p) => of(p).arms),
          ages: [spiral.age[spiral.members.indexOf(members[0])], spiral.age[spiral.members.indexOf(members[members.length - 1])]],
          oldest: mean(wide.slice(0, 24)), newest: mean(wide.slice(-24)), widest: Math.max(...wide), reach: spiral.reach,
          tallest: Math.max(...flat.map((d) => Math.abs(d[2]))), tall: spiral.tall, onArms: mean(onArms),
          ballTall: Math.max(...inBall.map((d) => Math.abs(d[2]))), ballWide: Math.max(...inBall.map((d) => Math.hypot(d[0], d[1]))),
          trailing: Math.sign(spiral.rate) === -Math.sign(spiral.wind), closest,
          inward: suns.every((g) => far(shaped, g.sun, 6) < 1e9), loose: at(shaped, 6),
          small: far(shaped, first[1], 1) / far(raw, first[1], 1),
          square: [dot(spiral.u, spiral.v), dot(spiral.u, spiral.n), dot(spiral.v, spiral.n), dot(spiral.n, spiral.n)],
          same: [...again].every((value, index) => value === shaped[index]), finite: [...shaped].every(Number.isFinite),
          untouched: [...raw].every((value, index) => Math.abs(value - laid[index]) < 1e-3), plane: planeOf([0, 1, 0]),
        }));
    """)

    assert out["shapedFrom"] == 8
    forms = out["forms"]
    # Large and written is a spiral, large and quiet a ball; a small one a cloud while written and a cluster once quiet.
    assert forms[0] in ("spiral", "barred") and forms[1] is None and forms[2] in ("lens", "ball")
    assert forms[3:] == ["ball", "cloud", "cluster"]
    assert out["arms"][0] in (2, 3) and out["arms"][2] == 0
    # The oldest memories are the heart and the newest the rim.
    assert out["ages"] == [0, 1] and out["oldest"] < 0.4 * out["newest"] and out["widest"] <= out["reach"] * 1.001
    # A disc with the thickness of a body: far wider than it is tall, and no memory stands above its heart.
    assert 0 < out["tallest"] <= out["tall"] * 1.001 and out["widest"] > 2.5 * out["tallest"]
    # Gathered on the arms: spread evenly around, the memories would average nought.
    assert out["onArms"] > 0.25 and out["trailing"]
    # A ball is a body in all three directions.
    assert out["ballTall"] > 0.35 * out["ballWide"]
    # No two galaxies lie through each other.
    assert out["closest"] > 0.6
    # What has no project comes nearer the middle; a project too small for a form keeps its ball, drawn nearer its sun.
    assert all(abs(v) < 20 for v in out["loose"]) and 0.5 < out["small"] < 1
    assert _near(out["square"], [0, 0, 0, 1], within=1e-6)
    assert out["same"] and out["finite"] and out["untouched"]  # the same memory takes the same form; the layout is not written over
    assert _near(out["plane"][0] + out["plane"][1], [0, 0, 1, 1, 0, 0])  # two directions square to the pole, a right hand with it


def test_a_galaxy_turns_as_one_body_and_the_flat_map_stays_flat(run) -> None:
    out = run(_GALAXIES + """
        const shaper = new Shaper(graph), raw = new Float32Array(laid), shaped = shaper.shape(raw, 3);
        const spiral = shaper.galaxies.get(0), [a, b, c] = spiral.members, other = shaper.galaxies.get(3).members[0];
        const still = shaper.turn(shaped);
        shaper.spin(40);
        const turned = shaper.turn(shaped), one = shaper.turned(shaped, a);
        const off = (positions, i) => at(positions, i).map((v, axis) => v - positions[spiral.sun * 3 + axis]);
        // The flat map: every z at nought before and after, every galaxy lying in the page.
        const flatLaid = new Float32Array(laid.map((value, index) => (index % 3 === 2 ? 0 : value)));
        const onFlat = new Shaper(graph), flat = onFlat.shape(flatLaid, 2);
        onFlat.spin(40);
        const flatTurned = onFlat.turn(flat);
        console.log(JSON.stringify({
          still: [...still].every((value, index) => value === shaped[index]),
          angle: spiral.angle, rate: spiral.rate, moved: far(turned, a, a) === 0 && Math.hypot(...at(turned, a).map((v, k) => v - at(shaped, a)[k])),
          fromSun: [far(shaped, a, spiral.sun), far(turned, a, spiral.sun)], apart: [far(shaped, a, b), far(turned, a, b), far(shaped, b, c), far(turned, b, c)],
          height: [dot(off(shaped, a), spiral.n), dot(off(turned, a), spiral.n)],
          sun: far(turned, spiral.sun, spiral.sun) + Math.hypot(...at(turned, spiral.sun).map((v, k) => v - at(shaped, spiral.sun)[k])),
          one: one.map((v, k) => v - at(turned, a)[k]), otherMoved: Math.hypot(...at(turned, other).map((v, k) => v - at(shaped, other)[k])) > 0,
          flatZ: Math.max(...[...flat, ...flatTurned].filter((_, index) => index % 3 === 2).map(Math.abs)),
          flatPole: onFlat.galaxies.get(0).n, flatMoved: Math.hypot(...at(flatTurned, a).map((v, k) => v - at(flat, a)[k])) > 1,
        }));
    """)

    assert out["still"]  # before any turn every light is where its galaxy's form put it
    assert abs(out["angle"] - 40 * out["rate"]) < 1e-9 and 0.004 < abs(out["rate"]) < 0.02 and out["moved"] > 1
    # A turn about the pole: as far from the sun as before, as high off the plane, and as far from its neighbours.
    assert _near([out["fromSun"][1], out["height"][1]], [out["fromSun"][0], out["height"][0]], within=1e-2)
    assert _near(out["apart"][1::2], out["apart"][0::2], within=1e-2)
    assert out["sun"] == 0 and _near(out["one"], [0, 0, 0], within=1e-3) and out["otherMoved"]
    assert out["flatZ"] == 0 and out["flatPole"] == [0, 0, 1] and out["flatMoved"]


def test_the_gas_hangs_on_the_memories_and_comes_with_them(run) -> None:
    out = run(_GALAXIES + """
        import { Gas, LOOK, AT } from "./gas.mjs";
        // Three links between memories of the spiral and of the great ball, and a resemblance that alone is too little.
        const edges = [0, 1, 2].map((k) => ({ s: first[0] + k, t: first[3] + k, k: 2, w: 1 }));
        edges.push({ s: first[4], t: first[5], k: 4, w: 0.9 });
        const birth = Float32Array.from(nodes, (node) => node.t / DAY);
        const world = { ...graph, edges, birth, colorOf: (p) => ["#8db7e0", "#e0a98a", "#a9c98c", "#c3a6e0", "#e3c27e", "#7fc7cc"][p] };
        const shaper = new Shaper(world), shaped = shaper.shape(new Float32Array(laid), 3), gas = new Gas(world, shaper);
        const runs = [...gas.ranges.values(), ...gas.dust.values(), gas.field].sort((a, b) => a.start - b.start);
        const gapless = runs.every((r, k) => r.start === (k ? runs[k - 1].start + runs[k - 1].count : 0));
        const view = { time: 0, play: 400, flat: false, centre: [0, 0, 0], radius: 900, beside: 1, shows: () => 1 };
        gas.updateBorn(400);
        const now = gas.place(shaped, view).slice();
        const spiral = shaper.galaxies.get(0), own = gas.ranges.get(0), dust = gas.dust.get(0);
        const where = (placed, j) => [placed[j * AT], placed[j * AT + 1], placed[j * AT + 2]];
        const fromSun = (placed, j) => Math.hypot(...where(placed, j).map((v, k) => v - shaped[spiral.sun * 3 + k]));
        // The last light of a galaxy's gas is the hot middle of its heart: at the sun itself.
        const heart = own.start + own.count - 1;
        const dustOff = [];
        for (let j = dust.start; j < dust.start + dust.count; j++) dustOff.push(Math.hypot(...where(now, j).map((v, k) => v - shaped[gas.anchor[j] * 3 + k])) / spiral.reach);
        // On the day the spiral was half written, the gas of what was written later is not there yet.
        gas.updateBorn(352);
        const half = gas.place(shaped, { ...view, play: 352 }).slice();
        let unborn = 0, bornThen = 0;
        for (let j = own.start; j < own.start + own.count; j++) {
          if (gas.anchor[j] === spiral.sun) continue;
          if (birth[gas.anchor[j]] > 354) unborn += half[j * AT + 4]; else if (birth[gas.anchor[j]] < 350) bornThen += half[j * AT + 4];
        }
        gas.updateBorn(400);
        const hidden = gas.place(shaped, { ...view, shows: (p) => (p === 0 ? 0 : 1) }).slice();
        const later = gas.place(shaped, { ...view, time: 7 }).slice();
        const beads = [];
        const bridge = gas.bridges[0], a = at(shaped, bridge.a), b = at(shaped, bridge.b), span = Math.hypot(...a.map((v, k) => v - b[k]));
        for (let j = gas.motes; j < gas.motes + 11; j++) {
          const p = where(now, j), t = Math.max(0, Math.min(1, dot(p.map((v, k) => v - a[k]), b.map((v, k) => v - a[k])) / (span * span)));
          beads.push(Math.hypot(...p.map((v, k) => v - (a[k] + (b[k] - a[k]) * t))) / span);
        }
        const flat = new Shaper(world), flatShaped = flat.shape(new Float32Array(laid.map((v, i) => (i % 3 === 2 ? 0 : v))), 2), flatGas = new Gas(world, flat);
        flatGas.updateBorn(400);
        const flatAt = flatGas.place(flatShaped, { ...view, flat: true });
        console.log(JSON.stringify({
          sizes: [LOOK, AT, gas.look.length / gas.count, gas.at.length / gas.count], gapless, total: runs.reduce((sum, r) => sum + r.count, 0) === gas.count,
          galaxies: [gas.ranges.size, gas.dust.size], heart: fromSun(now, heart), heartSize: now[heart * AT + 3] / spiral.reach,
          plain: gas.look[heart * LOOK + 5] < 0, wispy: gas.look[(own.start + 1) * LOOK + 5] >= 0,
          dust: [dust.count, Math.max(...dustOff), gas.look[dust.start * LOOK + 6]], inReach: fromSun(now, own.start + 1) < spiral.reach * 1.5,
          unborn, bornThen: bornThen > 0, hidden: hidden[(own.start + 1) * AT + 4], shown: now[(own.start + 1) * AT + 4] > 0,
          bridges: gas.bridges.length, joins: [bridge.pa, bridge.pb], sag: Math.max(...beads),
          travels: [0, 1, 2, 3].some((k) => Math.abs(later[(gas.motes + k) * AT + 4] - now[(gas.motes + k) * AT + 4]) > 0.01),
          bridgeHidden: hidden[gas.motes * AT + 4], flatZ: Math.max(...[...flatAt].filter((_, index) => index % AT === 2).slice(0, flatGas.motes).map(Math.abs)),
          finite: [...now].every(Number.isFinite) && [...gas.look].every(Number.isFinite),
        }));
    """)

    assert out["sizes"] == [7, 5, 7, 5] and out["gapless"] and out["total"]  # every light is in one run, and the runs meet
    assert out["galaxies"] == [5, 5]  # one run of gas and one of dust to a galaxy
    assert out["heart"] < 1e-3 and 0.1 < out["heartSize"] < 0.2 and out["plain"] and out["wispy"]
    # The dust of stars: many points to a galaxy, each close by a memory, each a point on the screen however near.
    assert out["dust"][0] >= 90 and out["dust"][1] < 0.16 and out["dust"][2] < 2 and out["inReach"]
    # The gas comes with the memories as the time machine plays, and steps back with its galaxy.
    assert out["unborn"] == 0 and out["bornThen"] and out["hidden"] == 0 and out["shown"]
    # One bridge: three links join the spiral and the great ball; one resemblance is not enough for the other pair.
    assert out["bridges"] == 1 and out["joins"] == [0, 3] and out["sag"] < 0.12 and out["travels"] and out["bridgeHidden"] == 0
    assert out["flatZ"] == 0 and out["finite"]


def test_the_wisps_fill_their_range_and_repeat_at_the_edges(run) -> None:
    out = run("""
        import { wisps } from "./render.mjs";
        const data = wisps(), size = Math.sqrt(data.length / 4), red = (x, y) => data[(y * size + x) * 4];
        let least = 255, most = 0, seam = 0, across = 0, inside = 0, opaque = true;
        for (let i = 0; i < data.length; i += 4) { least = Math.min(least, data[i]); most = Math.max(most, data[i]); if (data[i + 3] !== 255) opaque = false; }
        for (let k = 0; k < size; k++) {
          seam += Math.abs(red(0, k) - red(size - 1, k)) + Math.abs(red(k, 0) - red(k, size - 1));
          inside += Math.abs(red(200, k) - red(201, k)) + Math.abs(red(k, 200) - red(k, 201));
          across += Math.abs(red(0, k) - red(size / 2, k)) + Math.abs(red(k, 0) - red(k, size / 2));
        }
        const again = wisps();
        console.log(JSON.stringify({ size, least, most, opaque, seam: seam / size, inside: inside / size, across: across / size,
                                     same: data.every((value, index) => value === again[index]) }));
    """)

    assert out["size"] == 512 and out["least"] == 0 and out["most"] == 255 and out["opaque"]
    # Across the edge the wisps go on as they do anywhere else, so a cloud can be cut from any patch.
    assert out["seam"] < 3 * out["inside"] + 1 and out["seam"] < 0.3 * out["across"]
    assert out["same"]


def test_a_memory_wears_the_light_of_its_age_and_a_tone_of_its_project(run) -> None:
    out = run("""
        import { Graph } from "./model.mjs";
        const nodes = [{ id: "p:a", k: "project", l: "alpha", p: 0, t: null, u: null }];
        for (let i = 0; i < 40; i++) nodes.push({ id: "e:" + i, k: "event", l: "e" + i, p: 0, t: 1000 + i * 86400, u: null, c: i === 5 ? "decision" : "note" });
        const graph = new Graph({ nodes, projects: ["alpha"], edges: [], span: [1000, 1000 + 40 * 86400] });
        const warmth = (i) => graph.color[i * 3] - graph.color[i * 3 + 2];
        const mean = (list) => list.reduce((sum, i) => sum + warmth(i), 0) / list.length;
        const old = [...Array(10).keys()].map((k) => 1 + k), young = [...Array(10).keys()].map((k) => 31 + k);
        const spread = Math.max(...old.map((i) => graph.color[i * 3 + 2])) - Math.min(...old.map((i) => graph.color[i * 3 + 2]));
        console.log(JSON.stringify({ youth: [graph.youth[1], graph.youth[40], graph.youth[0]], old: mean(old), young: mean(young), spread,
                                     shapes: [graph.shape[1], graph.shape[6], graph.shape[0]], finite: [...graph.color].every(Number.isFinite) }));
    """)

    assert out["youth"] == [0, 1, 0.5]
    assert out["old"] > out["young"] + 0.05  # the old stars of the heart are warm, the young ones of the rim blue-white
    assert out["spread"] > 0.01  # and no two are one colour
    assert out["shapes"][0] == 0 and 0.15 < out["shapes"][1] < 0.5 and out["shapes"][2] == 4  # a decision is a bright star
    assert out["finite"]


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
