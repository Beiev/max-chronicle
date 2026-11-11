"""The Atlas page's own modules, run under node: the camera, the model, the address, the river's axis, picking, the layout."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
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
    # A page to run the wiring on: a document, a canvas and a clock, all of them fake.
    shutil.copy(Path(__file__).with_name("atlas_fake_page.mjs"), tmp_path / "atlas_fake_page.mjs")

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
          letGo: [...graph.letGo()], hiddenToo: (graph.gone[4] = 1, [...graph.letGo()]),
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
    # After the edges of the memory comes the figure of its one task: from the task to its one memory.
    assert out["ends"] == [2, 0, 3, 2, 4, 0, 5, 1, 3, 5, 2, 3]
    assert out["edgeBirth"] == [0, 1, 2, 3, 3, 1] and out["edgeKind"] == [1, 0, 1, 1, 4, 5]  # a line is born with its later end
    # A range that starts after a memory was written loses its lines too: a line by meaning between days 1 and 3
    # is gone from day 2 on. A task and a project stay as the frame, so the line between them is in every range.
    assert out["edgeSince"][1:] == [1, 2, 3, 1, 1] and out["edgeSince"][0] > 1e8
    assert out["neighboursOfDecided"] == [[2, 0, True]]  # the note went from the map as a copy
    # What circles the hole is what is stale and still on the map: hidden as a copy, it is gone from there too.
    assert out["letGo"] == [0, 0, 0, 0, 1, 0] and out["hiddenToo"] == [0] * 6


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


def test_a_flight_can_be_bent_to_a_new_place_without_starting_again(run) -> None:
    out = run("""
        import { Camera } from "./camera.mjs";
        const camera = new Camera();
        camera.setFrame({ cx: 400, cy: 300, w: 800, h: 600, height: 600 });
        const before = [...camera.target, camera.dist], idle = camera.bend([1, 2, 3], 500), unmoved = [...camera.target, camera.dist];
        camera.flyTo([100, 0, 0], 400, 0, { ms: 1000 });
        camera.step(500, 0.016);
        const half = [...camera.target];
        const bent = camera.bend([0, 200, 0], 800), started = camera.flight.start;
        camera.step(1000, 0.016);
        console.log(JSON.stringify({ idle, same: before.every((v, k) => v === unmoved[k]), half, bent, started,
                                     arrived: [...camera.target, camera.dist], landed: camera.flight === null }));
    """)

    assert out["idle"] is False and out["same"]  # with no flight under way there is nothing to bend
    assert 0 < out["half"][0] < 100 and out["bent"] is True and out["started"] == 0  # the same flight, from when it began
    assert _near(out["arrived"], [0, 200, 0, 800], within=1e-6) and out["landed"]


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
        // The same memory read two hundred days on: nobody is writing in it any more.
        const aged = new Shaper({ ...graph, now: NOW + 200 * DAY });
        console.log(JSON.stringify({
          aged: [0, 4].map((p) => aged.galaxies.get(p).form), balls: [[...shaper.balls.keys()], shaper.balls.get(1).members.length],
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
    # Whether a project is being written is asked of the day the memory is read: long after, the spiral is a great
    # ball and the cloud a cluster. The project too small for a form is kept as a ball of its five memories.
    assert out["aged"] == ["ball", "cluster"] and out["balls"] == [[1], 5]
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
        // One task of the spiral is at work: a comet.
        const comet = first[0] + 3, small = first[1] + 3, mark = new Float32Array(nodes.length);
        mark[comet] = 4;
        // And so is the one task of the project too small for a galaxy.
        mark[small] = 4;
        const world = { ...graph, edges, birth, mark, colorOf: (p) => ["#8db7e0", "#e0a98a", "#a9c98c", "#c3a6e0", "#e3c27e", "#7fc7cc"][p] };
        const shaper = new Shaper(world), shaped = shaper.shape(new Float32Array(laid), 3), gas = new Gas(world, shaper);
        const runs = [...gas.ranges.values(), ...gas.dust.values(), gas.field].sort((a, b) => a.start - b.start);
        const gapless = runs.every((r, k) => r.start === (k ? runs[k - 1].start + runs[k - 1].count : 0));
        const view = { time: 0, play: 400, flat: false, centre: [0, 0, 0], radius: 900, beside: 1, shows: () => 1, seen: () => true };
        gas.updateBorn(400);
        const now = gas.place(shaped, view).slice();
        const spiral = shaper.galaxies.get(0), own = gas.ranges.get(0), dust = gas.dust.get(0);
        const where = (placed, j) => [placed[j * AT], placed[j * AT + 1], placed[j * AT + 2]];
        const fromSun = (placed, j) => Math.hypot(...where(placed, j).map((v, k) => v - shaped[spiral.sun * 3 + k]));
        // The last light of a galaxy's gas is the hot middle of its heart: at the sun itself.
        const heart = own.start + own.count - 1;
        // The last lights of the spiral's dust are the two tails of its comet: sixteen of the blue one, nine of dust.
        const TAILS = 25, tail = dust.start + dust.count - TAILS;
        const head = at(shaped, comet), sunAt = at(shaped, spiral.sun), away = head.map((v, k) => v - sunAt[k]), awayLong = Math.hypot(...away);
        const blue = [], drift = [];
        for (let j = tail; j < tail + TAILS; j++) {
          const d = where(now, j).map((v, k) => v - head[k]), along = dot(d, away) / awayLong;
          (j < tail + 16 ? blue : drift).push([along / spiral.reach, Math.hypot(...d.map((v, k) => v - (away[k] / awayLong) * along)) / spiral.reach]);
        }
        const dustOff = [];
        for (let j = dust.start; j < tail; j++) dustOff.push(Math.hypot(...where(now, j).map((v, k) => v - shaped[gas.anchor[j] * 3 + k])) / spiral.reach);
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
        // Tasks switched off: the head of the comet is not on the map.
        const headless = gas.place(shaped, { ...view, seen: (i) => i !== comet }).slice();
        const smallTail = gas.dust.get(1), smallHead = at(shaped, small), smallSun = at(shaped, 1);
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
          tailOn: [...Array(TAILS).keys()].every((k) => gas.anchor[tail + k] === comet), noHole: gas.hole === null,
          blueOut: [Math.min(...blue.map((b) => b[0])), Math.max(...blue.map((b) => b[0])), Math.max(...blue.map((b) => b[1]))],
          driftAside: Math.max(...drift.map((d) => d[1])), driftOut: Math.min(...drift.map((d) => d[0])),
          streams: [0, 1, 2].some((k) => Math.abs(later[(tail + k) * AT] - now[(tail + k) * AT]) > 1e-3),
          tailPlain: gas.look[tail * LOOK + 5] < 0 && gas.look[tail * LOOK + 6] > 1e5,
          smallTail: [smallTail.count, gas.anchor[smallTail.start] === small,
                      dot(where(now, smallTail.start).map((v, k) => v - smallHead[k]), smallHead.map((v, k) => v - smallSun[k])) > 0],
          tailShows: [now, half, hidden, headless].map((placed) => Math.max(...[...Array(TAILS).keys()].map((k) => placed[(tail + k) * AT + 4]))),
          cometBorn: birth[comet],
          bridgeHidden: hidden[gas.motes * AT + 4], flatZ: Math.max(...[...flatAt].filter((_, index) => index % AT === 2).slice(0, flatGas.motes).map(Math.abs)),
          finite: [...now].every(Number.isFinite) && [...gas.look].every(Number.isFinite),
        }));
    """)

    assert out["sizes"] == [7, 5, 7, 5] and out["gapless"] and out["total"]  # every light is in one run, and the runs meet
    # One run of gas and one of dust to a galaxy; the small project has no gas and no dust, only its comet's tails.
    assert out["galaxies"] == [5, 6] and out["smallTail"] == [25, True, True]
    assert out["heart"] < 1e-3 and 0.1 < out["heartSize"] < 0.2 and out["plain"] and out["wispy"]
    # The dust of stars: many points to a galaxy, each close by a memory, each a point on the screen however near.
    assert out["dust"][0] >= 90 and out["dust"][1] < 0.16 and out["dust"][2] < 2 and out["inReach"]
    # The comet: both tails hang on the task at work and stream. The blue one runs straight out from the head along
    # the line from the sun, a quarter of the galaxy's reach at most; the one of dust leaves that line to one side.
    assert out["tailOn"] and out["streams"] and out["tailPlain"] and out["noHole"]
    # A tail is there once its task is (this one was opened after day 352), steps back with its galaxy, and goes
    # when its head is switched off with its kind.
    assert out["cometBorn"] > 352 and out["tailShows"][0] > 0.5 and out["tailShows"][1:] == [0, 0, 0]
    assert 0 < out["blueOut"][0] and out["blueOut"][1] < 0.25 and out["blueOut"][2] < 1e-4
    assert out["driftAside"] > 0.01 and out["driftOut"] > 0
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


def test_the_far_sky_is_a_night_of_many_faint_stars_and_a_band(run) -> None:
    out = run("""
        import { farSky, FAR, NEBULAE, BAND_POLE, BAND_ACROSS, BAND_ALONG, BAND_HEART } from "./far.mjs";
        const sky = farSky(), again = farSky(), at = (k) => [...sky.lights.slice(k * FAR, k * FAR + 3)];
        const dot = (a, b) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
        let longest = 0, shortest = 9, nearBand = 0, faint = 0, bright = 0, strongest = 0, smallest = 99, flattest = 0;
        let atHeart = 0, opposite = 0;
        for (let k = 0; k < sky.stars; k++) {
          const d = at(k), length = Math.hypot(...d), size = sky.lights[k * FAR + 7], off = Math.abs(dot(d, BAND_POLE));
          longest = Math.max(longest, length); shortest = Math.min(shortest, length);
          if (off < 0.17) nearBand++;
          if (size < 1.6) faint++; else if (size > 4.1) bright++;
          strongest = Math.max(strongest, sky.lights[k * FAR + 6]); smallest = Math.min(smallest, size);
          flattest = Math.max(flattest, sky.lights[k * FAR + 10]);
          if (off > 0.3) continue;
          // Along the band: how far round from its heart, the short way.
          const round = Math.atan2(dot(d, BAND_ALONG), dot(d, BAND_ACROSS));
          const from = Math.abs(((round - BAND_HEART + Math.PI * 3) % (Math.PI * 2)) - Math.PI);
          if (from < 0.6) atHeart++; else if (from > Math.PI - 0.6) opposite++;
        }
        const galaxies = [];
        for (let k = sky.stars; k < sky.count; k++) galaxies.push([Math.abs(dot(at(k), BAND_POLE)), sky.lights[k * FAR + 10], sky.lights[k * FAR + 7]]);
        console.log(JSON.stringify({
          floats: [FAR, sky.lights.length / sky.count], stars: sky.stars, count: sky.count, unit: [shortest, longest],
          nearBand: nearBand / sky.stars, faint: faint / sky.stars, bright: bright / sky.stars, strongest, smallest, flattest, atHeart, opposite,
          galaxies: [Math.min(...galaxies.map((g) => g[0])), Math.min(...galaxies.map((g) => g[1])), Math.max(...galaxies.map((g) => g[1])), Math.min(...galaxies.map((g) => g[2]))],
          square: [dot(BAND_POLE, BAND_ACROSS), dot(BAND_POLE, BAND_ALONG), dot(BAND_ACROSS, BAND_ALONG), dot(BAND_POLE, BAND_POLE)],
          clouds: NEBULAE.map((cloud) => [Math.hypot(...cloud.at) - 1, dot(cloud.at, cloud.aside), dot(cloud.at, cloud.over), dot(cloud.aside, cloud.over)]),
          faintClouds: NEBULAE.every((cloud) => cloud.strength <= 0.15 && cloud.reach < 0.5),
          same: sky.lights.every((value, index) => value === again.lights[index]), finite: [...sky.lights].every(Number.isFinite),
        }));
    """)

    assert out["floats"] == [11, 11] and out["stars"] == 24000 and out["count"] == 24046
    assert _near(out["unit"], [1, 1], within=1e-5)  # every light is a direction, at no distance
    # Half the stars crowd the band: spread evenly, a sixth of them would lie that near its middle.
    assert out["nearBand"] > 0.4 and out["atHeart"] > 1.5 * out["opposite"]  # and more of them toward its heart
    # Many faint, few bright; none as strong as a memory, none too small to be drawn; a star is a point, not a smudge.
    assert out["faint"] > 0.8 and 0.001 < out["bright"] < 0.03 and out["strongest"] < 0.75 and out["smallest"] > 1.1
    assert out["flattest"] == 0
    # The far galaxies keep off the band and are each longer than wide.
    assert out["galaxies"][0] >= 0.2 and 0.2 < out["galaxies"][1] and out["galaxies"][2] < 0.85 and out["galaxies"][3] >= 8
    assert _near(out["square"], [0, 0, 0, 1], within=1e-6)
    assert all(_near(cloud, [0, 0, 0, 0], within=1e-6) for cloud in out["clouds"]) and out["faintClouds"]
    assert out["same"] and out["finite"]  # the same night on every visit


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
                                     shapes: [graph.shape[1], graph.shape[6], graph.shape[0]], decided: [graph.mark[6] & 16, graph.mark[1] & 16],
                                     finite: [...graph.color].every(Number.isFinite) }));
    """)

    assert out["youth"] == [0, 1, 0.5]
    assert out["old"] > out["young"] + 0.05  # the old stars of the heart are warm, the young ones of the rim blue-white
    assert out["spread"] > 0.01  # and no two are one colour
    # A decision is a star as any event is; that it was decided is a mark on it, for the ring it gives off.
    assert out["shapes"] == [0, 0, 4] and out["decided"] == [16, 0]
    assert out["finite"]


def test_what_a_light_is_besides_its_kind_is_read_off_the_memory(run) -> None:
    out = run("""
        import { Graph, MARK, FIGURE } from "./model.mjs";
        const DAY = 86400, T0 = 1000, END = T0 + 60 * DAY;
        const nodes = [
          { id: "p:a", k: "project", l: "alpha", p: 0, t: null, u: null },
          { id: "t:a/now", k: "task", l: "now", p: 0, t: T0 + 50 * DAY, u: END - DAY, o: 1 },   // open, touched yesterday
          { id: "t:a/idle", k: "task", l: "idle", p: 0, t: T0, u: T0 + 5 * DAY, o: 1 },         // open, untouched for weeks
          { id: "t:a/done", k: "task", l: "done", p: 0, t: T0 + 50 * DAY, u: END - DAY },       // touched yesterday, closed
          { id: "e:d", k: "event", l: "decided", p: 0, t: T0 + 10 * DAY, u: null, c: "decision" },
          { id: "e:r", k: "event", l: "daybook", p: 0, t: T0 + 11 * DAY, u: null, c: "daily_summary" },
          { id: "n:index", k: "note", l: "index", p: 0, t: T0 + 12 * DAY, u: null },
          { id: "e:copy", k: "event", l: "copy", p: 0, t: T0 + 13 * DAY, u: null, c: "note" },
        ];
        for (let i = 0; i < 12; i++) nodes.push({ id: "n:" + i, k: "note", l: "n" + i, p: 0, t: T0 + (20 + i) * DAY, u: null });
        // Four more copies of the decision, and plain events enough that two memories in the whole may be bright stars.
        for (let i = 0; i < 4; i++) nodes.push({ id: "e:c" + i, k: "event", l: "copy " + i, p: 0, t: T0 + (14 + i) * DAY, u: null, c: "note" });
        for (let i = 0; i < 12; i++) nodes.push({ id: "e:f" + i, k: "event", l: "plain " + i, p: 0, t: T0 + (40 + i) * DAY, u: null, c: "note" });
        // The task at work has three memories, recorded out of order; the index links to every note; two notes link to each other.
        const edges = [[10, 1, 0], [8, 1, 0], [9, 1, 0]];
        for (let i = 0; i < 12; i++) edges.push([6, 8 + i, 2]);
        edges.push([8, 9, 2]);
        // Two notes that link each other: a pair, however many lines are drawn between them.
        edges.push([13, 14, 2], [14, 13, 2]);
        const graph = new Graph({ nodes, projects: ["alpha"], edges, span: [T0, END] });
        // The same memory read forty days after its last entry.
        const later = new Graph({ nodes, projects: ["alpha"], edges, span: [T0, END], generated_at: new Date((END + 40 * DAY) * 1000).toISOString() });
        const first = [...graph.mark];
        // The daybook resembles thirteen memories, more than anything else is joined to; the decision has five copies.
        graph.addSimilar([...Array(12).keys()].map((i) => ["e:r", "n:" + i, 0.9, 0]).concat([["e:r", "e:f0", 0.9, 0]],
          [["e:d", "e:copy", 0.99, 1]], [0, 1, 2, 3].map((i) => ["e:d", "e:c" + i, 0.99, 1])));
        const buffers = graph.edgeBuffers(), lines = buffers.kind.length;
        console.log(JSON.stringify({
          marks: MARK, figure: FIGURE, first: first.slice(0, 8), then: [...graph.mark].slice(0, 8), notes: [...graph.mark].slice(8, 20),
          copies: [...graph.mark].slice(20, 24), plain: [...graph.mark].slice(24),
          figures: graph.figures(), drawn: [...buffers.kind].slice(lines - 3), ends: [...buffers.ends].slice(lines * 2 - 6),
          ofIndex: buffers.color[3 * 4 + 3], ofPair: buffers.color[15 * 4 + 3],
          read: [graph.now - END, later.now - END], later: later.mark[1],
        }));
    """)

    marks = out["marks"]
    assert marks == {"routine": 1, "twin": 2, "working": 4, "hub": 8, "decided": 16}
    # A comet is a task open and touched lately, not one left idle and not one closed. A decision is marked as one,
    # what the machinery wrote as routine, and the memory joined to the most others is the bright star.
    assert out["first"] == [0, marks["working"], 0, 0, marks["decided"], marks["routine"], marks["hub"], 0]
    # With the lines by meaning: the decision and its copies are twins. Two memories might be bright stars here and
    # one is. The daybook resembles thirteen memories and is none: the machinery resembles itself. The decision is
    # joined to five and is none either: they are its copies, not other things it is joined to.
    assert out["then"] == [0, marks["working"], 0, 0, marks["decided"] + marks["twin"], marks["routine"], marks["hub"], marks["twin"]]
    # The note that links another and is linked back is joined to three memories, not to four: no bright star.
    assert set(out["notes"]) == {0} and out["copies"] == [marks["twin"]] * 4 and set(out["plain"]) == {0}
    # Lately is counted back from the day the memory is read: with no word of it, from its last entry. Read forty
    # days on, the task touched "yesterday" is at work no more.
    assert out["read"] == [0, 40 * DAY] and out["later"] == 0
    # The figure of a task goes from the task to its first memory and on in the order they were written.
    assert out["figures"] == [[1, 8], [8, 9], [9, 10]]
    assert out["drawn"] == [out["figure"]] * 3 and out["ends"] == [1, 8, 8, 9, 9, 10]
    # A link of the index is one of twelve and is drawn faint; a link between two notes of few links is drawn in full.
    assert abs(out["ofIndex"] - 0.18 * 5 / 12) < 1e-4 and abs(out["ofPair"] - 0.18) < 1e-4


def test_what_was_let_go_circles_a_hole_apart_from_the_galaxies(run) -> None:
    out = run(_GALAXIES + """
        import { Gas, LOOK, AT, HOLE_SEED, HOLE_WIDE } from "./gas.mjs";
        // Three memories of the spiral are stale, one of the project too small for a form, and the one of no project.
        const gone = [first[0], first[0] + 1, first[0] + 2, first[1], 6], stale = new Uint8Array(nodes.length);
        for (const i of gone) stale[i] = 1;
        const raw = new Float32Array(laid), none = new Shaper(graph), shaper = new Shaper({ ...graph, stale });
        none.shape(raw, 3);
        const shaped = shaper.shape(raw, 3), hole = shaper.hole, spiral = shaper.galaxies.get(0);
        const fromHole = (positions, i) => Math.hypot(...at(positions, i).map((v, a) => v - hole.centre[a])) / hole.shadow;
        const outs = hole.members.map((i) => fromHole(shaped, i));
        const before = shaper.turn(shaped);
        // A few seconds: less than half a turn for the nearest, so how far each went can be read off two places.
        shaper.spin(5);
        const after = shaper.turn(shaped), one = shaper.turned(shaped, gone[0]);
        const went = (i) => {
          const a = at(before, i).map((v, k) => v - hole.centre[k]), b = at(after, i).map((v, k) => v - hole.centre[k]);
          return Math.acos(Math.max(-1, Math.min(1, dot(a, b) / (Math.hypot(...a) * Math.hypot(...b)))));
        };
        const clear = Math.min(...shaper.suns.map(({ sun }) => fromHole(shaped, sun)));
        // The middle of the suns as the layout left them, and how far from it the hole and the outermost sun are drawn.
        const middle = [0, 1, 2].map((a) => mean(shaper.suns.map(({ sun }) => raw[sun * 3 + a])));
        const fromMiddle = (point) => Math.hypot(...point.map((v, a) => v - middle[a]));
        const outmost = Math.max(...shaper.suns.map(({ sun }) => fromMiddle(at(shaped, sun))));
        const again = new Shaper({ ...graph, stale }).shape(raw, 3);
        const birth = Float32Array.from(nodes, (node) => node.t / DAY);
        const world = { ...graph, stale, edges: [], birth, mark: new Float32Array(nodes.length), colorOf: () => "#8db7e0" };
        const gas = new Gas(world, shaper);
        gas.updateBorn(400);
        const placed = gas.place(after, { time: 0, play: 400, flat: false, centre: [0, 0, 0], radius: 900, beside: 1, shows: () => 1, seen: () => true, hole: 0.5 });
        const light = gas.hole.start;
        const runs = [...gas.ranges.values(), ...gas.dust.values(), gas.field, gas.hole].sort((a, b) => a.start - b.start);
        const flat = new Shaper({ ...graph, stale }), flatShaped = flat.shape(new Float32Array(laid.map((v, i) => (i % 3 === 2 ? 0 : v))), 2);
        flat.spin(30);
        const flatTurned = flat.turn(flatShaped);
        /** @type {Record<string, any>} */
        const report = {
          noHole: none.hole === null, members: [...hole.members].sort((a, b) => a - b), fallen: gone.every((i) => shaper.fallen[i] === 1),
          left: [spiral.members.length, spiral.all.length, gone.slice(0, 3).some((i) => spiral.members.includes(i))],
          forms: [spiral.form === none.galaxies.get(0).form, spiral.arms === none.galaxies.get(0).arms],
          outs: [Math.min(...outs), Math.max(...outs)], oldestNearest: outs[0] === Math.min(...outs),
          kept: hole.members.map((i) => fromHole(after, i) - fromHole(before, i)),
          went: [went(hole.members[0]), went(hole.members[hole.members.length - 1])],
          one: one.map((v, k) => v - at(after, gone[0])[k]), clear, pole: Math.hypot(...hole.n),
          same: [...again].every((value, index) => value === shaped[index]), finite: [...after].every(Number.isFinite),
          light: [placed[light * AT], placed[light * AT + 1], placed[light * AT + 2]].map((v, k) => v - hole.centre[k]),
          wide: placed[light * AT + 3] / hole.shadow, shows: placed[light * AT + 4], seed: gas.look[light * LOOK + 5],
          holeSeed: HOLE_SEED, holeWide: HOLE_WIDE, total: runs.reduce((sum, r) => sum + r.count, 0) === gas.count,
          flatZ: Math.max(...gone.flatMap((i) => [flatShaped[i * 3 + 2], flatTurned[i * 3 + 2]]).map(Math.abs)), flatPole: flat.hole.n,
          apart: fromMiddle(hole.centre) / outmost,
        };
        // One more is let go while the others are already circling: it starts from where it is put, with no leap.
        const late = first[0] + 5, more = Uint8Array.from(stale);
        more[late] = 1;
        shaper.restale(more);
        const reshaped = shaper.shape(raw, 3);
        report.late = shaper.turned(reshaped, late).map((v, k) => v - at(reshaped, late)[k]);
        // A light is put where it must be to be seen at a given place, whatever it turns with: one at the hole, one in the spiral.
        report.put = [gone[0], first[0] + 9].map((i) => {
          const moved = Float32Array.from(reshaped);
          moved.set(shaper.unturned(reshaped, i, [30, -40, 50]), i * 3);
          return shaper.turned(moved, i).map((v, k) => v - [30, -40, 50][k]);
        });
        report.turns = [shaper.galaxies.get(0).angle !== 0, Math.hypot(...shaper.unturned(reshaped, first[0] + 9, [30, -40, 50]).map((v, k) => v - [30, -40, 50][k])) > 1];
        // Nothing current lies in the disc of the hole.
        const outOfDisc = (which, positions) => Math.min(...[...positions.keys()].filter((k) => k % 3 === 0 && !which.fallen[k / 3])
          .map((k) => Math.hypot(positions[k] - which.hole.centre[0], positions[k + 1] - which.hole.centre[1], positions[k + 2] - which.hole.centre[2]) / which.hole.shadow));
        report.disc = outOfDisc(shaper, reshaped);
        report.early = Math.hypot(...shaper.turned(reshaped, gone[0]).map((v, k) => v - at(reshaped, gone[0])[k]));
        // Three small projects laid in one place, one memory of them stale: the hole would lie among their suns.
        const crowd = [0, 1, 2].map((p) => ({ k: "project", p, t: 0 })), crowded = [0, 0, 0, 1, 0, 0, 0, 1, 0];
        for (let p = 0; p < 3; p++) for (let i = 0; i < 10; i++) {
          crowd.push({ k: "event", p, t: NOW - (p * 10 + i) * DAY });
          crowded.push(crowded[p * 3] + Math.cos(i) * 10, crowded[p * 3 + 1] + Math.sin(i) * 10, Math.cos(i * 2.4) * 10);
        }
        // With them a project too small for a galaxy, of three memories; and one memory of no project.
        const tiny = crowd.length;
        crowd.push({ k: "project", p: 3, t: 0 }, { k: "event", p: 3, t: NOW }, { k: "event", p: 3, t: NOW }, { k: "event", p: 3, t: NOW }, { k: "note", p: null, t: NOW });
        crowded.push(0.5, 0.5, 0, 5, 0.5, 0, 0.5, 5, 0, 0.5, 0.5, 5, 400, 400, 400);
        const forgotten = new Uint8Array(crowd.length);
        forgotten[3] = 1;
        const tight = new Shaper({ nodes: crowd, projectNode: new Map([[0, 0], [1, 1], [2, 2], [3, tiny]]), stale: forgotten });
        let tightShaped = tight.shape(new Float32Array(crowded), 3);
        // One memory of the small project is laid far from its sun, on the side where the hole is.
        const toHole = tight.hole.centre.map((v, a) => v - tightShaped[tiny * 3 + a]), length = Math.hypot(...toHole);
        toHole.forEach((v, a) => { crowded[(tiny + 1) * 3 + a] = crowded[tiny * 3 + a] + (v / length) * 150; });
        tightShaped = tight.shape(new Float32Array(crowded), 3);
        // And the memory of no project is laid where it would be drawn in the middle of the hole (everything is drawn
        // 0.78 as far from the middle of the suns as it is laid).
        const amid = [0, 1, 2].map((a) => mean([0, 1, 2, tiny].map((sun) => crowded[sun * 3 + a])));
        tight.hole.centre.forEach((v, a) => { crowded[(tiny + 4) * 3 + a] = amid[a] + (v - amid[a]) / 0.78; });
        tightShaped = tight.shape(new Float32Array(crowded), 3);
        report.tight = Math.min(...[0, 1, 2, tiny].map((sun) => Math.hypot(...at(tightShaped, sun).map((v, a) => v - tight.hole.centre[a])) / tight.hole.shadow));
        report.tightDisc = outOfDisc(tight, tightShaped);
        report.ball = [tight.balls.get(3).reach, [...tightShaped].every(Number.isFinite)];
        // Marked current again, they are back in their galaxies and the hole is gone.
        shaper.restale(new Uint8Array(nodes.length));
        console.log(JSON.stringify({ ...report, back: [shaper.hole === null, spiral.members.length, shaper.fallen.some((v) => v === 1)] }));
    """)

    assert out["noHole"]  # with nothing let go there is no hole
    assert len(out["members"]) == 5 and out["fallen"]
    # They leave their galaxy; the galaxy keeps the form read off the whole project.
    assert out["left"] == [237, 240, False] and out["forms"] == [True, True]
    # In a disc about the hole, the oldest nearest, each a little off the plane.
    assert 1.6 < out["outs"][0] and out["outs"][1] < 3.6 and out["oldestNearest"]
    # They circle: as far from the hole as before, the nearest faster than the furthest.
    assert all(abs(change) < 1e-2 for change in out["kept"]) and out["went"][0] > out["went"][1] > 0.2
    assert _near(out["one"], [0, 0, 0], within=1e-2)
    # The hole is out at the edge of the sky, not in the middle of it, and no sun lies through its disc: not as the
    # galaxies happen to lie here, and not when their suns crowd the place the hole takes.
    assert out["apart"] > 0.6 and out["clear"] > 3 and out["tight"] > 3
    # Nothing current lies in the disc (it reaches three and a half shadows out): not the rim of a galaxy, not a far
    # memory of a project too small for one, which keeps clear by its whole reach, and not a memory of no project.
    assert out["disc"] > 3.5 and out["tightDisc"] > 3.5 and abs(out["ball"][0] - 150 * 0.78) < 1e-3 and out["ball"][1]
    # A light whose company changes is put where it will be seen in the place it was seen: at the hole or in a
    # galaxy that has turned (where that is not the place itself).
    assert all(_near(put, [0, 0, 0], within=1e-2) for put in out["put"]) and out["turns"] == [True, True]
    assert abs(out["pole"] - 1) < 1e-6 and out["same"] and out["finite"]
    # A memory let go later begins its circle where it is put; those before it have gone on.
    assert _near(out["late"], [0, 0, 0], within=1e-3) and out["early"] > 1
    # The hole is one light of its own, where the hole is, a few shadows wide, and shows as much as the page says.
    assert _near(out["light"], [0, 0, 0]) and abs(out["wide"] - out["holeWide"]) < 1e-3 and out["shows"] == 0.5
    assert out["seed"] == out["holeSeed"] == -2 and out["total"]
    assert out["flatZ"] == 0 and out["flatPole"] == [0, 0, 1]
    assert out["back"] == [True, 240, False]


# ---------------------------------------------------------------------------
# The page's own wiring, on a fake document
# ---------------------------------------------------------------------------


def test_the_page_runs_on_an_empty_memory_and_the_overture_gives_way_to_the_reader(run) -> None:
    out = run("""
        import { openPage, memory } from "./atlas_fake_page.mjs";
        import { writePlace, dayOf } from "./urlstate.mjs";
        const text = (error) => (error ? String(error.stack || error) : null);
        // An empty memory: there is nothing to play, and the frames go on.
        const empty = await openPage({ data: { nodes: [], projects: [], edges: [], span: null, facts: [], actions: { enabled: false, token: null } } });
        const first = empty.frame(16), second = empty.frame(16);
        // A memory of two projects opened on no place in particular: the overture plays.
        const data = memory([{ name: "alpha", events: 60 }, { name: "beta", events: 40 }], { days: 100 });
        const page = await openPage({ data });
        page.finishLayout();
        let failed = null;
        for (let k = 0; k < 30; k++) failed = failed || page.frame(16);
        const during = page.uniform.u_play;
        // The reader goes back to an entry of an earlier day while it plays.
        page.location.hash = "#" + writePlace({ day: dayOf(data.span[0] + 49.5 * 86400) });
        page.window("popstate");
        failed = failed || page.frame(16);
        const restored = page.uniform.u_play;
        for (let k = 0; k < 400; k++) failed = failed || page.frame(16);
        const kept = page.uniform.u_play;
        // Another visit: a key ends the overture, and the camera is in within the second.
        const again = await openPage({ data });
        again.finishLayout();
        for (let k = 0; k < 30; k++) failed = failed || again.frame(16);
        const coming = again.uniform.u_fog[0];
        again.window("keydown", { key: "?" });
        failed = failed || again.frame(16);
        const ended = again.uniform.u_play;
        for (let k = 0; k < 50; k++) failed = failed || again.frame(16);
        const settled = again.uniform.u_fog[0];
        for (let k = 0; k < 120; k++) failed = failed || again.frame(16);
        // A page opened on the last memory of a project: it holds its line in brass. Then the reader is taken to a
        // day long before that memory was written.
        const held = await openPage({ data, hash: "#" + writePlace({ node: "e:0-59" }) });
        held.finishLayout();
        for (let k = 0; k < 10; k++) failed = failed || held.frame(16);
        const bowed = () => { held.inkLog.length = 0; failed = failed || held.frame(16); return held.inkLog.filter((call) => call[0] === "quadraticCurveTo").length; };
        const shown = bowed();
        held.location.hash = "#" + writePlace({ node: "e:0-59", day: dayOf(data.span[0] + 10 * 86400) });
        held.window("popstate");
        const before = bowed(), rings = held.inkLog.filter((call) => call[0] === "arc").length;
        console.log(JSON.stringify({
          shown, before, rings,
          first: text(first), second: text(second), failed: text(failed), errors: [...empty.errors, ...page.errors, ...again.errors, ...held.errors].map(String),
          during, restored, kept, ended, coming, settled, rested: again.uniform.u_fog[0],
          draws: page.glLog.length, feedback: page.glLog.filter((draw) => draw.feedback).length,
        }));
        process.exit(0);
    """)

    assert out["first"] is None and out["second"] is None and out["failed"] is None and out["errors"] == []
    # The overture is under way after half a second; the day the address names is taken and kept to the end.
    assert 0 < out["during"] < 100 and 49 < out["restored"] < 51 and out["kept"] == out["restored"]
    # A key ends it: the time is at the last day at once, and the camera, which was still coming in, is at rest
    # within the second.
    assert out["ended"] == 100 and out["coming"] > out["settled"] * 1.05 and abs(out["settled"] - out["rested"]) < 1e-3 * out["rested"]
    # A memory not yet written on the day shown holds no line in brass and wears no ring.
    assert out["shown"] > 0 and out["before"] == 0 and out["rings"] == 0
    # The gas is never drawn into the sheet it is reading.
    assert out["draws"] > 100 and out["feedback"] == 0


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
