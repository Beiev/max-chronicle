// @ts-check
/**
 * The Atlas: Chronicle's memory as a sky to turn and wander, and a river of time.
 *
 * Projects are galaxies: a sun, and around it the memories of the project in a
 * form of its own (a spiral, a ring, a great ball, a cloud), the oldest at the
 * heart, lit by its own gas and turning slowly. Tasks are ringed points,
 * events stars, notes small squares and fact slots diamonds.
 * Lines are what the memory records; dashed brass between memories is
 * resemblance by meaning. Drag turns the sky, the wheel comes closer, a click
 * opens a memory or the line between two. The river at the bottom is how much
 * each project recorded per day: its two handles frame a stretch of time and
 * the sky shows what was known then.
 *
 * This file is the wiring. What a thing is lives in model.js, where the layout
 * puts it in layout.worker.js, the form of its galaxy in shape.js and its gas
 * in gas.js, where it is drawn in camera.js, and each surface has a module of
 * its own: the far sky, the lights (render.js), the names and the brass lines
 * (ink.js), the panel's cards, the river.
 */

import { Renderer } from "./render.js";
import { Camera } from "./camera.js";
import { Graph, DAY, EDGE, EDGE_NAME, KIND_LABEL, SIMILAR_TINT, clamp, mulberry32, percent } from "./model.js";
import { Shaper } from "./shape.js";
import { Gas } from "./gas.js";
import { Sky } from "./sky.js";
import { Ink, cut, LINK_BOW } from "./ink.js";
import { pickNode, pickEdge } from "./pick.js";
import { Timeline, momentAt } from "./timeline.js";
import { el, kindChip } from "./panel.js";
import { Cards } from "./cards.js";
import { readPlace, writePlace, dayOf, dayStart, dayEnd } from "./urlstate.js";

const LAYOUT_KEY = "chronicle.atlas.layout.v2";
const LEGEND_KEY = "chronicle.atlas.legend.v1";
const HINT_KEY = "chronicle.atlas.hint.v2";
const COLD_TICKS = 340;
const WARM_TICKS = 120;
const PLAY_DAYS_PER_S = 2;
const FOLLOW_MS = 600;
const PANEL_W = 436;
const LEGEND_W = 236;
// Fitting leaves room on the sides for the names drawn beside the outermost suns.
const LABEL_ROOM = 150;
const TIMELINE_H = 134;
// The lights are drawn at full size in a frame this many pixels across, and smaller in a smaller one.
const FULL_LIGHTS_FRAME = 640;
const NARROW = 760;
/** Yaw and pitch of the first view, in radians. */
const OPENING_TURN = [-0.5, 0.3];
const IDLE_TURN_AFTER_MS = 14000;
const IDLE_TURN = 0.03; // radians a second: the sky drifts when nobody touches it
/** With a memory open the drift is a slow walk around it. */
const IDLE_TURN_HELD = 0.014;
/**
 * The overture: a page opened on no place in particular first plays the memory from its first day to now, seen from
 * further off and a little aside, and comes in as the sky fills. Any touch ends it.
 */
const OVERTURE_MS = 5600;
const OVERTURE_FROM = { back: 1.6, yaw: -0.75, pitch: 0.14 };
/** The group that is the hole and what circles it, in place of a sun. */
const HOLE = -2;
/** A memory joined to more than this many others keeps its lines as plain light: brass is for a handful. */
const INKED_MAX = 40;
/** How much light is left to what is not in focus: beside a memory or a line, and beside a project shown alone. */
const DIM_BESIDE_FOCUS = 0.34;
const DIM_BESIDE_PROJECT = 0.12;
/** The lines inside a galaxy can be pointed at from this near: the renderer has them well in view by then. */
const LINES_FROM = 2.2;
/** A memory in focus is seen with this much of the way to its sun, so it is seen in its place. */
const LEAN_TO_SUN = 0.55;
/** A galaxy showing less of its face than this (1 face on, 0 edge on) is turned to when a memory of it is opened. */
const FACE_SHOWN = 0.42;
/** How far the camera then leans from where it looked toward the pole of the disc. */
const FACE_TURNED_TO = 0.5;
/** @type {Record<string, string>} */
const DONE = {
  pin: "Pinned to the brief.", unpin: "Unpinned from the brief.", stale: "Marked stale.", fresh: "Marked current again.",
  duplicate: "The copy is hidden.", restore: "The copy is back.",
};
/** How to move around, as the card behind the question mark says it. @type {[string[], string][]} */
const HOW = [
  [["Drag"], "turn the sky"], [["Shift", "drag"], "slide it"], [["Scroll"], "come closer"],
  [["Click"], "a memory, or a line to see why two are joined"], [["F"], "all of it"], [["2", "3"], "flat map or sky"],
  [["M"], "lines of meaning"], [["Space"], "play"], [["←", "→"], "a day back or on"], [["/"], "search"], [["I"], "the sky alone"],
];
const LINE_HINT = ["part of a task", "part of a project", "a link in a note", "a fact it set", "close in meaning"];
const number = new Intl.NumberFormat("en");
const dayFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", year: "numeric" });
const shortFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short" });

/** @param {string} id */
const $ = (id) => /** @type {HTMLElement} */ (document.getElementById(id));
/** @param {number} ms */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function load() {
  for (let attempt = 0; ; attempt++) {
    const response = await fetch("/ui/api/atlas", { headers: { accept: "application/json" } });
    if (response.status === 503 && attempt < 120) { await sleep(1000); continue; }
    if (!response.ok) throw new Error(`The memory did not load (HTTP ${response.status}).`);
    return response.json();
  }
}

/** One answer per address, asked once; a failure is forgotten so the next look asks again. @param {string} url */
function once(url) {
  if (!once.cache.has(url)) {
    once.cache.set(url, fetch(url).then((r) => (r.ok ? r.json() : null)).catch(() => { once.cache.delete(url); return null; }));
  }
  return /** @type {Promise<any>} */ (once.cache.get(url));
}
/** @type {Map<string, Promise<any>>} */
once.cache = new Map();

function storageGet(/** @type {string} */ key) {
  try { return JSON.parse(localStorage.getItem(key) || "null"); } catch { return null; }
}
function storageSet(/** @type {string} */ key, /** @type {any} */ value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* private window or full: asked again next time */ }
}

async function main() {
  const loadingText = $("loading-text");
  let data;
  try {
    data = await load();
  } catch (error) {
    loadingText.textContent = error instanceof Error ? error.message : String(error);
    return;
  }
  const graph = new Graph(data);
  const { nodes, n, edges, days } = graph;
  /** @type {{enabled: boolean, token: string | null}} */
  const actions = data.actions || { enabled: false, token: null };
  const pins = new Set(/** @type {number[]} */ ([...graph.pinned.keys()].filter((i) => graph.pinned[i])));
  loadingText.textContent = `Mapping ${number.format(n)} memories…`;

  // ---------- surfaces ----------
  const glCanvas = /** @type {HTMLCanvasElement} */ ($("gl"));
  /** @type {Renderer} */ let renderer;
  try {
    renderer = new Renderer(glCanvas);
  } catch (error) {
    loadingText.textContent = error instanceof Error ? error.message : String(error);
    return;
  }
  renderer.setNodes(graph);
  const camera = new Camera();
  const shaper = new Shaper(graph);
  let gas = new Gas(graph, shaper);
  /**
   * What is drawn together, a galaxy's gas and then its memories: one group to a project, and one of what belongs to
   * none. Each has the node its distance is measured by: its sun.
   * @type {{sun: number, nodes: number[], gas: import("./render.js").Range | null, dust: import("./render.js").Range | null}[]}
   */
  let groups = [];
  /** The gas again, once the memory has changed what it rests on: the lines by meaning bridge galaxies too. */
  function layGas() {
    gas = new Gas(graph, shaper);
    renderer.setGas(gas.look, gas.count);
    /** @type {Map<number | null, number[]>} */ const members = new Map();
    /** @type {number[]} */ const fallen = [];
    nodes.forEach((node, i) => {
      // What was let go is drawn with the hole it circles, not with the galaxy it left.
      if (shaper.fallen[i]) { fallen.push(i); return; }
      const key = node.p !== null && graph.projectNode.has(node.p) ? node.p : null;
      const list = members.get(key) || [];
      list.push(i);
      members.set(key, list);
    });
    groups = [...members].map(([project, list]) => ({
      sun: project === null ? -1 : /** @type {number} */ (graph.projectNode.get(project)), nodes: list,
      gas: project === null ? null : gas.ranges.get(project) || null, dust: project === null ? null : gas.dust.get(project) || null,
    }));
    if (gas.hole) groups.push({ sun: HOLE, nodes: fallen, gas: null, dust: gas.hole });
    renderer.setGroups(groups);
  }
  layGas();
  /** @type {number[]} */ const order = [];
  const sky = new Sky(/** @type {HTMLCanvasElement} */ ($("sky")), shaper);
  const ink = new Ink(/** @type {HTMLCanvasElement} */ ($("ink")), graph);
  const projected = new Float32Array(n * 3);
  const nodeHl = new Float32Array(n);
  /** @type {Float32Array} */ let edgeHl = new Float32Array(edges.length);
  /** The lines of the memory in focus that the ink draws in brass. @type {number[]} */
  const inked = [];
  /** The two ends of every line drawn: the edges of the memory, then the figures of the tasks. */
  let drawnEnds = new Int32Array(0);
  const uploadEdges = () => {
    const buffers = graph.edgeBuffers();
    renderer.setEdges(buffers.ends, buffers);
    drawnEnds = buffers.ends;
    edgeHl = new Float32Array(buffers.ends.length / 2);
  };
  uploadEdges();

  // ---------- state ----------
  const kindOn = [true, true, true, true, true];
  let meaningOn = false, similarReady = false, similarNote = "computing…";
  let focus = -1, line = -1, hovered = -1, hoveredLine = -1;
  /** @type {number | null} */ let isolate = null;
  /** @type {number[]} */ const trail = [];
  let play = days, from = 0, playing = false, speed = 1, following = false, followAt = 0;
  let travel = 0, focusMix = 0, similarMix = 0;
  let panelOpen = false, rangeOpen = false, legendOpen = storageGet(LEGEND_KEY) !== false;
  let W = window.innerWidth, H = window.innerHeight, dpr = Math.min(window.devicePixelRatio || 1, 2);
  let userMoved = false, lastInput = performance.now();
  /** When the overture began, 0 when none plays; and whether only the sky is shown. */
  let overture = 0, immersed = false;
  const calm = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  /** The line an address asked for before the lines by meaning had arrived; any move of the reader's drops it. @type {import("./urlstate.js").Place | null} */
  let awaitedLine = null;
  /**
   * How a move reaches the page: a step the reader takes (it joins the path
   * walked and the history), a return to a place on that path (the history
   * only), or a place restored from the address (nothing is written, and the
   * time stays as the address has it).
   * @typedef {"step" | "return" | "restore"} Move
   */
  /** What the panel is on, as one word: actions and their outcomes belong to it. */
  const subject = () => (focus >= 0 ? `n${focus}` : line >= 0 ? `l${line}` : rangeOpen ? "days" : "");

  /** A memory before the start of the range is dark; suns and tasks stay, as the frame of the map. @param {number} i */
  const structural = (i) => nodes[i].k === "project" || nodes[i].k === "task";
  /** @param {number} i */
  const shown = (i) => nodeHl[i] >= 0 && graph.birth[i] <= play + 0.01 && (graph.birth[i] >= from - 0.01 || structural(i));
  /** @param {number} i */
  const lit = (i) => nodeHl[i] >= 1;

  function updateHighlight() {
    for (let i = 0; i < n; i++) nodeHl[i] = kindOn[graph.kind[i]] && !graph.gone[i] ? 0 : -1;
    // A line is drawn while both its ends are: an edge of the memory, or a stroke of a task's figure.
    for (let index = 0; index < edgeHl.length; index++) {
      edgeHl[index] = nodeHl[drawnEnds[index * 2]] < 0 || nodeHl[drawnEnds[index * 2 + 1]] < 0 ? -1 : 0;
    }
    inked.length = 0;
    if (focus >= 0) {
      // Its own galaxy stays half lit around it: a memory is seen in its place.
      const home = nodes[focus].p;
      if (home !== null) for (let i = 0; i < n; i++) if (nodes[i].p === home && nodeHl[i] === 0) nodeHl[i] = 0.5;
      nodeHl[focus] = 2;
      const links = graph.adj[focus].filter((link) => nodeHl[link.o] >= 0);
      const brass = links.length <= INKED_MAX;
      for (const link of links) {
        nodeHl[link.o] = Math.max(nodeHl[link.o], 1);
        edgeHl[link.e] = brass ? 2 : 1;
        if (brass) inked.push(link.e);
      }
    } else if (line >= 0) {
      const e = edges[line];
      nodeHl[e.s] = nodeHl[e.t] = 2;
      edgeHl[line] = 1;
    } else if (isolate !== null) {
      // A project shown alone keeps its lines as they are: lit all at once they are a web over the galaxy.
      for (let i = 0; i < n; i++) if (nodes[i].p === isolate && nodeHl[i] >= 0) nodeHl[i] = 1;
      for (let index = 0; index < edgeHl.length; index++) {
        if (nodeHl[drawnEnds[index * 2]] > 0 && nodeHl[drawnEnds[index * 2 + 1]] > 0) edgeHl[index] = 0.5;
      }
    }
    renderer.setHighlight(nodeHl, edgeHl);
  }
  updateHighlight();

  // ---------- positions and layout ----------
  /** Where the lights would be had no galaxy turned; they glide to where the layout and the form of their galaxy put them. */
  let positions = new Float32Array(n * 3);
  let goal = positions;
  /** Where the lights are drawn: the same, each galaxy turned as far as it has turned. */
  const drawn = new Float32Array(n * 3);
  /** Whether the lights must be put again where they are drawn, though nothing glides and nothing turns. */
  let stale = true;
  /** Where the layout puts the nodes, before each galaxy is given its form: what the layout starts from and what is kept. */
  let laid = positions;
  let gliding = false;
  /** @type {Worker | null} */ let worker = null;
  /** @type {2 | 3} */ let dims = 3;

  /** @param {2 | 3} into @param {boolean} first whether this is the page's first layout */
  function layout(into, first) {
    worker?.terminate();
    dims = into;
    const cached = storageGet(`${LAYOUT_KEY}.${dims}d`);
    let seeded;
    if (!first && !(cached && cached.positions)) {
      // No layout of this kind yet: start from where the lights are, so the map folds or unfolds in place.
      const random = mulberry32(7), init = positions.slice();
      for (let i = 0; i < n; i++) init[i * 3 + 2] = dims === 3 ? (random() - 0.5) * 320 : 0;
      seeded = { positions: init, warm: false };
    } else {
      seeded = graph.seedPositions(dims, (cached && cached.positions) || {});
    }
    laid = seeded.positions;
    goal = shaper.shape(laid, dims);
    if (first) positions = goal.slice();
    gliding = true;
    const ticks = seeded.warm ? WARM_TICKS : COLD_TICKS;
    const explicit = new Int32Array(graph.explicitCount * 3);
    edges.slice(0, graph.explicitCount).forEach((e, index) => { explicit.set([e.s, e.t, e.k], index * 3); });
    const running = new Worker("/ui/static/atlas/layout.worker.js");
    worker = running;
    running.onmessage = (message) => {
      if (worker !== running) return;
      laid = message.data.positions;
      goal = shaper.shape(laid, dims);
      gliding = true;
      const narrowed = focus >= 0 || line >= 0 || isolate !== null;
      // While the map forms the whole of it stays in view; a memory in focus is flown to once it has its place.
      if (!userMoved && (!narrowed || message.data.done)) aim(narrowed);
      if (message.data.done) {
        running.terminate();
        const saved = /** @type {Record<string, number[]>} */ ({});
        nodes.forEach((node, i) => { saved[node.id] = [0, 1, 2].map((a) => Math.round(laid[i * 3 + a] * 10) / 10); });
        storageSet(`${LAYOUT_KEY}.${dims}d`, { positions: saved });
        $("loading").classList.add("done");
      } else {
        loadingText.textContent = `Mapping ${number.format(n)} memories… ${Math.round((message.data.tick / ticks) * 100)}%`;
        $("loading").classList.remove("done");
      }
    };
    running.postMessage({ n, dims, kinds: graph.kind, edges: explicit, init: laid.slice(), ticks, warm: seeded.warm });
  }

  // ---------- the frame and the fit ----------
  /** How much of the window the panels cover on each side. @returns {[number, number]} */
  const covered = () => [legendOpen && !immersed && W > 900 ? LEGEND_W : 0, panelOpen && W > NARROW ? PANEL_W : 0];
  function frameOf() {
    const [left, right] = covered(), top = immersed ? 0 : 56, bottom = immersed ? 0 : TIMELINE_H;
    return { cx: left + (W - left - right) / 2, cy: top + (H - top - bottom) / 2,
             w: Math.max(W - left - right - Math.min(LABEL_ROOM, W * 0.2), 160), h: Math.max(H - top - bottom - 40, 160), height: H };
  }
  let radius = 800;
  /** The middle of everything shown and how far out it reaches: the globe the sky draws. @type {import("./sky.js").Globe | null} */
  let globe = null;
  /** The middle of what is shown, and the radius that holds nearly all of it and every sun. @param {(i: number) => boolean} [among] */
  function ball(among = (i) => nodeHl[i] >= 0) {
    let cx = 0, cy = 0, cz = 0, count = 0;
    for (let i = 0; i < n; i++) if (among(i)) { cx += goal[i * 3]; cy += goal[i * 3 + 1]; cz += goal[i * 3 + 2]; count++; }
    if (!count) return { centre: /** @type {[number, number, number]} */ ([0, 0, 0]), radius: 200 };
    cx /= count; cy /= count; cz /= count;
    const far = [];
    let suns = 0;
    for (let i = 0; i < n; i++) {
      if (!among(i)) continue;
      const d = Math.hypot(goal[i * 3] - cx, goal[i * 3 + 1] - cy, goal[i * 3 + 2] - cz);
      far.push(d);
      if (nodes[i].k === "project") suns = Math.max(suns, d);
    }
    far.sort((a, b) => a - b);
    return { centre: /** @type {[number, number, number]} */ ([cx, cy, cz]),
             radius: Math.max(far[Math.floor((far.length - 1) * 0.98)], suns, 40) };
  }
  /** @param {number} i @returns {[number, number, number]} where a node comes to rest, its galaxy turned as it is now */
  const placeOf = (i) => shaper.turned(goal, i);
  /**
   * Where the camera looks to see a memory in its place, and from how far: the
   * memory, what it is joined to within reach, and the heart of its galaxy all
   * in the frame, the memory a little off the middle toward its own side.
   * A galaxy seen nearly edge on is turned to, as far as shows its face.
   * @param {number} i @returns {{target: [number, number, number], dist: number, turn: {yaw?: number, pitch?: number}}}
   */
  function placeToSee(i) {
    // What was let go is seen where it is now, with the hole it circles, not with the galaxy it left.
    const hole = shaper.fallen[i] ? shaper.hole : null;
    const at = placeOf(i), home = hole ? null : nodes[i].p, sun = home === null ? undefined : graph.projectNode.get(home);
    const galaxy = home === null ? undefined : shaper.galaxies.get(home);
    const away = (/** @type {number} */ o) => { const there = placeOf(o); return Math.hypot(there[0] - at[0], there[1] - at[1], there[2] - at[2]); };
    // What it is joined to, the nearer four fifths: a line to another galaxy does not set the frame.
    const near = graph.neighbours(i).filter((link) => nodeHl[link.o] >= 0 && nodes[link.o].k !== "project").map((link) => away(link.o))
      .sort((a, b) => a - b);
    let need = near.length ? near[Math.floor((near.length - 1) * 0.8)] * 1.25 : 0;
    /** @type {[number, number, number]} */ let target = at;
    if (sun !== undefined && sun !== i) {
      const far = away(sun), lean = LEAN_TO_SUN;
      target = [at[0] + (goal[sun * 3] - at[0]) * lean, at[1] + (goal[sun * 3 + 1] - at[1]) * lean, at[2] + (goal[sun * 3 + 2] - at[2]) * lean];
      need = Math.max(need, Math.min(far, galaxy ? galaxy.reach : far) * (1 - lean) * 1.12);
    }
    // Never so close that its galaxy is a fog around it: the whole galaxy stays in the frame, and the memory is
    // found in it by its ring, its name and its lines.
    need = Math.max(need, galaxy ? galaxy.reach * 1.1 : 0, 45);
    if (hole) { target = hole.centre; need = hole.shadow * 5.5; }
    /** @type {{yaw?: number, pitch?: number}} */ const turn = {};
    if (galaxy && !camera.flat) {
      const back = camera.back, side = galaxy.n[0] * back[0] + galaxy.n[1] * back[1] + galaxy.n[2] * back[2];
      if (Math.abs(side) < FACE_SHOWN) {
        // Lean the way the camera looks from toward the pole of the disc on its own side, until the face shows.
        const sign = side < 0 ? -1 : 1, lean = FACE_TURNED_TO;
        const b = [0, 1, 2].map((a) => back[a] * (1 - lean) + sign * galaxy.n[a] * lean), length = Math.hypot(b[0], b[1], b[2]) || 1;
        turn.yaw = Math.atan2(b[0], b[2]);
        turn.pitch = clamp(Math.asin(clamp(b[1] / length, -1, 1)), -1.2, 1.2);
      }
    }
    return { target, dist: Math.min(camera.distanceToFit(need), camera.fitDist / 2.2), turn };
  }
  /**
   * Turns the camera to what the reader is on: a memory, the line between two,
   * one project alone, or all of it. It never backs away from a memory the
   * reader is already closer to.
   * @param {boolean} smooth a flight, or a jump
   * @param {boolean} [afresh] the memory in focus has moved: it is seen from as far as shows it in its new place
   */
  function aim(smooth, afresh = false) {
    const whole = ball();
    radius = whole.radius;
    globe = whole;
    camera.fitDist = camera.distanceToFit(whole.radius * 1.06);
    let target = whole.centre, dist = camera.fitDist;
    /** @type {{yaw?: number, pitch?: number}} */ let turn = {};
    if (focus >= 0 && nodes[focus].k === "project") {
      const own = ball((i) => nodes[i].p === nodes[focus].p && nodeHl[i] >= 0);
      target = own.centre;
      dist = Math.min(camera.dist, camera.distanceToFit(own.radius * 1.15), camera.fitDist);
    } else if (focus >= 0) {
      const see = placeToSee(focus);
      target = see.target;
      dist = afresh ? see.dist : Math.min(camera.dist, see.dist);
      turn = see.turn;
    } else if (line >= 0) {
      const a = placeOf(edges[line].s), b = placeOf(edges[line].t);
      target = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2];
      dist = clamp(camera.distanceToFit(Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]) * 0.75), camera.fitDist / 9, camera.fitDist);
    } else if (isolate !== null) {
      const alone = ball((i) => nodes[i].p === isolate && nodeHl[i] >= 0);
      target = alone.centre;
      dist = Math.min(camera.distanceToFit(alone.radius * 1.15), camera.fitDist);
    }
    // While the overture plays the camera is on its way in: where it is going changes, the flight goes on.
    if (overture && !smooth && camera.bend(target, dist)) return;
    if (smooth) camera.flyTo(target, dist, performance.now(), turn);
    else {
      camera.jumpTo(target, dist);
      if (turn.yaw !== undefined && turn.pitch !== undefined) { camera.yaw = turn.yaw; camera.pitch = turn.pitch; camera.update(); }
    }
  }

  // ---------- the hover card ----------
  const tip = $("tip");
  /** @param {number} mx @param {number} my */
  function placeTip(mx, my) {
    tip.hidden = false;
    tip.style.left = `${mx + 340 > W ? mx - 360 : mx}px`;
    tip.style.top = `${my + 120 > H ? my - 110 : my}px`;
  }
  /** @param {number} i @param {number} mx @param {number} my */
  function tipNode(i, mx, my) {
    const node = nodes[i];
    tip.replaceChildren();
    const meta = el("div", "meta");
    meta.append(kindChip(KIND_LABEL[node.k] + (node.c && node.k === "event" ? ` · ${node.c}` : ""), graph.colorOf(node.p)));
    if (node.p !== null && node.k !== "project") meta.append(el("span", "", graph.projects[node.p]));
    if (node.t) meta.append(el("span", "", dayFormat.format(new Date(node.t * 1000))));
    if (graph.pinned[i]) meta.append(el("span", "badge pin", "pinned"));
    if (graph.stale[i]) meta.append(el("span", "badge stale", "stale"));
    tip.append(meta, el("div", "text", graph.title(i)));
    placeTip(mx, my);
  }
  /** @param {number} index @param {number} mx @param {number} my */
  function tipLine(index, mx, my) {
    const e = edges[index];
    tip.replaceChildren();
    const meta = el("div", "meta");
    meta.append(kindChip(`Line · ${LINE_HINT[e.k]}${e.k === EDGE.similar ? ` · ${percent(e.w)}` : ""}`,
                         e.k === EDGE.similar ? SIMILAR_TINT : graph.colorOf(nodes[e.s].p)));
    const between = el("div", "between");
    between.append(cut(graph.title(e.s), 44), el("b", "", "↔"), cut(graph.title(e.t), 44));
    tip.append(meta, between, el("div", "meta", "Click to see why they are joined"));
    placeTip(mx, my);
  }

  // ---------- the panel ----------
  const panel = $("panel"), panelBody = $("panel-body"), trailBox = $("trail");
  /** What the last action did, and what the panel was on when it was done. @type {{of: string, text: string, error: boolean} | null} */
  let outcome = null;
  let acting = false;
  const dateAt = (/** @type {number} */ day) => new Date(momentAt([graph.span0, graph.span1], day) * 1000);
  const cards = new Cards(panelBody, graph, {
    go: (i) => focusOn(i),
    goLine: (index) => chooseLine(index),
    act: (action, target, origin) => { act(action, target, origin); },
    fetchItem: (id) => once(`/ui/api/item?id=${encodeURIComponent(id)}`),
    fetchLink: (a, b) => once(`/ui/api/atlas/link?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`),
    actions,
    similar: () => ({ ready: similarReady, note: similarNote }),
    acting: () => acting,
    outcome: () => (outcome && outcome.of === subject() ? outcome : null),
    dateAt,
  });

  function renderTrail() {
    trailBox.replaceChildren();
    trail.slice(-6).forEach((i, k) => {
      if (k) trailBox.append(el("span", "sep", "›"));
      const b = el("button", i === focus ? "here" : "", cut(graph.title(i), 22));
      b.setAttribute("type", "button");
      b.title = graph.title(i);
      b.addEventListener("click", () => focusOn(i, "return"));
      trailBox.append(b);
    });
  }
  /** @param {boolean} open */
  function setPanel(open) {
    panelOpen = open;
    panel.classList.toggle("open", open);
    panel.setAttribute("aria-hidden", String(!open));
    renderKnown();
  }
  /** Draws again what the panel shows, without the rise of a new card. */
  function redrawPanel() {
    panelBody.classList.add("still");
    if (focus >= 0) cards.node(focus);
    else if (line >= 0) cards.line(line);
    else if (rangeOpen) cards.days(from, play);
  }

  /** @param {number} i @param {Move} [move] */
  function focusOn(i, move = "step") {
    if (graph.gone[i]) return;  // hidden as a copy since the map loaded
    awaitedLine = null;
    outcome = null;
    panelBody.classList.remove("still");
    if (move !== "restore") {
      // A step shows what it steps to; an address is taken at its word.
      if (graph.birth[i] > play + 0.01) jumpTo(days);
      if (graph.birth[i] < from - 0.01 && !structural(i)) setFrom(0);
    }
    focus = i; line = -1; rangeOpen = false; isolate = null;
    if (!kindOn[graph.kind[i]]) { kindOn[graph.kind[i]] = true; renderFilters(); }
    updateHighlight();
    renderLegend();
    if (move === "step" && trail[trail.length - 1] !== i) { trail.push(i); if (trail.length > 24) trail.shift(); }
    renderTrail();
    cards.node(i);
    setPanel(true);
    aim(true);
    if (move !== "restore") remember(true);
  }
  /** @param {number} index of an edge @param {Move} [move] */
  function chooseLine(index, move = "step") {
    const e = edges[index];
    if (!e || graph.gone[e.s] || graph.gone[e.t]) return;
    awaitedLine = null;
    outcome = null;
    panelBody.classList.remove("still");
    focus = -1; line = index; rangeOpen = false; isolate = null;
    if (e.k === EDGE.similar && !meaningOn) { meaningOn = true; renderFilters(); }
    updateHighlight();
    renderLegend();
    renderTrail();
    cards.line(index);
    setPanel(true);
    aim(true);
    if (move !== "restore") remember(true);
  }
  /** Closes the panel and lets go of what it was on. @param {boolean} [byReader] false when the page itself is on its way elsewhere */
  function clearFocus(byReader = true) {
    const had = focus >= 0 || line >= 0 || rangeOpen;
    focus = -1; line = -1; rangeOpen = false;
    outcome = null;
    if (byReader) awaitedLine = null;
    updateHighlight();
    setPanel(false);
    if (had && byReader) remember(true);
  }
  /** Back to the whole memory, as the page opens. */
  function showAll() {
    clearFocus(false);
    awaitedLine = null;
    isolate = null;
    userMoved = false;
    updateHighlight();
    renderLegend();
    aim(true);
  }
  $("panel-close").addEventListener("click", () => clearFocus());

  // ---------- light actions ----------
  function requestId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");
  }
  /**
   * Take an action; the server writes it as the operator's event, then the map follows.
   * @param {string} action @param {string} target @param {string} [origin] the memory a copy repeats
   */
  async function act(action, target, origin) {
    if (acting || (focus < 0 && line < 0)) return;
    const taken = subject();
    acting = true;
    outcome = null;
    redrawPanel();
    let text = DONE[action] || "Done.", error = false;
    /** Whether the reader is still where the action was taken: its outcome is said there, or not at all. */
    let stayed = false;
    try {
      const response = await fetch("/ui/api/atlas/action", {
        method: "POST",
        headers: { "content-type": "application/json", "x-chronicle-token": actions.token || "" },
        body: JSON.stringify({ action, target, of: action === "duplicate" ? origin : undefined, request_id: requestId() }),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || `The server answered HTTP ${response.status}.`);
      if (body.status === "unchanged") text = "Already so: nothing changed.";
      stayed = subject() === taken;
      applyAction(target, body, stayed);
      if (action === "restore" && !graph.byMemoryId.has(target)) text += " Reload the map to see it.";
    } catch (failure) {
      text = failure instanceof Error ? failure.message : String(failure);
      error = true;
      stayed = subject() === taken;
    }
    acting = false;
    for (const id of [target, origin]) if (id) once.cache.delete(`/ui/api/item?id=${encodeURIComponent(id)}`);
    // applyAction may have moved the panel from a line to the memory that is left of it: the outcome goes with it.
    if (stayed) outcome = { of: subject(), text, error };
    redrawPanel();
  }
  /**
   * The map as the server says the memory now is (another tab may have changed it too).
   * @param {string} target @param {{marks?: {pinned?: object, stale?: object}, hidden?: boolean}} body
   * @param {boolean} stayed whether the reader is still where the action was taken
   */
  function applyAction(target, body, stayed) {
    const i = graph.byMemoryId.get(target);
    if (i === undefined) return;
    const marks = body.marks || {};
    graph.pinned[i] = marks.pinned ? 1 : 0;
    if (graph.pinned[i]) pins.add(i); else pins.delete(i);
    const wasStale = graph.stale[i];
    graph.stale[i] = marks.stale ? 1 : 0;
    if (graph.stale[i] !== wasStale) {
      graph.paint(i);
      renderer.setNodes(graph);
      // It leaves its galaxy for the hole, or comes back from it: the lights glide to where they now belong.
      shaper.restale(graph.stale);
      goal = shaper.shape(laid, dims);
      gliding = true;
      layGas();
      gas.updateBorn(play);
      updateHighlight();
      stale = true;
      // The reader goes with the memory they are on.
      if (focus === i) aim(true, true);
    }
    const hide = body.hidden ? 1 : 0;
    if (graph.gone[i] !== hide) counts[graph.kind[i]] += hide ? -1 : 1;
    graph.gone[i] = hide;
    updateHighlight();
    renderFilters();
    if (!graph.gone[i]) return;
    // The memory left the map. A line that ended in it gives way to the memory that is left; its own panel closes.
    if (focus === i) clearFocus();
    else if (line >= 0 && (edges[line].s === i || edges[line].t === i)) {
      const left = edges[line].s === i ? edges[line].t : edges[line].s;
      if (stayed) focusOn(left); else clearFocus();
    }
  }

  // ---------- filters, legend, the layout switch ----------
  const filters = $("filters");
  const counts = [0, 0, 0, 0, 0];
  for (let i = 0; i < n; i++) counts[graph.kind[i]]++;
  function renderFilters() {
    filters.replaceChildren();
    // Each switch wears the symbol its kind has on the map: the row is the chart's key.
    /** @type {[number, string][]} */ ([[2, "Events"], [3, "Notes"], [1, "Tasks"], [4, "Facts"]]).forEach(([k, label]) => {
      const chip = el("button", `chip${kindOn[k] ? "" : " off"}`);
      chip.setAttribute("type", "button");
      chip.setAttribute("aria-pressed", String(kindOn[k]));
      chip.append(el("span", `sym ${label.toLowerCase().slice(0, -1)}`), label, el("span", "n", number.format(counts[k])));
      chip.addEventListener("click", () => { kindOn[k] = !kindOn[k]; updateHighlight(); renderFilters(); });
      filters.append(chip);
    });
    const meaning = el("button", `chip${meaningOn ? "" : " off"}`);
    meaning.setAttribute("type", "button");
    meaning.setAttribute("aria-pressed", String(meaningOn));
    meaning.title = "Show lines between memories that say similar things (M)";
    meaning.append(el("span", "sym meaning"), "Meaning", el("span", "n", similarReady ? number.format(edges.length - graph.explicitCount) : similarNote));
    meaning.addEventListener("click", () => { meaningOn = !meaningOn; renderFilters(); });
    filters.append(meaning);
  }
  renderFilters();

  const legend = $("legend");
  /** @param {number | null} project */
  function setIsolate(project) {
    isolate = project;
    updateHighlight();
    renderLegend();
    aim(true);
  }
  function renderLegend() {
    const head = el("button", "legend-head");
    head.setAttribute("type", "button");
    head.setAttribute("aria-expanded", String(legendOpen));
    head.append(el("span", "", "Projects"), el("span", "caret", legendOpen ? "–" : "+"));
    head.addEventListener("click", () => { legendOpen = !legendOpen; storageSet(LEGEND_KEY, legendOpen); renderLegend(); });
    legend.replaceChildren(head);
    legend.classList.toggle("isolating", isolate !== null);
    legend.classList.toggle("collapsed", !legendOpen);
    if (!legendOpen) return;
    for (const p of graph.byProjectSize) {
      const b = el("button", isolate === p ? "on" : "");
      b.setAttribute("type", "button");
      const dot = el("span", "dot");
      dot.style.background = graph.colorOf(p);
      b.append(dot, el("span", "name", graph.projects[p]), el("span", "n", number.format(graph.projectSize.get(p) || 0)));
      b.addEventListener("click", () => { clearFocus(false); awaitedLine = null; setIsolate(isolate === p ? null : p); remember(true); });
      legend.append(b);
    }
  }
  renderLegend();
  $("stats").replaceChildren(el("b", "", number.format(n)), " memories · ", el("b", "", String(graph.projects.length)),
                             " projects · since ", dayFormat.format(new Date(graph.span0 * 1000)));

  /** @param {boolean} flat @param {boolean} [push] */
  function setFlat(flat, push = true) {
    if (flat === camera.flat) return;
    camera.setFlat(flat);
    $("mode-3d").classList.toggle("on", !flat);
    $("mode-2d").classList.toggle("on", flat);
    userMoved = false;
    layout(flat ? 2 : 3, false);
    if (push) remember(true);
  }
  $("mode-3d").addEventListener("click", () => setFlat(false));
  $("mode-2d").addEventListener("click", () => setFlat(true));

  // ---------- time ----------
  const moments = [];
  for (let i = 0; i < n; i++) if (nodes[i].k === "event" && nodes[i].t) moments.push({ t: /** @type {number} */ (nodes[i].t), p: nodes[i].p });
  const timeline = new Timeline(/** @type {HTMLCanvasElement} */ ($("river")), {
    span: [graph.span0, graph.span1], moments, color: (p) => graph.colorOf(p),
    onScrub: (day) => { setPlaying(false); jumpTo(day); flashDate(); remember(false); },
    onRange: (day) => { setFrom(day); remember(false); },
  });
  const dateBig = $("date-big"), timeLabel = $("time-label"), nowButton = $("now"), playButton = $("play"), rangeChip = $("range");
  const dateInput = /** @type {HTMLInputElement} */ ($("time-date"));
  const known = $("known"), knownList = $("known-list"), knownDate = $("known-date");
  let dateTimer = 0, knownTimer = 0;
  function bornCount() {
    let c = 0;
    for (let i = 0; i < n; i++) if (graph.birth[i] <= play && (graph.birth[i] >= from || structural(i))) c++;
    return c;
  }
  function flashDate() {
    dateBig.replaceChildren(dayFormat.format(dateAt(play)), el("small", "", `${number.format(bornCount())} memories`));
    dateBig.classList.add("on");
    clearTimeout(dateTimer);
    dateTimer = window.setTimeout(() => { if (!playing) dateBig.classList.remove("on"); }, 1400);
  }
  function renderKnown() {
    known.hidden = true;
    if (play >= days - 0.01 || panelOpen || overture) return;
    knownDate.textContent = dayFormat.format(dateAt(play));
    const now = momentAt([graph.span0, graph.span1], play);
    const current = new Map();
    for (const f of graph.facts) {
      if (f.from <= now && (f.to === null || f.to > now)) {
        const before = current.get(f.node);
        if (!before || before.from < f.from) current.set(f.node, f);
      }
    }
    const list = [...current.values()].sort((a, b) => b.from - a.from).slice(0, 14);
    knownList.replaceChildren(...list.map((f) => {
      const li = el("li", now - f.from < 3 * DAY ? "fresh" : "");
      li.append(el("span", "slot", nodes[f.node].l), el("span", "val", f.value));
      return li;
    }));
    known.hidden = !list.length;
  }
  /** What the bar above the river says: the day, the range, and the ways back. */
  function renderTime() {
    const travelling = play < days - 0.01, ranged = from > 0.01;
    nowButton.hidden = !travelling && !ranged;
    nowButton.textContent = ranged && !travelling ? "All of time" : "Back to now";
    timeLabel.replaceChildren(travelling ? dayFormat.format(dateAt(play)) : "Now", el("span", "", travelling
      ? ` · ${number.format(bornCount())} memories` : ` · ${dayFormat.format(new Date())}`));
    rangeChip.hidden = !travelling && !ranged;
    if (!rangeChip.hidden) {
      let written = 0;
      for (let i = 0; i < n; i++) {
        if ((nodes[i].k === "event" || nodes[i].k === "note") && graph.birth[i] >= from && graph.birth[i] <= play) written++;
      }
      rangeChip.textContent = ranged ? `${shortFormat.format(dateAt(from))} – ${shortFormat.format(dateAt(play))} · ${number.format(written)} written ▸`
        : `Up to ${shortFormat.format(dateAt(play))} · ${number.format(written)} written ▸`;
    }
  }
  /** @param {number} day */
  function jumpTo(day) {
    play = clamp(day, 0, days);
    if (from > play - 1) setFrom(Math.max(0, play - 1));
    timeline.setPlay(play);
    gas.updateBorn(play);
    renderTime();
    if (!knownTimer) knownTimer = window.setTimeout(() => { knownTimer = 0; renderKnown(); if (rangeOpen) redrawPanel(); }, 120);
  }
  /** Where the range starts: memories written before it go dark. @param {number} day */
  function setFrom(day) {
    from = clamp(day, 0, Math.max(play - 1, 0));
    timeline.setFrom(from);
    renderTime();
    if (rangeOpen && !knownTimer) knownTimer = window.setTimeout(() => { knownTimer = 0; redrawPanel(); }, 120);
  }
  function setPlaying(/** @type {boolean} */ on) {
    if (on === playing) return;
    playing = on;
    following = on && focus < 0 && line < 0;
    // From the end, play begins where the range does: a day in, so the playhead does not push its start back.
    if (on && play >= days - 0.01) jumpTo(from > 0.01 ? Math.min(from + 1, days) : 0);
    playButton.textContent = on ? "❚❚" : "▶";
    playButton.classList.toggle("on", on);
    if (on) dateBig.classList.add("on"); else flashDate();
  }
  playButton.addEventListener("click", () => setPlaying(!playing));
  nowButton.addEventListener("click", () => { setPlaying(false); setFrom(0); jumpTo(days); dateBig.classList.remove("on"); remember(false); });
  rangeChip.addEventListener("click", () => {
    focus = -1; line = -1; rangeOpen = true;
    awaitedLine = null;
    outcome = null;
    updateHighlight();
    panelBody.classList.remove("still");
    cards.days(from, play);
    setPanel(true);
    remember(true);
  });
  document.querySelectorAll(".speed button").forEach((b) => b.addEventListener("click", () => {
    speed = Number(/** @type {HTMLElement} */ (b).dataset.speed);
    document.querySelectorAll(".speed button").forEach((o) => o.classList.toggle("on", o === b));
  }));
  // The date is a button: it opens the browser's own calendar, bounded by the memory's span.
  dateInput.min = dayOf(graph.span0);
  dateInput.max = dayOf(graph.span1);
  timeLabel.addEventListener("click", () => {
    dateInput.value = dayOf(momentAt([graph.span0, graph.span1], play));
    try { dateInput.showPicker(); } catch { dateInput.focus(); }
  });
  dateInput.addEventListener("change", () => {
    const end = dayEnd(dateInput.value);
    if (end === null) return;
    setPlaying(false);
    // The end of the chosen day: everything written on it is in.
    jumpTo(clamp((end - graph.span0) / DAY, 0, days));
    flashDate();
    remember(false);
  });

  // ---------- search ----------
  const search = $("search"), input = /** @type {HTMLInputElement} */ ($("search-input")), results = $("search-results");
  const haystack = nodes.map((_, i) => graph.title(i).toLowerCase());
  const kindWeight = { project: 4, task: 3, fact: 2.5, note: 1.6, event: 1 };
  let found = /** @type {number[]} */ ([]), cursor = 0;
  function openSearch() { search.hidden = false; input.value = ""; runSearch(); input.focus(); }
  function closeSearch() { search.hidden = true; }
  function runSearch() {
    const words = input.value.toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) {
      found = [...graph.projectNode.values()].sort((a, b) => (nodes[b].u || 0) - (nodes[a].u || 0)).slice(0, 12);
    } else {
      const scored = [];
      for (let i = 0; i < n; i++) {
        const text = haystack[i];
        if (graph.gone[i] || !words.every((w) => text.includes(w))) continue;
        const weight = kindWeight[/** @type {keyof typeof kindWeight} */ (nodes[i].k)] - (graph.routine(i) ? 0.8 : 0);
        scored.push([i, weight + (text.startsWith(words[0]) ? 2 : 0) + (nodes[i].u || 0) / (graph.span1 || 1)]);
      }
      found = scored.sort((a, b) => b[1] - a[1]).slice(0, 30).map((s) => s[0]);
    }
    cursor = 0;
    results.replaceChildren(...found.map((i, k) => {
      const li = el("li", k === cursor ? "on" : "");
      const dot = el("span", "dot");
      dot.style.background = graph.colorOf(nodes[i].p);
      const label = el("span", "label");
      const text = graph.title(i);
      const at = words.length ? text.toLowerCase().indexOf(words[0]) : -1;
      if (at >= 0) label.append(text.slice(0, at), el("mark", "", text.slice(at, at + words[0].length)), text.slice(at + words[0].length));
      else label.textContent = text;
      const when = nodes[i].t ? ` · ${shortFormat.format(new Date(/** @type {number} */ (nodes[i].t) * 1000))}` : "";
      li.append(dot, label, el("span", "meta", `${KIND_LABEL[nodes[i].k]}${when}`));
      li.addEventListener("click", () => { closeSearch(); focusOn(i); });
      li.addEventListener("mousemove", () => { cursor = k; markCursor(); });
      return li;
    }));
  }
  function markCursor() {
    [...results.children].forEach((li, k) => li.classList.toggle("on", k === cursor));
    results.children[cursor]?.scrollIntoView({ block: "nearest" });
  }
  input.addEventListener("input", runSearch);
  input.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") { cursor = Math.min(cursor + 1, found.length - 1); markCursor(); event.preventDefault(); }
    else if (event.key === "ArrowUp") { cursor = Math.max(cursor - 1, 0); markCursor(); event.preventDefault(); }
    else if (event.key === "Enter" && found[cursor] !== undefined) { closeSearch(); focusOn(found[cursor]); }
    else if (event.key === "Escape") closeSearch();
  });
  search.addEventListener("click", (event) => { if (event.target === search) closeSearch(); });
  $("search-open").addEventListener("click", openSearch);

  // ---------- pointer ----------
  /** @param {number} i */
  const radiusOf = (i) => ink.radius(i, view);
  /** A node the pointer may take: whatever is shown. Beside a focus the rest is dimmer, and still a step away. @param {number} i */
  const takes = (i) => shown(i);
  /** How far a line is bowed: the brass ones leave the memory in focus all turning the same way. @param {number} index */
  const bowOf = (index) => (edgeHl[index] > 1.5 ? (edges[index].s === focus ? LINK_BOW : -LINK_BOW) : 0);
  /** A line the pointer may take: drawn brightly enough to be aimed at. @param {number} index */
  const takesLine = (index) => {
    const e = edges[index];
    if (edgeHl[index] < 0 || !shown(e.s) || !shown(e.t)) return false;
    if (focus >= 0 || line >= 0) return edgeHl[index] > 0;
    if (e.k === EDGE.similar) return meaningOn;
    // That a memory belongs to a task or a project is drawn only for what is in focus: nothing to point at.
    if (e.k === EDGE.task || e.k === EDGE.project) return false;
    // A line inside a galaxy is drawn once the reader is close enough to tell it from its neighbours.
    return Math.min(projected[e.s * 3 + 2], projected[e.t * 3 + 2]) / view.fit > LINES_FROM;
  };
  /** @type {{x: number, y: number, moved: boolean, slide: boolean, vx: number, vy: number, t: number} | null} */
  let drag = null;
  glCanvas.addEventListener("pointerdown", (event) => {
    drag = { x: event.clientX, y: event.clientY, moved: false, slide: event.shiftKey || event.button !== 0, vx: 0, vy: 0, t: performance.now() };
    glCanvas.setPointerCapture(event.pointerId);
    camera.spin = { yaw: 0, pitch: 0 };
    lastInput = performance.now();
    tip.hidden = true;  // the card under the pointer has said its piece once the reader presses
  });
  glCanvas.addEventListener("pointermove", (event) => {
    lastInput = performance.now();
    if (drag) {
      const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
      if (!drag.moved && Math.hypot(dx, dy) > 3) { drag.moved = true; glCanvas.classList.add("dragging"); userMoved = true; following = false; tip.hidden = true; }
      if (drag.moved) {
        if (drag.slide) camera.slide(dx, dy); else camera.drag(dx, dy);
        const now = performance.now(), dt = Math.max(now - drag.t, 1);
        drag.vx = (dx / dt) * 16; drag.vy = (dy / dt) * 16; drag.t = now;
      }
      drag.x = event.clientX; drag.y = event.clientY;
      return;
    }
    hovered = pickNode(event.clientX, event.clientY, projected, radiusOf, takes);
    hoveredLine = hovered >= 0 ? -1 : pickEdge(event.clientX, event.clientY, edges, projected, takesLine, bowOf);
    glCanvas.classList.toggle("pointing", hovered >= 0 || hoveredLine >= 0);
    if (hovered >= 0) tipNode(hovered, event.clientX, event.clientY);
    else if (hoveredLine >= 0) tipLine(hoveredLine, event.clientX, event.clientY);
    else tip.hidden = true;
  });
  glCanvas.addEventListener("pointerup", (event) => {
    if (drag && !drag.moved) {
      const i = pickNode(event.clientX, event.clientY, projected, radiusOf, takes);
      const index = i >= 0 ? -1 : pickEdge(event.clientX, event.clientY, edges, projected, takesLine, bowOf);
      if (i >= 0) focusOn(i);
      else if (index >= 0) chooseLine(index);
      else if (focus >= 0 || line >= 0 || rangeOpen) clearFocus();
    } else if (drag && !drag.slide && !camera.flat && performance.now() - drag.t < 80) {
      // Let go while moving: the sky keeps turning and slows down.
      camera.spin = { yaw: -drag.vx * 0.0055, pitch: drag.vy * 0.0055 };
    }
    drag = null;
    glCanvas.classList.remove("dragging");
  });
  glCanvas.addEventListener("pointerleave", () => { hovered = -1; hoveredLine = -1; tip.hidden = true; });
  glCanvas.addEventListener("contextmenu", (event) => event.preventDefault());
  glCanvas.addEventListener("dblclick", () => { showAll(); remember(true); });
  glCanvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    lastInput = performance.now();
    userMoved = true; following = false;
    camera.dolly(Math.exp(event.deltaY * (event.ctrlKey ? 0.01 : 0.0016)), event.clientX, event.clientY);
  }, { passive: false });

  // ---------- how to move around ----------
  const hint = $("hint");
  /** @param {boolean} open */
  function setHint(open) {
    hint.hidden = !open;
    if (!open) storageSet(HINT_KEY, true);
  }

  // ---------- the sky alone ----------
  const whisper = $("whisper");
  let whisperTimer = 0;
  /** Hides everything but the sky and what the reader opens, or brings it back. @param {boolean} on */
  function setImmersed(on) {
    immersed = on;
    document.body.classList.toggle("immersed", on);
    $("immerse").setAttribute("aria-pressed", String(on));
    whisper.classList.toggle("on", on);
    clearTimeout(whisperTimer);
    if (on) whisperTimer = window.setTimeout(() => whisper.classList.remove("on"), 3200);
    if (!userMoved && !camera.flight) aim(true);
  }
  $("immerse").addEventListener("click", () => setImmersed(!immersed));
  // Reading the panel is being here: the sky does not start to drift under a reader who is scrolling.
  for (const type of ["pointermove", "wheel", "keydown"]) panel.addEventListener(type, () => { lastInput = performance.now(); }, { passive: true });

  // ---------- the overture ----------
  /** The days on which the memories were written, in order: the overture fills the sky at an even pace, not the calendar's. */
  const written = Float32Array.from(graph.birth).sort();
  function endOverture() {
    if (!overture) return;
    overture = 0;
    jumpTo(days);
    dateBig.classList.remove("on");
    renderKnown();
  }
  for (const type of ["pointerdown", "wheel", "keydown"]) window.addEventListener(type, endOverture, { capture: true, passive: true });

  // ---------- keys ----------
  window.addEventListener("keydown", (event) => {
    const typing = event.target instanceof HTMLInputElement;
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") { event.preventDefault(); search.hidden ? openSearch() : closeSearch(); return; }
    if (typing || event.metaKey || event.ctrlKey || event.altKey) return;
    lastInput = performance.now();
    const turn = (/** @type {number} */ dx, /** @type {number} */ dy) => { if (!camera.flat) { camera.drag(dx, dy); userMoved = true; } };
    if (event.key === "/") { event.preventDefault(); openSearch(); }
    else if (event.key === "?") setHint(hint.hidden);
    else if (event.key === "i") setImmersed(!immersed);
    else if (event.key === "Escape") {
      if (!search.hidden) closeSearch();
      else if (!hint.hidden) setHint(false);
      else if (focus >= 0 || line >= 0 || rangeOpen) clearFocus();
      else if (immersed) setImmersed(false);
      else if (isolate !== null) { setIsolate(null); remember(true); }
    }
    else if (event.key === " ") { event.preventDefault(); setPlaying(!playing); }
    else if (event.key === "f") { showAll(); remember(true); }
    else if (event.key === "m") { meaningOn = !meaningOn; renderFilters(); }
    else if (event.key === "2") setFlat(true);
    else if (event.key === "3") setFlat(false);
    else if (event.key === "a") turn(40, 0);
    else if (event.key === "d") turn(-40, 0);
    else if (event.key === "w") turn(0, 40);
    else if (event.key === "s") turn(0, -40);
    else if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      setPlaying(false);
      jumpTo(play + (event.key === "ArrowLeft" ? -1 : 1) * (event.shiftKey ? 7 : 1));
      flashDate();
      remember(false);
    }
  });

  // ---------- the address ----------
  let addressTimer = 0;
  /** The address a slide of the time is waiting to write: the place as it was when the slide ended. @type {string | null} */
  let pendingAddress = null;
  function placeNow() {
    /** @type {import("./urlstate.js").Place} */ const place = {};
    if (line >= 0) {
      place.line = [nodes[edges[line].s].id, nodes[edges[line].t].id];
      place.kind = EDGE_NAME[edges[line].k];
    } else if (focus >= 0) place.node = nodes[focus].id;
    if (play < days - 0.01) place.day = dayOf(momentAt([graph.span0, graph.span1], play));
    if (from > 0.01) place.from = dayOf(graph.span0 + from * DAY);
    if (isolate !== null) place.project = graph.projects[isolate];
    if (camera.flat) place.flat = true;
    return place;
  }
  const addressNow = () => { const hash = writePlace(placeNow()); return hash ? `#${hash}` : location.pathname + location.search; };
  /** @param {string} address */
  const isCurrent = (address) => (address.startsWith("#") ? address === location.hash : !location.hash);
  /** Writes what a slide left waiting, into the history entry the reader was on when it slid. */
  function settleAddress() {
    clearTimeout(addressTimer);
    if (pendingAddress !== null && !isCurrent(pendingAddress)) history.replaceState(null, "", pendingAddress);
    pendingAddress = null;
  }
  /**
   * Writes where the reader is into the address.
   * @param {boolean} step a step worth a place in the history, or a slide of the time that only amends the place
   */
  function remember(step) {
    if (step) {
      settleAddress();  // the place being left keeps the time it was left at
      const address = addressNow();
      if (!isCurrent(address)) history.pushState(null, "", address);
    } else {
      pendingAddress = addressNow();
      clearTimeout(addressTimer);
      addressTimer = window.setTimeout(settleAddress, 300);
    }
  }
  /**
   * The edge an address names: its ends in their order and its kind, then the
   * same pair the other way round; -1 when it is not on the map.
   * @param {import("./urlstate.js").Place} place
   */
  function lineOf(place) {
    const [first, second] = place.line || ["", ""];
    const a = graph.byId.get(first), b = graph.byId.get(second), kind = EDGE_NAME.indexOf(place.kind || "");
    if (a === undefined || b === undefined) return -1;
    const fits = (/** @type {import("./model.js").Edge} */ e) => kind < 0 || e.k === kind;
    const forward = edges.findIndex((e) => e.s === a && e.t === b && fits(e));
    return forward >= 0 ? forward : edges.findIndex((e) => e.s === b && e.t === a && fits(e));
  }
  /** Goes where an address says. @param {import("./urlstate.js").Place} place */
  function goTo(place) {
    setFlat(Boolean(place.flat), false);
    const end = place.day ? dayEnd(place.day) : null, start = place.from ? dayStart(place.from) : null;
    jumpTo(end === null ? days : clamp((end - graph.span0) / DAY, 0, days));
    setFrom(start === null ? 0 : (start - graph.span0) / DAY);
    if (from > 0.01 || play < days - 0.01) timeline.show(from, play);
    const project = place.project ? graph.projects.indexOf(place.project) : -1;
    awaitedLine = null;
    if (place.line) {
      const index = lineOf(place);
      if (index >= 0) chooseLine(index, "restore");
      else {
        clearFocus(false);
        // A line by meaning is drawn once those lines arrive.
        if (!similarReady && (!place.kind || place.kind === EDGE_NAME[EDGE.similar])) awaitedLine = place;
      }
    } else if (place.node && graph.byId.has(place.node)) {
      focusOn(/** @type {number} */ (graph.byId.get(place.node)), "restore");
    } else {
      clearFocus(false);
      if (project >= 0 && isolate !== project) setIsolate(project);
      else if (project < 0 && isolate !== null) setIsolate(null);
    }
  }
  window.addEventListener("popstate", () => {
    clearTimeout(addressTimer);
    pendingAddress = null;  // the reader left that entry: nothing is written into the one arrived at
    goTo(readPlace(location.hash));
  });

  // ---------- size ----------
  function resize() {
    W = window.innerWidth; H = window.innerHeight; dpr = Math.min(window.devicePixelRatio || 1, 2);
    glCanvas.width = Math.round(W * dpr); glCanvas.height = Math.round(H * dpr);
    sky.resize(W, H, dpr);
    ink.resize(W, H, dpr);
    timeline.resize();
    camera.setFrame(frameOf());
    if (!userMoved && !camera.flight) aim(false);
  }
  window.addEventListener("resize", resize);

  // ---------- start ----------
  const arrival = readPlace(location.hash);
  camera.flat = Boolean(arrival.flat);
  // The sky opens a little turned and tipped, so the globe reads as a globe and not as a target.
  if (!camera.flat) { camera.yaw = OPENING_TURN[0]; camera.pitch = OPENING_TURN[1]; }
  $("mode-3d").classList.toggle("on", !camera.flat);
  $("mode-2d").classList.toggle("on", camera.flat);
  layout(camera.flat ? 2 : 3, true);
  resize();
  jumpTo(days);
  if (Object.keys(arrival).length) goTo(arrival);
  else if (!calm && !camera.flat) {
    overture = performance.now();
    const fit = camera.dist, to = [...camera.target];
    camera.dist = fit * OVERTURE_FROM.back; camera.yaw += OVERTURE_FROM.yaw; camera.pitch += OVERTURE_FROM.pitch;
    camera.update();
    camera.flyTo(/** @type {[number, number, number]} */ (to), fit, overture, { yaw: OPENING_TURN[0], pitch: OPENING_TURN[1], ms: OVERTURE_MS });
    jumpTo(0);
  }
  for (const [keys, what] of HOW) {
    const way = el("span", "how");
    way.append(...keys.map((key) => el("kbd", "", key)), what);
    $("hint-text").append(way);
  }
  $("hint-close").addEventListener("click", () => setHint(false));
  $("help").addEventListener("click", () => setHint(hint.hidden));
  hint.hidden = Boolean(storageGet(HINT_KEY));  // shown until the reader has closed it once

  // ---------- edges by meaning ----------
  (async () => {
    for (;;) {
      try {
        const body = await (await fetch("/ui/api/atlas/similar")).json();
        // Stale edges come from the pass before the newest memories: shown at once,
        // while the server brings them up to date for the next visit.
        if (body.status === "ready" || ((body.status === "stale" || body.status === "failed") && body.edges.length)) {
          graph.addSimilar(body.edges);
          similarReady = true;
          // What a memory is joined to has changed, and with it which are the bright stars and which have a twin.
          renderer.setNodes(graph);
          uploadEdges();
          layGas();
          gas.updateBorn(play);
          stale = true;
          updateHighlight();
          renderFilters();
          // The line an address asked for, if the reader has not gone elsewhere since.
          const awaited = awaitedLine ? lineOf(awaitedLine) : -1;
          awaitedLine = null;
          if (awaited >= 0) chooseLine(awaited, "restore");
          else if (focus >= 0 || line >= 0) redrawPanel();
          return;
        }
        if (body.status === "none" || body.status === "failed") {
          similarNote = body.status === "none" ? "no vectors" : "unavailable";
          renderFilters();
          if (focus >= 0) redrawPanel();
          return;
        }
      } catch { /* the server restarts or is busy: try again */ }
      await sleep(3000);
    }
  })();

  // ---------- the loop ----------
  /** @type {import("./ink.js").View} */
  const view = { projected, fit: 1, lights: 1, zoom: 1, focus, hovered, hoveredEdge: hoveredLine, chosenEdge: line, isolate, focusMix, trail, pins,
                 rings: sky.rings, inked, span: [0, W], shown, lit, hole: null };
  let then = performance.now(), turning = 0, spinning = 0, holeMix = 0;
  /** @param {number} now */
  function frame(now) {
    const dt = Math.min((now - then) / 1000, 0.05);
    then = now;
    const time = now / 1000;
    if (overture) {
      const t = clamp((now - overture) / OVERTURE_MS, 0, 1), eased = t * t * (3 - 2 * t);
      if (t >= 1) endOverture();
      else {
        jumpTo(Math.min(written[Math.floor(eased * (n - 1))] + 0.5, days));
        dateBig.replaceChildren(dayFormat.format(dateAt(play)), el("small", "", `${number.format(bornCount())} memories`));
        dateBig.classList.add("on");
      }
    }
    if (playing) {
      jumpTo(play + dt * PLAY_DAYS_PER_S * speed);
      dateBig.replaceChildren(dayFormat.format(dateAt(play)), el("small", "", `${number.format(bornCount())} memories`));
      if (play >= days) { setPlaying(false); dateBig.classList.remove("on"); }
      if (following && now - followAt > FOLLOW_MS && !camera.flight) {
        followAt = now;
        const grown = ball(shown);
        camera.flyTo(grown.centre, clamp(camera.distanceToFit(grown.radius * 1.2), camera.fitDist / 2.5, camera.fitDist), now, { ms: FOLLOW_MS * 1.5 });
      }
    }
    const k = 1 - Math.exp(-dt * 7);
    travel += ((play < days - 0.01 || from > 0.01 ? 1 : 0) - travel) * k;
    focusMix += ((focus >= 0 || line >= 0 || isolate !== null ? 1 : 0) - focusMix) * k;
    similarMix += ((meaningOn && similarReady ? 1 : 0) - similarMix) * k;

    // The lights glide to where the layout puts them: the map forms, folds flat and unfolds without a jump.
    if (gliding) {
      const g = 1 - Math.exp(-dt * 9);
      let apart = 0;
      for (let a = 0; a < positions.length; a++) {
        const d = goal[a] - positions[a];
        positions[a] += d * g;
        if (d > apart) apart = d; else if (-d > apart) apart = -d;
      }
      if (apart < 0.05) { positions.set(goal); gliding = false; }
      stale = true;
    }
    // The galaxies turn while the reader looks around, and hold still while the reader holds something: a memory
    // open in the panel, a line, or whatever is under the pointer.
    const held = calm || focus >= 0 || line >= 0 || hovered >= 0 || hoveredLine >= 0;
    spinning += ((held ? 0 : 1) - spinning) * (1 - Math.exp(-dt * 4));
    if (spinning > 0.002) { shaper.spin(dt * spinning); stale = true; }
    if (stale) {
      shaper.turn(positions, drawn);
      renderer.setPositions(drawn);
      stale = false;
    }

    // The frame slides with the panels instead of jumping when one opens.
    const want = frameOf(), have = camera.frame, f = 1 - Math.exp(-dt * 10);
    camera.setFrame({ cx: have.cx + (want.cx - have.cx) * f, cy: have.cy + (want.cy - have.cy) * f, w: want.w, h: want.h, height: want.height });
    const moved = camera.step(now, dt);
    const idle = !calm && !camera.flat && !drag && !moved && !playing && !overture && hovered < 0 && hoveredLine < 0
      && search.hidden && now - lastInput > IDLE_TURN_AFTER_MS;
    // Left alone the sky drifts; with a memory open it is a slow walk around it.
    if (idle) camera.yaw += dt * (focus >= 0 || line >= 0 ? IDLE_TURN_HELD : IDLE_TURN);
    // The globe shows while the reader turns the sky, and fades when it rests.
    const turned = Boolean(drag && drag.moved && !drag.slide) || Math.abs(camera.spin.yaw) + Math.abs(camera.spin.pitch) > 0.0004;
    turning += ((turned && !camera.flat ? 1 : 0) - turning) * (1 - Math.exp(-dt * (turned ? 9 : 1.6)));
    camera.update();
    camera.projectAll(drawn, projected);

    view.fit = camera.focal / camera.fitDist; view.zoom = camera.zoom;
    view.lights = clamp(Math.min(camera.frame.w, camera.frame.h) / FULL_LIGHTS_FRAME, 0.5, 1);
    view.focus = focus; view.hovered = hovered; view.hoveredEdge = hoveredLine; view.chosenEdge = line;
    view.isolate = isolate; view.focusMix = focusMix;
    view.span = [covered()[0], W - covered()[1]];
    // Beside a memory in focus the other galaxies step back and its own stays lit; beside a project shown alone
    // the others all but go.
    const narrowed = focus >= 0 || line >= 0 ? focusMix : 0, home = focus >= 0 ? nodes[focus].p : null;
    const whole = globe || { centre: /** @type {[number, number, number]} */ ([0, 0, 0]), radius };
    // The hole: how its disc lies to the reader, how near it is, and where its name goes.
    const pit = shaper.hole;
    holeMix += ((pit ? 1 : 0) - holeMix) * (1 - Math.exp(-dt * 1.2));
    /** @type {[number, number, number]} */ let holeLies = [1, 0, 1];
    let holeNear = -1;
    view.hole = null;
    if (pit) {
      const b = camera.back, side = pit.n[0] * b[0] + pit.n[1] * b[1] + pit.n[2] * b[2], way = side < 0 ? -1 : 1;
      const x = way * (pit.n[0] * camera.right[0] + pit.n[1] * camera.right[1] + pit.n[2] * camera.right[2]);
      const y = way * (pit.n[0] * camera.up[0] + pit.n[1] * camera.up[1] + pit.n[2] * camera.up[2]), length = Math.hypot(x, y);
      // The pole of the disc, as it lies on the screen, points to the far half of the disc; the long axis is across it.
      holeLies = length > 1e-4 ? [y / length, -x / length, Math.abs(side)] : [1, 0, 1];
      const on = camera.project(pit.centre[0], pit.centre[1], pit.centre[2]);
      if (on) {
        holeNear = on[2];
        view.hole = { x: on[0], y: on[1], r: pit.shadow * on[2], count: pit.members.length, shows: holeMix * (1 - 0.6 * narrowed) };
      }
    }
    renderer.setGasAt(gas.place(drawn, {
      hole: holeMix,
      time, play, flat: camera.flat, centre: whole.centre, radius: whole.radius, beside: 1 - 0.3 * narrowed,
      shows: (project) => (isolate === null || isolate === project ? 1 : 1 - 0.8 * focusMix) * (1 - (project === home ? 0.3 : 0.66) * narrowed),
    }));
    sky.draw(time, camera, { focus: narrowed, turning, globe, positions: drawn, shown });
    // The galaxies from the furthest to the nearest, by how near their suns are; what belongs to none goes first.
    order.length = 0;
    for (let k = 0; k < groups.length; k++) order.push(k);
    const nearness = (/** @type {number} */ k) => (groups[k].sun === HOLE ? holeNear : groups[k].sun < 0 ? -1 : projected[groups[k].sun * 3 + 2]);
    order.sort((a, b) => nearness(a) - nearness(b));
    renderer.draw({ res: [W, H], camera, radius, dpr, time, play, from, travel, focus: focusMix, similar: similarMix, lights: view.lights,
                    dim: focus < 0 && line < 0 && isolate !== null ? DIM_BESIDE_PROJECT : DIM_BESIDE_FOCUS, order, field: gas.field,
                    hole: holeLies });
    ink.draw(time, view);
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

main();
