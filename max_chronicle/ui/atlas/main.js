// @ts-check
/**
 * The Atlas: Chronicle's memory as a sky to wander and a river of time.
 *
 * Projects are suns, tasks ring-shaped planets around them, events stars,
 * notes pages and fact slots crystals. Lines are what the memory records;
 * dotted light between distant memories is resemblance by meaning. The river
 * at the bottom is how much each project recorded per day: drag the playhead
 * and the sky shows what was known then. In the panel, the operator pins a
 * memory to the brief, marks it stale, or hides a copy of it; each action is
 * an event in the memory, written by the server.
 */

import { Renderer } from "./render.js";
import { Timeline, momentAt } from "./timeline.js";
import { el, markdown, relations, kindChip, statRow } from "./panel.js";

const DAY = 86400;
const PALETTE = ["#7cc7ff", "#b18cff", "#ff7ad9", "#ffb86b", "#5ef0c8", "#ff8a7a", "#c3f56b", "#6b8cff",
                 "#ffd86b", "#5fd4ff", "#f59bff", "#8affa1", "#ff9f5a", "#9ad0ff", "#d4a5ff", "#7affd9"];
const UNSCOPED = "#9aa6c7";
const KINDS = ["project", "task", "event", "note", "fact"];
const KIND_LABEL = { project: "Project", task: "Task", event: "Event", note: "Note", fact: "Fact" };
const SHAPE = { event: 0, task: 1, fact: 2, note: 3, project: 4 };
const EDGE = { task: 0, project: 1, link: 2, fact: 3, similar: 4 };
const EDGE_ALPHA = [0.12, 0.06, 0.17, 0.3, 0.12];
const SIMILAR_TINT = "#cbbcff";
/** @type {Record<string, string>} */
const SIMILAR_WAIT = {
  "computing…": "Resemblance by meaning is still being computed.",
  "no vectors": "Resemblance by meaning needs vectors: none are stored yet.",
  unavailable: "Resemblance by meaning is unavailable: the server log says why.",
};
const BIG_CATEGORIES = new Set(["decision", "milestone"]);
const STALE_GREY = /** @type {[number, number, number]} */ ([0.42, 0.45, 0.53]);
const PIN_GOLD = "rgba(255, 214, 128, 0.8)";
/** @type {Record<string, string>} */
const DONE = {
  pin: "Pinned to the brief.", unpin: "Unpinned from the brief.", stale: "Marked stale.", fresh: "Marked current again.",
  duplicate: "The copy is hidden.", restore: "The copy is back.",
};
const PICK_PX = 12;
const FLY_MS = 1100;
const LAYOUT_KEY = "chronicle.atlas.layout.v1";
const COLD_TICKS = 340;
const WARM_TICKS = 120;
const PLAY_DAYS_PER_S = 2;
const FOLLOW_MS = 400;
const PANEL_W = 436;
const TITLE_CHARS = 96;
const LEGEND_W = 236;
// Fitting leaves room on the sides for the names drawn beside the outermost suns.
const LABEL_ROOM = 150;
const LEGEND_KEY = "chronicle.atlas.legend.v1";
const TIMELINE_H = 134;
const DUST = 900;
const ZOOM_MIN = 0.15;
const ZOOM_MAX = 40;
const number = new Intl.NumberFormat("en");
const dayFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", year: "numeric" });
const timeFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
const shortFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short" });

/** @param {string} id */
const $ = (id) => /** @type {HTMLElement} */ (document.getElementById(id));
/** @param {number} ms */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** @param {number} v @param {number} lo @param {number} hi */
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
/** @param {number} t */
const ease = (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2);

/** A small seeded generator, so the same memory always draws the same sky. @param {number} seed */
function mulberry32(seed) {
  return () => {
    seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** @param {string} hex @returns {[number, number, number]} */
function rgb(hex) {
  const v = parseInt(hex.slice(1), 16);
  return [((v >> 16) & 255) / 255, ((v >> 8) & 255) / 255, (v & 255) / 255];
}

/** @param {[number, number, number]} c @param {[number, number, number]} d @param {number} t */
const mix = (c, d, t) => /** @type {[number, number, number]} */ (c.map((v, k) => v + (d[k] - v) * t));

async function load() {
  for (let attempt = 0; ; attempt++) {
    const response = await fetch("/ui/api/atlas", { headers: { accept: "application/json" } });
    if (response.status === 503 && attempt < 120) { await sleep(1000); continue; }
    if (!response.ok) throw new Error(`The memory did not load (HTTP ${response.status}).`);
    return response.json();
  }
}

/** @type {Map<string, Promise<any>>} */
const items = new Map();
/** @param {string} id */
function fetchItem(id) {
  if (!items.has(id)) {
    items.set(id, fetch(`/ui/api/item?id=${encodeURIComponent(id)}`).then((r) => (r.ok ? r.json() : null))
      .catch(() => { items.delete(id); return null; }));  // the server restarts: asked again next time
  }
  return /** @type {Promise<any>} */ (items.get(id));
}

function storageGet(key) {
  try { return JSON.parse(localStorage.getItem(key) || "null"); } catch { return null; }
}
function storageSet(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); } catch { /* private window or full: the layout is recomputed next time */ }
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
  /** @type {{id: string, k: string, l: string, p: number | null, t: number | null, u: number | null, c?: string, a?: string, o?: number}[]} */
  const nodes = data.nodes;
  const n = nodes.length;
  /** @type {{enabled: boolean, token: string | null}} */
  const actions = data.actions || { enabled: false, token: null };
  // What the operator marked: pinned to the brief, stale, or hidden as a copy since the map loaded.
  const pinned = new Uint8Array(n), stale = new Uint8Array(n), gone = new Uint8Array(n);
  nodes.forEach((node, i) => { pinned[i] = node.pi ? 1 : 0; stale[i] = node.st ? 1 : 0; });
  const pins = new Set(/** @type {number[]} */ ([...pinned.keys()].filter((i) => pinned[i])));
  /** The id the server knows a memory by (event:<id>, note:<id>); null for other nodes. @param {number} i */
  const memoryId = (i) => (nodes[i].k === "event" ? `event:${nodes[i].id.slice(2)}`
    : nodes[i].k === "note" ? `note:${nodes[i].id.slice(2)}` : null);
  /** @type {Map<string, number>} */
  const byMemoryId = new Map();
  nodes.forEach((_, i) => { const id = memoryId(i); if (id) byMemoryId.set(id, i); });
  const projects = /** @type {string[]} */ (data.projects);
  const span0 = data.span ? data.span[0] : Date.now() / 1000;
  const span1 = data.span ? data.span[1] : span0 + DAY;
  const days = Math.max(1, Math.ceil((span1 - span0) / DAY));
  loadingText.textContent = `Mapping ${number.format(n)} memories…`;

  // ---------- the graph ----------
  const kind = new Int8Array(n);
  nodes.forEach((node, i) => { kind[i] = KINDS.indexOf(node.k); });
  const projectNode = new Map();
  nodes.forEach((node, i) => { if (node.k === "project") projectNode.set(node.p, i); });
  const projectSize = new Map();
  for (const node of nodes) if (node.p !== null) projectSize.set(node.p, (projectSize.get(node.p) || 0) + 1);
  const byProjectSize = [...projectSize.entries()].sort((a, b) => b[1] - a[1]).map(([p]) => p);
  const projectColor = new Map(byProjectSize.map((p, rank) => [p, PALETTE[rank % PALETTE.length]]));
  /** @param {number | null} p */
  const colorOf = (p) => (p === null ? UNSCOPED : projectColor.get(p) || UNSCOPED);

  /** @type {{s: number, t: number, k: number, w: number, dup: number}[]} */
  const edges = data.edges.map((/** @type {number[]} */ e) => ({ s: e[0], t: e[1], k: e[2], w: 1, dup: 0 }));
  const explicitCount = edges.length;
  /** @type {{o: number, e: number, k: number, out: boolean}[][]} */
  const adj = Array.from({ length: n }, () => []);
  const degree = new Float32Array(n);
  const addAdjacency = (/** @type {number} */ index) => {
    const e = edges[index];
    adj[e.s].push({ o: e.t, e: index, k: e.k, out: true });
    adj[e.t].push({ o: e.s, e: index, k: e.k, out: false });
    if (e.k !== EDGE.similar) { degree[e.s]++; degree[e.t]++; }
  };
  edges.forEach((_, index) => addAdjacency(index));

  const random = mulberry32(20261029);
  const birth = new Float32Array(n), last = new Float32Array(n), seed = new Float32Array(n), size = new Float32Array(n);
  const color = new Float32Array(n * 3), shape = new Float32Array(n);
  const white = /** @type {[number, number, number]} */ ([1, 1, 1]);
  /** A node's colour and size; a stale memory is greyer and smaller. @param {number} i */
  function paint(i) {
    const node = nodes[i];
    let c = rgb(colorOf(node.p));
    if (node.k === "note") c = mix(c, [0.78, 0.84, 1], 0.35);
    if (node.k === "fact") c = mix(c, [1, 0.93, 0.7], 0.6);
    if (node.k === "project") c = mix(c, white, 0.15);
    if (stale[i]) c = mix(c, STALE_GREY, 0.6);
    color.set(c, i * 3);
    const d = degree[i];
    size[i] = (node.k === "project" ? clamp(6 + Math.sqrt(d) * 0.32, 6, 15)
      : node.k === "task" ? clamp(2.3 + Math.sqrt(d) * 0.4, 2.3, 6)
      : node.k === "fact" ? 2.6
      : node.k === "note" ? 1.35
      : BIG_CATEGORIES.has(node.c || "") ? 1.9 : 1.5) * (stale[i] ? 0.8 : 1);
  }
  nodes.forEach((node, i) => {
    birth[i] = node.t === null ? 0 : (node.t - span0) / DAY;
    last[i] = node.u === null ? birth[i] : (node.u - span0) / DAY;
    seed[i] = random();
    shape[i] = SHAPE[/** @type {keyof typeof SHAPE} */ (node.k)];
    paint(i);
  });

  // ---------- rendering ----------
  const sky = /** @type {HTMLCanvasElement} */ ($("sky"));
  const glCanvas = /** @type {HTMLCanvasElement} */ ($("gl"));
  const ink = /** @type {HTMLCanvasElement} */ ($("ink"));
  const skyCtx = /** @type {CanvasRenderingContext2D} */ (sky.getContext("2d"));
  const inkCtx = /** @type {CanvasRenderingContext2D} */ (ink.getContext("2d"));
  let renderer;
  try {
    renderer = new Renderer(glCanvas);
  } catch (error) {
    loadingText.textContent = error instanceof Error ? error.message : String(error);
    return;
  }
  renderer.setNodes({ color, size, shape, birth, last, seed });

  const nodeHl = new Float32Array(n);
  /** @type {Float32Array} */ let edgeHl = new Float32Array(edges.length);
  const uploadEdges = () => {
    const m = edges.length;
    const ends = new Int32Array(m * 2), rgba = new Float32Array(m * 4), eb = new Float32Array(m), ek = new Float32Array(m);
    const tint = rgb(SIMILAR_TINT);
    edges.forEach((e, index) => {
      ends[index * 2] = e.s; ends[index * 2 + 1] = e.t;
      const c = e.k === EDGE.similar ? tint : /** @type {[number, number, number]} */ ([...color.subarray(e.s * 3, e.s * 3 + 3)]);
      const a = e.k === EDGE.similar ? EDGE_ALPHA[e.k] * clamp((e.w - 0.78) / 0.12, 0.35, 1.2) : EDGE_ALPHA[e.k];
      rgba.set([c[0], c[1], c[2], a], index * 4);
      eb[index] = Math.max(birth[e.s], birth[e.t]);
      ek[index] = e.k;
    });
    renderer.setEdges(ends, { color: rgba, birth: eb, kind: ek });
    edgeHl = new Float32Array(m);
  };
  uploadEdges();

  // ---------- positions and layout ----------
  let positions = new Float32Array(n * 2);
  const parent = new Int32Array(n).fill(-1);
  for (const e of edges) if (e.k !== EDGE.link && parent[e.s] < 0) parent[e.s] = e.t;
  const cached = storageGet(LAYOUT_KEY);
  const cachedPositions = cached && cached.positions ? cached.positions : {};
  let warmCount = 0;
  const spread = { project: 0, task: 90, event: 26, fact: 8, note: 150 };
  const order = [...nodes.keys()].sort((a, b) => kindOrder(a) - kindOrder(b));
  function kindOrder(/** @type {number} */ i) { return ["project", "task", "event", "fact", "note"].indexOf(nodes[i].k); }
  byProjectSize.forEach((p, rank) => {
    const i = projectNode.get(p);
    if (i === undefined) return;
    const radius = 300 * Math.sqrt(rank + 0.35), angle = rank * 2.39996;
    positions[i * 2] = Math.cos(angle) * radius;
    positions[i * 2 + 1] = Math.sin(angle) * radius;
  });
  for (const i of order) {
    const known = cachedPositions[nodes[i].id];
    if (known) { positions[i * 2] = known[0]; positions[i * 2 + 1] = known[1]; warmCount++; continue; }
    if (nodes[i].k === "project") continue;
    const anchor = parent[i];
    const r = spread[/** @type {keyof typeof spread} */ (nodes[i].k)] * (0.4 + random());
    const a = random() * Math.PI * 2;
    const ax = anchor >= 0 ? positions[anchor * 2] : (random() - 0.5) * 900;
    const ay = anchor >= 0 ? positions[anchor * 2 + 1] : (random() - 0.5) * 900;
    positions[i * 2] = ax + Math.cos(a) * r;
    positions[i * 2 + 1] = ay + Math.sin(a) * r;
  }
  const warm = warmCount > n * 0.8;
  renderer.setPositions(positions);

  let userMoved = false;
  const worker = new Worker("/ui/static/atlas/layout.worker.js");
  const explicitEnds = new Int32Array(explicitCount * 3);
  edges.slice(0, explicitCount).forEach((e, index) => { explicitEnds.set([e.s, e.t, e.k], index * 3); });
  worker.onmessage = (message) => {
    positions = message.data.positions;
    renderer.setPositions(positions);
    updateNebulae();
    if (!userMoved) fitScale = boundsScale(allBounds());
    if (!userMoved && !fly) fit(false);
    if (message.data.done) {
      worker.terminate();
      const saved = /** @type {Record<string, [number, number]>} */ ({});
      nodes.forEach((node, i) => { saved[node.id] = [Math.round(positions[i * 2] * 10) / 10, Math.round(positions[i * 2 + 1] * 10) / 10]; });
      storageSet(LAYOUT_KEY, { positions: saved });
      $("loading").classList.add("done");
    } else {
      loadingText.textContent = `Mapping ${number.format(n)} memories… ${Math.round((message.data.tick / (warm ? WARM_TICKS : COLD_TICKS)) * 100)}%`;
    }
  };
  worker.postMessage({ n, kinds: kind, edges: explicitEnds, init: positions.slice(), ticks: warm ? WARM_TICKS : COLD_TICKS, warm });

  // ---------- camera ----------
  let W = window.innerWidth, H = window.innerHeight, dpr = Math.min(window.devicePixelRatio || 1, 2);
  const cam = { s: 1, x: W / 2, y: H / 2 };
  const goal = { s: 1, x: W / 2, y: H / 2 };
  let fitScale = 1;
  /** @type {{from: {cx: number, cy: number, s: number}, to: {cx: number, cy: number, s: number}, start: number, dip: number} | null} */
  let fly = null;
  let panelOpen = false;
  let legendOpen = storageGet(LEGEND_KEY) !== false;
  const viewport = () => {
    const left = legendOpen && W > 900 ? LEGEND_W : 0, right = panelOpen ? PANEL_W : 0;
    return { cx: left + (W - left - right) / 2, cy: 56 + (H - 56 - TIMELINE_H) / 2,
             w: W - left - right - LABEL_ROOM, h: H - 56 - TIMELINE_H - 40 };
  };

  function allBounds() {
    const xs = [], ys = [];
    for (let i = 0; i < n; i++) if (nodeHl[i] >= 0) { xs.push(positions[i * 2]); ys.push(positions[i * 2 + 1]); }
    if (!xs.length) return { x0: -100, x1: 100, y0: -100, y1: 100 };
    xs.sort((a, b) => a - b); ys.sort((a, b) => a - b);
    const q = (/** @type {number[]} */ v, /** @type {number} */ f) => v[Math.floor((v.length - 1) * f)];
    // Stray memories may fall outside, but every sun stays in the frame.
    const b = { x0: q(xs, 0.01), x1: q(xs, 0.99), y0: q(ys, 0.01), y1: q(ys, 0.99) };
    for (const i of projectNode.values()) {
      if (nodeHl[i] < 0) continue;
      b.x0 = Math.min(b.x0, positions[i * 2]); b.x1 = Math.max(b.x1, positions[i * 2]);
      b.y0 = Math.min(b.y0, positions[i * 2 + 1]); b.y1 = Math.max(b.y1, positions[i * 2 + 1]);
    }
    return b;
  }
  /** @param {{x0: number, x1: number, y0: number, y1: number}} b */
  function boundsScale(b) {
    const v = viewport();
    return Math.min(v.w / Math.max(b.x1 - b.x0, 1), v.h / Math.max(b.y1 - b.y0, 1)) * 0.94;
  }
  /** Frames what is born so far, never closer than a couple of steps in. */
  function followGrowth() {
    const xs = [], ys = [];
    for (let i = 0; i < n; i++) if (shownNow(i)) { xs.push(positions[i * 2]); ys.push(positions[i * 2 + 1]); }
    if (xs.length < 3) return;
    xs.sort((a, b) => a - b); ys.sort((a, b) => a - b);
    const q = (/** @type {number[]} */ v, /** @type {number} */ f) => v[Math.floor((v.length - 1) * f)];
    const b = { x0: q(xs, 0.02) - 60, x1: q(xs, 0.98) + 60, y0: q(ys, 0.02) - 60, y1: q(ys, 0.98) + 60 };
    const s = Math.min(boundsScale(b), fitScale * 2.5), v = viewport();
    goal.s = s; goal.x = v.cx - ((b.x0 + b.x1) / 2) * s; goal.y = v.cy - ((b.y0 + b.y1) / 2) * s;
  }
  /** @param {boolean} smooth */
  function fit(smooth, bounds = allBounds()) {
    const s = boundsScale(bounds), v = viewport();
    const cx = (bounds.x0 + bounds.x1) / 2, cy = (bounds.y0 + bounds.y1) / 2;
    if (smooth) flyTo(cx, cy, s);
    else { cam.s = goal.s = s; cam.x = goal.x = v.cx - cx * s; cam.y = goal.y = v.cy - cy * s; }
  }
  /** @param {number} wx @param {number} wy @param {number} s */
  function flyTo(wx, wy, s) {
    const v = viewport();
    const from = { cx: (v.cx - cam.x) / cam.s, cy: (v.cy - cam.y) / cam.s, s: cam.s };
    const distance = Math.hypot(wx - from.cx, wy - from.cy) * Math.min(cam.s, s);
    fly = { from, to: { cx: wx, cy: wy, s: clamp(s, ZOOM_MIN * fitScale, ZOOM_MAX * fitScale) }, start: performance.now(),
            dip: clamp(Math.log1p(distance / Math.max(W, 1)) * 0.9, 0, 1.4) };
  }
  function stepFly(/** @type {number} */ now) {
    if (!fly) return;
    const t = clamp((now - fly.start) / FLY_MS, 0, 1), e = ease(t), v = viewport();
    const logS = Math.log(fly.from.s) + (Math.log(fly.to.s) - Math.log(fly.from.s)) * e - fly.dip * Math.sin(Math.PI * e);
    const s = Math.exp(logS);
    const cx = fly.from.cx + (fly.to.cx - fly.from.cx) * e, cy = fly.from.cy + (fly.to.cy - fly.from.cy) * e;
    cam.s = goal.s = s; cam.x = goal.x = v.cx - cx * s; cam.y = goal.y = v.cy - cy * s;
    if (t >= 1) fly = null;
  }

  // ---------- state ----------
  const kindOn = [true, true, true, true, true];
  let meaningOn = false;
  let similarReady = false;
  /** What the Meaning chip says until the edges by meaning arrive. */
  let similarNote = "computing…";
  let focus = -1;
  /** @type {number | null} */ let isolate = null;
  let hovered = -1;
  /** @type {number[]} */ const trail = [];
  let play = days, playing = false, speed = 1, following = false, followAt = 0;
  let travel = 0, focusMix = 0, similarMix = 0;

  function updateHighlight() {
    for (let i = 0; i < n; i++) nodeHl[i] = kindOn[kind[i]] && !gone[i] ? 0 : -1;
    edges.forEach((e, index) => { edgeHl[index] = nodeHl[e.s] < 0 || nodeHl[e.t] < 0 ? -1 : 0; });
    if (focus >= 0) {
      if (nodeHl[focus] < 0) nodeHl[focus] = 0;
      nodeHl[focus] = 2;
      for (const a of adj[focus]) {
        if (nodeHl[a.o] < 0) continue;
        nodeHl[a.o] = Math.max(nodeHl[a.o], 1);
        edgeHl[a.e] = 1;
      }
    } else if (isolate !== null) {
      for (let i = 0; i < n; i++) if (nodes[i].p === isolate && nodeHl[i] >= 0) nodeHl[i] = 1;
      edges.forEach((e, index) => { if (nodeHl[e.s] > 0 && nodeHl[e.t] > 0) edgeHl[index] = 1; });
    }
    renderer.setHighlight(nodeHl, edgeHl);
  }
  updateHighlight();

  /** @param {number} i */
  const shownNow = (i) => nodeHl[i] >= 0 && birth[i] <= play + 0.01;
  const zoomLevel = () => cam.s / fitScale;
  /** @param {number} i */
  const radiusPx = (i) => size[i] * clamp(Math.pow(zoomLevel(), 0.55), 0.45, 7);

  // ---------- nebulae and dust ----------
  /** @type {{x: number, y: number, r: number, color: string, p: number, total: number}[]} */
  let nebulae = [];
  function updateNebulae() {
    const sums = new Map();
    for (let i = 0; i < n; i++) {
      const p = nodes[i].p;
      if (p === null) continue;
      const s = sums.get(p) || { x: 0, y: 0, xx: 0, count: 0 };
      s.x += positions[i * 2]; s.y += positions[i * 2 + 1]; s.count++;
      sums.set(p, s);
    }
    for (let i = 0; i < n; i++) {
      const p = nodes[i].p;
      if (p === null) continue;
      const s = sums.get(p);
      const dx = positions[i * 2] - s.x / s.count, dy = positions[i * 2 + 1] - s.y / s.count;
      s.xx += dx * dx + dy * dy;
    }
    nebulae = [...sums.entries()].filter(([, s]) => s.count >= 8).map(([p, s]) => ({
      x: s.x / s.count, y: s.y / s.count, r: Math.sqrt(s.xx / s.count) * 1.35 + 30, color: colorOf(p), p, total: s.count,
    }));
  }
  const bornShare = new Map();
  function updateBornShare() {
    const counts = new Map();
    for (let i = 0; i < n; i++) if (nodes[i].p !== null && birth[i] <= play) counts.set(nodes[i].p, (counts.get(nodes[i].p) || 0) + 1);
    for (const nebula of nebulae) bornShare.set(nebula.p, (counts.get(nebula.p) || 0) / nebula.total);
  }
  const dust = Array.from({ length: DUST }, () => ({
    x: random(), y: random(), par: 0.04 + random() * 0.22, r: 0.35 + random() * random() * 1.3,
    a: 0.15 + random() * 0.55, f: 0.3 + random() * 1.2, ph: random() * 6.28,
  }));

  function drawSky(/** @type {number} */ time) {
    const ctx = skyCtx;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.globalCompositeOperation = "source-over";
    ctx.fillStyle = "#03040a";
    ctx.fillRect(0, 0, W, H);
    const glow = ctx.createRadialGradient(W * 0.5, H * 0.45, 0, W * 0.5, H * 0.45, Math.max(W, H) * 0.75);
    glow.addColorStop(0, "rgba(22, 28, 58, 0.55)");
    glow.addColorStop(1, "rgba(3, 4, 10, 0)");
    ctx.fillStyle = glow;
    ctx.fillRect(0, 0, W, H);
    ctx.globalCompositeOperation = "lighter";
    const spanW = W * 1.4, spanH = H * 1.4;
    for (const star of dust) {
      const x = (((star.x * spanW + cam.x * star.par) % spanW) + spanW) % spanW - W * 0.2;
      const y = (((star.y * spanH + cam.y * star.par) % spanH) + spanH) % spanH - H * 0.2;
      const a = star.a * (0.7 + 0.3 * Math.sin(time * star.f + star.ph));
      ctx.fillStyle = `rgba(200, 212, 255, ${a.toFixed(3)})`;
      ctx.fillRect(x, y, star.r, star.r);
    }
    const dim = 1 - 0.75 * focusMix;
    for (const nebula of nebulae) {
      const x = nebula.x * cam.s + cam.x, y = nebula.y * cam.s + cam.y, r = nebula.r * cam.s;
      if (x + r < 0 || x - r > W || y + r < 0 || y - r > H || r < 2) continue;
      const share = travel > 0.01 ? (bornShare.get(nebula.p) || 0) : 1;
      const lit = isolate === null || isolate === nebula.p ? 1 : 0.25;
      const alpha = 0.11 * dim * lit * Math.sqrt(share);
      if (alpha < 0.004) continue;
      const g = ctx.createRadialGradient(x, y, 0, x, y, r);
      const [cr, cg, cb] = rgb(nebula.color).map((v) => Math.round(v * 255));
      g.addColorStop(0, `rgba(${cr}, ${cg}, ${cb}, ${alpha.toFixed(3)})`);
      g.addColorStop(0.45, `rgba(${cr}, ${cg}, ${cb}, ${(alpha * 0.38).toFixed(3)})`);
      g.addColorStop(1, `rgba(${cr}, ${cg}, ${cb}, 0)`);
      ctx.fillStyle = g;
      ctx.fillRect(x - r, y - r, r * 2, r * 2);
    }
  }

  // ---------- labels ----------
  /** @param {string} text @param {number} max */
  const cut = (text, max) => (text.length > max ? text.slice(0, max - 1) + "…" : text);
  function drawInk(/** @type {number} */ time) {
    const ctx = inkCtx;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    const z = zoomLevel();
    /** @type {[number, number, number, number][]} */
    const boxes = [];
    const fits = (/** @type {number} */ x, /** @type {number} */ y, /** @type {number} */ w, /** @type {number} */ h) => {
      for (const b of boxes) if (x < b[0] + b[2] && x + w > b[0] && y < b[1] + b[3] && y + h > b[1]) return false;
      boxes.push([x, y, w, h]);
      return true;
    };
    const sx = (/** @type {number} */ i) => positions[i * 2] * cam.s + cam.x;
    const sy = (/** @type {number} */ i) => positions[i * 2 + 1] * cam.s + cam.y;
    const onScreen = (/** @type {number} */ i) => { const x = sx(i), y = sy(i); return x > -40 && x < W + 40 && y > -20 && y < H + 20; };

    // the path walked so far
    if (trail.length > 1) {
      ctx.save();
      ctx.setLineDash([2, 6]);
      ctx.lineDashOffset = -time * 14;
      ctx.strokeStyle = "rgba(220, 228, 255, 0.55)";
      ctx.lineWidth = 1.2;
      ctx.shadowColor = "rgba(160, 185, 255, 0.9)";
      ctx.shadowBlur = 8;
      ctx.beginPath();
      trail.forEach((i, k) => (k ? ctx.lineTo(sx(i), sy(i)) : ctx.moveTo(sx(i), sy(i))));
      ctx.stroke();
      ctx.restore();
    }

    // rings: hover and focus
    const ring = (/** @type {number} */ i, /** @type {number} */ alpha, /** @type {number} */ grow) => {
      const r = radiusPx(i) * 1.9 + 5 + grow;
      ctx.beginPath();
      ctx.arc(sx(i), sy(i), r, 0, Math.PI * 2);
      ctx.strokeStyle = `rgba(235, 240, 255, ${alpha})`;
      ctx.lineWidth = 1;
      ctx.stroke();
    };
    if (focus >= 0) ring(focus, 0.55 + 0.25 * Math.sin(time * 2.4), 2 + Math.sin(time * 2.4) * 1.5);
    if (hovered >= 0 && hovered !== focus) ring(hovered, 0.45, 0);
    // pinned to the brief: a thin gold halo
    ctx.strokeStyle = PIN_GOLD;
    ctx.lineWidth = 1.1;
    for (const i of pins) {
      if (!shownNow(i) || !onScreen(i) || (focus >= 0 && nodeHl[i] < 1 && focusMix > 0.5)) continue;
      ctx.beginPath();
      ctx.arc(sx(i), sy(i), radiusPx(i) * 1.6 + 3.5, 0, Math.PI * 2);
      ctx.stroke();
    }

    ctx.textBaseline = "middle";
    ctx.shadowColor = "rgba(0, 0, 0, 0.85)";
    ctx.shadowBlur = 6;
    /** @param {number} i @param {string} text @param {string} font @param {string} fill @param {boolean} [force] */
    const label = (i, text, font, fill, force = false) => {
      ctx.font = font;
      const w = ctx.measureText(text).width;
      const x = sx(i) + radiusPx(i) * 1.6 + 6, y = sy(i);
      if (!fits(x - 2, y - 8, w + 4, 16) && !force) return;
      ctx.fillStyle = fill;
      ctx.fillText(text, x, y);
    };

    if (focus >= 0) {
      label(focus, cut(nodes[focus].l, 64), "600 13px -apple-system, BlinkMacSystemFont, sans-serif", "#ffffff", true);
      let count = 0;
      for (const a of adj[focus]) {
        if (count > 40 || !shownNow(a.o) || !onScreen(a.o)) continue;
        label(a.o, cut(nodes[a.o].l, 44), "12px -apple-system, BlinkMacSystemFont, sans-serif", "rgba(225, 231, 248, 0.92)");
        count++;
      }
    }
    if (hovered >= 0 && hovered !== focus) {
      label(hovered, cut(nodes[hovered].l, 56), "500 12px -apple-system, BlinkMacSystemFont, sans-serif", "#ffffff", true);
    }
    // project names
    const projectAlpha = z < 3 ? 0.9 : Math.max(0.3, 0.9 - (z - 3) * 0.15);
    ctx.textAlign = "center";
    if ("letterSpacing" in ctx) /** @type {any} */ (ctx).letterSpacing = "2px";
    for (const [p, i] of projectNode) {
      if (!shownNow(i) || !onScreen(i)) continue;
      const dimmed = (focus >= 0 && nodeHl[i] < 1) || (isolate !== null && isolate !== p);
      ctx.font = "600 10.5px -apple-system, BlinkMacSystemFont, sans-serif";
      const text = projects[p].toUpperCase();
      const w = ctx.measureText(text).width;
      const x = sx(i), y = sy(i) + radiusPx(i) * 2.2 + 12;
      if (!fits(x - w / 2 - 4, y - 8, w + 8, 16)) continue;
      ctx.fillStyle = colorOf(p);
      ctx.globalAlpha = projectAlpha * (dimmed ? 0.25 : 1);
      ctx.fillText(text, x, y);
    }
    ctx.globalAlpha = 1;
    ctx.textAlign = "left";
    if ("letterSpacing" in ctx) /** @type {any} */ (ctx).letterSpacing = "0px";
    if (focus >= 0) return;
    // tasks, then single memories, as room allows
    if (z > 1.7) {
      const tasks = [];
      for (let i = 0; i < n; i++) if (nodes[i].k === "task" && shownNow(i) && onScreen(i)) tasks.push(i);
      tasks.sort((a, b) => degree[b] - degree[a]);
      const alpha = clamp((z - 1.7) / 1.2, 0, 1) * 0.85;
      for (const i of tasks.slice(0, 70)) {
        if (isolate !== null && nodes[i].p !== isolate) continue;
        label(i, cut(nodes[i].l, 34), "500 11px -apple-system, BlinkMacSystemFont, sans-serif", `rgba(214, 222, 245, ${alpha})`);
      }
    }
    if (z > 5) {
      const alpha = clamp((z - 5) / 2, 0, 1) * 0.8;
      let count = 0;
      for (let i = 0; i < n && count < 90; i++) {
        if ((nodes[i].k === "event" || nodes[i].k === "note" || nodes[i].k === "fact") && shownNow(i) && onScreen(i)) {
          if (isolate !== null && nodes[i].p !== isolate) continue;
          label(i, cut(nodes[i].l, 42), "11px -apple-system, BlinkMacSystemFont, sans-serif", `rgba(200, 208, 232, ${alpha})`);
          count++;
        }
      }
    }
  }

  // ---------- picking and the hover card ----------
  const tip = $("tip");
  /** @param {number} mx @param {number} my */
  function pick(mx, my) {
    let best = -1, bestScore = Infinity;
    for (let i = 0; i < n; i++) {
      if (!shownNow(i)) continue;
      if (focus >= 0 && nodeHl[i] < 1 && focusMix > 0.5) continue;
      const dx = positions[i * 2] * cam.s + cam.x - mx, dy = positions[i * 2 + 1] * cam.s + cam.y - my;
      const d = Math.sqrt(dx * dx + dy * dy), r = PICK_PX + radiusPx(i) * 1.4;
      if (d < r && d - radiusPx(i) < bestScore) { best = i; bestScore = d - radiusPx(i); }
    }
    return best;
  }
  /** @param {number} i @param {number} mx @param {number} my */
  function showTip(i, mx, my) {
    if (i < 0) { tip.hidden = true; return; }
    const node = nodes[i];
    tip.replaceChildren();
    const meta = el("div", "meta");
    meta.append(kindChip(KIND_LABEL[/** @type {keyof typeof KIND_LABEL} */ (node.k)] + (node.c && node.k === "event" ? ` · ${node.c}` : ""), colorOf(node.p)));
    if (node.p !== null && node.k !== "project") meta.append(el("span", "", projects[node.p]));
    if (node.t) meta.append(el("span", "", dayFormat.format(new Date(node.t * 1000))));
    if (pinned[i]) meta.append(el("span", "badge pin", "pinned"));
    if (stale[i]) meta.append(el("span", "badge stale", "stale"));
    tip.append(meta, el("div", "text", node.l));
    tip.hidden = false;
    const x = mx + 340 > W ? mx - 360 : mx, y = my + 120 > H ? my - 110 : my;
    tip.style.left = `${x}px`;
    tip.style.top = `${y}px`;
  }

  // ---------- the panel ----------
  const panel = $("panel"), panelBody = $("panel-body"), trailBox = $("trail");
  /** @param {number} i */
  const when = (i) => (nodes[i].t ? shortFormat.format(new Date(/** @type {number} */ (nodes[i].t) * 1000)) : "");
  /** @param {number} i @param {string} [note] */
  const row = (i, note) => ({ index: i, label: nodes[i].l, color: colorOf(nodes[i].p), when: when(i), note });
  const byTimeDesc = (/** @type {number} */ a, /** @type {number} */ b) => (nodes[b].t || 0) - (nodes[a].t || 0);
  const noteByName = new Map();
  nodes.forEach((node, i) => { if (node.k === "note") noteByName.set(node.l.trim().toLowerCase(), i); });
  /** @param {string} target */
  const resolveNote = (target) => {
    const name = target.split("/").pop()?.replace(/\.md$/, "").trim().toLowerCase() || "";
    const i = noteByName.get(name);
    return i === undefined ? null : () => focusOn(i);
  };
  /** What a memory is joined to, less copies hidden since the map loaded. @param {number} i */
  const neighbours = (i) => adj[i].filter((a) => !gone[a.o]);

  function renderTrail() {
    trailBox.replaceChildren();
    trail.slice(-6).forEach((i, k) => {
      if (k) trailBox.append(el("span", "sep", "›"));
      const b = el("button", i === focus ? "here" : "", cut(nodes[i].l, 22));
      b.setAttribute("type", "button");
      b.title = nodes[i].l;
      b.addEventListener("click", () => focusOn(i, false));
      trailBox.append(b);
    });
  }

  let rendered = 0;  // a record that arrives after the panel was drawn again is for a panel that is gone
  /** @param {number} i */
  function renderPanel(i) {
    const generation = ++rendered;
    const node = nodes[i];
    const c = colorOf(node.p);
    panelBody.replaceChildren();
    const head = el("div");
    head.append(kindChip(KIND_LABEL[/** @type {keyof typeof KIND_LABEL} */ (node.k)] + (node.c ? ` · ${node.c}` : ""), c));
    head.append(el("h2", "", node.k === "project" ? projects[/** @type {number} */ (node.p)] : cut(node.l, TITLE_CHARS)));
    const sub = el("div", "sub");
    const go = (/** @type {number | undefined} */ target, /** @type {string} */ text) => {
      if (target === undefined) return sub.append(el("span", "", text));
      const a = el("a", "link", text);
      a.addEventListener("click", () => focusOn(target));
      sub.append(a);
    };
    if (node.p !== null && node.k !== "project") go(projectNode.get(node.p), projects[node.p]);
    const taskLink = neighbours(i).find((a) => a.k === EDGE.task && a.out);
    if (taskLink) go(taskLink.o, nodes[taskLink.o].l);
    if (node.t) sub.append(el("span", "", (node.k === "event" ? timeFormat : dayFormat).format(new Date(node.t * 1000))));
    if (node.a) sub.append(el("b", "", node.a));
    head.append(sub);
    panelBody.append(head);
    const id = memoryId(i);
    const similar = neighbours(i).filter((a) => a.k === EDGE.similar)
      .sort((a, b) => edges[b.e].w - edges[a.e].w)
      .map((a) => {
        const r = row(a.o, edges[a.e].dup ? "same" : `${Math.round(edges[a.e].w * 100)}%`);
        const copy = memoryId(a.o);
        if (actions.enabled && edges[a.e].dup && id && copy && nodes[a.o].k === node.k) {
          r.action = { label: "Hide copy", title: "Hide that copy from agents and the map; this memory stays",
                       run: () => act("duplicate", copy, id) };
        }
        return r;
      });
    const focusTo = (/** @type {number} */ target) => focusOn(target);

    if (id) {
      panelBody.append(curateBar(i, id));
      const box = el("div", "text", "…");
      box.style.marginTop = "14px";
      panelBody.append(box);
      const plain = el("a", "open-plain", node.k === "event" ? "Open the plain record ↗" : "Open the plain note ↗");
      fetchItem(id).then((item) => {
        if (focus !== i || generation !== rendered) return;
        if (!item) { box.textContent = node.l; return; }
        if (item.copies && item.copies.length) panelBody.insertBefore(copiesSection(item.copies, id), plain);
        markTitles(item);
        if (node.k === "note") box.replaceWith(markdown(item.text || "", resolveNote, item.title || node.l));
        else box.textContent = item.text || node.l;
        if (item.why) panelBody.insertBefore(el("div", "why", item.why), panelBody.children[3] || null);
        if (item.checkpoint) {
          const cp = el("section");
          cp.append(el("h3", "", "Checkpoint"));
          if (item.checkpoint.goal) cp.append(el("div", "text", item.checkpoint.goal));
          for (const [key, title] of [["completed", "Completed"], ["next_steps", "Next"], ["open_questions", "Open questions"]]) {
            const list = item.checkpoint[key];
            if (!list || !list.length) continue;
            const ul = el("ul");
            ul.style.paddingLeft = "18px";
            for (const entry of list) ul.append(el("li", "text", entry));
            cp.append(el("h3", "", title), ul);
          }
          panelBody.insertBefore(cp, panelBody.children[4] || null);
        }
      }).catch(() => { if (focus === i && generation === rendered) box.textContent = node.l; });
      if (node.k === "note") {
        panelBody.append(relations("Links to", neighbours(i).filter((a) => a.k === EDGE.link && a.out).map((a) => row(a.o)), focusTo));
        panelBody.append(relations("Linked from", neighbours(i).filter((a) => a.k === EDGE.link && !a.out).map((a) => row(a.o)), focusTo));
      } else {
        panelBody.append(relations("Facts it set", neighbours(i).filter((a) => a.k === EDGE.fact).map((a) => row(a.o)), focusTo));
      }
      panelBody.append(relations("Close in meaning", similar, focusTo));
      if (!similarReady) panelBody.append(el("p", "sub", SIMILAR_WAIT[similarNote] || SIMILAR_WAIT["computing…"]));
      plain.setAttribute("href", `/ui/${node.k}/${encodeURIComponent(node.id.slice(2))}`);
      panelBody.append(plain);
    } else if (node.k === "task") {
      const events = neighbours(i).filter((a) => a.k === EDGE.task && !a.out).map((a) => a.o).sort(byTimeDesc);
      panelBody.append(statRow([["events", events.length], ["first", when(i)],
                                ["last", node.u ? shortFormat.format(new Date(node.u * 1000)) : "—"]]));
      panelBody.append(relations("Timeline", events.map((e) => row(e)), focusTo));
      if (node.p !== null) {
        const plain = el("a", "open-plain", "Open the task page ↗");
        plain.setAttribute("href", `/ui/task/${encodeURIComponent(projects[node.p])}/${encodeURIComponent(node.l)}`);
        panelBody.append(plain);
      }
    } else if (node.k === "project") {
      const tasks = neighbours(i).filter((a) => !a.out && nodes[a.o].k === "task").map((a) => a.o)
        .sort((a, b) => (nodes[b].u || 0) - (nodes[a].u || 0));
      const members = [];
      for (let k = 0; k < n; k++) if (nodes[k].p === node.p && !gone[k] && (nodes[k].k === "event" || nodes[k].k === "note")) members.push(k);
      const eventsCount = members.filter((k) => nodes[k].k === "event").length;
      panelBody.append(statRow([["events", number.format(eventsCount)], ["tasks", tasks.length],
                                ["notes", number.format(members.length - eventsCount)], ["since", when(i)]]));
      panelBody.append(relations("Tasks, latest first", tasks.map((t) => row(t)), focusTo));
      panelBody.append(relations("Latest memories", members.sort(byTimeDesc).slice(0, 60).map((k) => row(k)), focusTo));
      const plain = el("a", "open-plain", "Open the project page ↗");
      plain.setAttribute("href", `/ui/project/${encodeURIComponent(projects[/** @type {number} */ (node.p)])}`);
      panelBody.append(plain);
    } else if (node.k === "fact") {
      const history = data.facts.filter((/** @type {any} */ f) => f.node === i).sort((/** @type {any} */ a, /** @type {any} */ b) => b.from - a.from);
      const section = el("section");
      section.append(el("h3", "", "Values, newest first"));
      const ul = el("ul", "rel facts-chain");
      for (const f of history) {
        const li = el("li");
        li.append(el("span", "v", f.value));
        li.append(f.to ? el("span", "when", `${shortFormat.format(new Date(f.from * 1000))} – ${shortFormat.format(new Date(f.to * 1000))}`)
                       : el("span", "now", `current since ${shortFormat.format(new Date(f.from * 1000))}`));
        ul.append(li);
      }
      section.append(ul);
      panelBody.append(section);
      panelBody.append(relations("Set by", neighbours(i).filter((a) => a.k === EDGE.fact).map((a) => row(a.o)).sort((a, b) => b.when.localeCompare(a.when)), focusTo));
    }
  }

  // ---------- light actions ----------
  /** The last action's outcome, shown on the memory it was taken from. @type {{at: number, text: string, error: boolean} | null} */
  let outcome = null;
  let acting = false;

  /** What the operator marked on a memory, and the buttons to change it. @param {number} i @param {string} id */
  function curateBar(i, id) {
    const bar = el("section", "curate");
    const marks = el("div", "marks");
    if (pinned[i]) marks.append(el("span", "badge pin", "Pinned to the brief"));
    if (stale[i]) marks.append(el("span", "badge stale", "Marked stale"));
    if (marks.childElementCount) bar.append(marks);
    if (actions.enabled) {
      const buttons = el("div", "actions");
      /** @param {string} label @param {string} title @param {string} action */
      const button = (label, title, action) => {
        const b = el("button", "act", label);
        b.setAttribute("type", "button");
        b.title = title;
        b.disabled = acting;
        b.addEventListener("click", () => act(action, id));
        buttons.append(b);
      };
      if (pinned[i]) button("Unpin", "Take it out of the brief", "unpin");
      else button("Pin to brief", "Open every brief of its project with it", "pin");
      if (stale[i]) button("Mark current", "Rank it as any other memory again", "fresh");
      else button("Mark stale", "Agents still find it, marked stale and ranked lower; it leaves the brief", "stale");
      bar.append(buttons);
    }
    if (outcome && outcome.at === i) bar.append(el("p", `act-status${outcome.error ? " error" : ""}`, outcome.text));
    return bar;
  }

  /** When and by whom the marks were set, once the full record has come. @param {any} item */
  function markTitles(item) {
    const since = (/** @type {any} */ mark) => (mark ? `since ${timeFormat.format(new Date(mark.since))}, by ${mark.by}` : "");
    const pin = panelBody.querySelector(".badge.pin"), old = panelBody.querySelector(".badge.stale");
    if (pin instanceof HTMLElement) pin.title = since(item.pinned);
    if (old instanceof HTMLElement) old.title = since(item.stale);
  }

  /** The copies hidden as repeating a memory, each with a way back. @param {any[]} copies @param {string} id */
  function copiesSection(copies, id) {
    const section = el("section");
    section.append(el("h3", "", `Hidden copies · ${copies.length}`));
    const list = el("ul", "rel copies");
    for (const copy of copies) {
      const li = el("li");
      const label = el("span", "label", copy.label);
      label.title = `Hidden ${timeFormat.format(new Date(copy.since))} by ${copy.by}`;
      li.append(label);
      if (actions.enabled) {
        const back = el("button", "row-act", "Restore");
        back.setAttribute("type", "button");
        back.title = "Show it to agents and on the map again";
        back.addEventListener("click", () => act("restore", copy.id, id));
        li.append(back);
      }
      list.append(li);
    }
    section.append(list);
    return section;
  }

  function requestId() {
    if (crypto.randomUUID) return crypto.randomUUID();
    return Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");
  }

  /**
   * Take an action; the server writes it as the operator's event, then the map follows.
   * @param {string} action @param {string} target @param {string} [origin] the memory a copy repeats
   */
  async function act(action, target, origin) {
    if (acting || focus < 0) return;
    const at = focus;
    acting = true;
    panelBody.classList.add("still");  // the same memory again: no rising in
    renderPanel(at);
    let text = DONE[action] || "Done.", error = false;
    try {
      const response = await fetch("/ui/api/atlas/action", {
        method: "POST",
        headers: { "content-type": "application/json", "x-chronicle-token": actions.token || "" },
        body: JSON.stringify({ action, target, of: action === "duplicate" ? origin : undefined, request_id: requestId() }),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.error || `The server answered HTTP ${response.status}.`);
      if (body.status === "unchanged") text = "Already so: nothing changed.";
      applyAction(target, body);
      if (action === "restore" && !byMemoryId.has(target)) text += " Reload the map to see it.";
    } catch (failure) {
      text = failure instanceof Error ? failure.message : String(failure);
      error = true;
    }
    acting = false;
    for (const id of [target, origin]) if (id) items.delete(id);
    outcome = { at, text, error };
    if (focus === at) renderPanel(at);
  }

  /**
   * The map as the server says the memory now is (another tab may have changed it too).
   * @param {string} target @param {{marks?: {pinned?: object, stale?: object}, hidden?: boolean}} body
   */
  function applyAction(target, body) {
    const i = byMemoryId.get(target);
    if (i === undefined) return;
    const marks = body.marks || {};
    pinned[i] = marks.pinned ? 1 : 0;
    if (pinned[i]) pins.add(i); else pins.delete(i);
    const wasStale = stale[i];
    stale[i] = marks.stale ? 1 : 0;
    if (stale[i] !== wasStale) {
      paint(i);
      renderer.setNodes({ color, size, shape, birth, last, seed });
      renderer.setPositions(positions);
    }
    const hide = body.hidden ? 1 : 0;
    if (gone[i] !== hide) counts[kind[i]] += hide ? -1 : 1;
    gone[i] = hide;
    if (gone[i] && focus === i) clearFocus();
    updateHighlight();
    renderFilters();
  }

  /** @param {number} i @param {boolean} [push] */
  function focusOn(i, push = true) {
    if (gone[i]) return;  // hidden as a copy since the map loaded
    if (outcome && outcome.at !== i) outcome = null;
    panelBody.classList.remove("still");
    if (birth[i] > play + 0.01) jumpTo(days);
    focus = i;
    isolate = null;
    if (!kindOn[kind[i]]) { kindOn[kind[i]] = true; renderFilters(); }
    updateHighlight();
    renderLegend();
    if (push && trail[trail.length - 1] !== i) { trail.push(i); if (trail.length > 24) trail.shift(); }
    renderTrail();
    renderPanel(i);
    panelOpen = true;
    panel.classList.add("open");
    panel.setAttribute("aria-hidden", "false");
    renderKnown();
    const depth = { project: 2.2, task: 4.5, event: 7, note: 7, fact: 7 }[nodes[i].k] || 5;
    flyTo(positions[i * 2], positions[i * 2 + 1], Math.max(cam.s, fitScale * depth));
  }
  function clearFocus() {
    focus = -1;
    updateHighlight();
    panelOpen = false;
    panel.classList.remove("open");
    panel.setAttribute("aria-hidden", "true");
    renderKnown();
  }
  $("panel-close").addEventListener("click", clearFocus);

  // ---------- filters and legend ----------
  const filters = $("filters");
  const counts = [0, 0, 0, 0, 0];
  for (let i = 0; i < n; i++) counts[kind[i]]++;
  function renderFilters() {
    filters.replaceChildren();
    [[2, "Events"], [3, "Notes"], [1, "Tasks"], [4, "Facts"]].forEach(([k, label]) => {
      const chip = el("button", `chip glass${kindOn[/** @type {number} */ (k)] ? "" : " off"}`);
      chip.setAttribute("type", "button");
      chip.append(el("span", "dot"), String(label), el("span", "n", number.format(counts[/** @type {number} */ (k)])));
      chip.addEventListener("click", () => { kindOn[/** @type {number} */ (k)] = !kindOn[/** @type {number} */ (k)]; updateHighlight(); renderFilters(); });
      filters.append(chip);
    });
    const meaning = el("button", `chip glass${meaningOn ? "" : " off"}`);
    meaning.setAttribute("type", "button");
    meaning.title = "Show lines between memories that say similar things";
    const dot = el("span", "dot");
    dot.style.color = SIMILAR_TINT;
    meaning.append(dot, "Meaning", el("span", "n", similarReady ? number.format(edges.length - explicitCount) : similarNote));
    meaning.addEventListener("click", () => { meaningOn = !meaningOn; renderFilters(); });
    filters.append(meaning);
  }
  renderFilters();

  const legend = $("legend");
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
    for (const p of byProjectSize) {
      const b = el("button", isolate === p ? "on" : "");
      b.setAttribute("type", "button");
      const dot = el("span", "dot");
      dot.style.background = colorOf(p);
      dot.style.boxShadow = `0 0 8px ${colorOf(p)}`;
      b.append(dot, el("span", "name", projects[p]), el("span", "n", number.format(projectSize.get(p) || 0)));
      b.addEventListener("click", () => {
        clearFocus();
        isolate = isolate === p ? null : p;
        updateHighlight();
        renderLegend();
        if (isolate === null) { fit(true); return; }
        const xs = [], ys = [];
        for (let i = 0; i < n; i++) if (nodes[i].p === p) { xs.push(positions[i * 2]); ys.push(positions[i * 2 + 1]); }
        fit(true, { x0: Math.min(...xs), x1: Math.max(...xs), y0: Math.min(...ys), y1: Math.max(...ys) });
      });
      legend.append(b);
    }
  }
  renderLegend();
  $("stats").replaceChildren(el("b", "", number.format(n)), " memories · ", el("b", "", String(projects.length)),
                             " projects · since ", dayFormat.format(new Date(span0 * 1000)));

  // ---------- time ----------
  const moments = [];
  for (let i = 0; i < n; i++) if (nodes[i].k === "event" && nodes[i].t) moments.push({ t: /** @type {number} */ (nodes[i].t), p: nodes[i].p });
  const timeline = new Timeline(/** @type {HTMLCanvasElement} */ ($("river")), {
    span: [span0, span1], moments, color: (p) => colorOf(p), onScrub: (day) => { playing = false; jumpTo(day); flashDate(); },
  });
  const dateBig = $("date-big"), timeLabel = $("time-label"), nowButton = $("now"), playButton = $("play");
  const known = $("known"), knownList = $("known-list"), knownDate = $("known-date");
  let dateTimer = 0;
  const dateAt = (/** @type {number} */ day) => new Date(momentAt([span0, span1], day) * 1000);
  function bornCount() { let c = 0; for (let i = 0; i < n; i++) if (birth[i] <= play) c++; return c; }
  function flashDate() {
    dateBig.replaceChildren(dayFormat.format(dateAt(play)), el("small", "", `${number.format(bornCount())} memories`));
    dateBig.classList.add("on");
    clearTimeout(dateTimer);
    dateTimer = window.setTimeout(() => { if (!playing) dateBig.classList.remove("on"); }, 1400);
  }
  function renderKnown() {
    known.hidden = true;
    if (play >= days - 0.01 || panelOpen) return;
    knownDate.textContent = dayFormat.format(dateAt(play));
    const now = momentAt([span0, span1], play);
    const current = new Map();
    for (const f of data.facts) {
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
  let knownTimer = 0;
  /** @param {number} day */
  function jumpTo(day) {
    play = clamp(day, 0, days);
    timeline.setPlay(play);
    updateBornShare();
    const travelling = play < days - 0.01;
    nowButton.hidden = !travelling;
    timeLabel.replaceChildren(travelling ? dayFormat.format(dateAt(play)) : "Now", el("span", "", travelling
      ? ` · ${number.format(bornCount())} memories` : ` · ${dayFormat.format(new Date())}`));
    if (!knownTimer) knownTimer = window.setTimeout(() => { knownTimer = 0; renderKnown(); }, 80);
  }
  jumpTo(days);
  function setPlaying(/** @type {boolean} */ on) {
    playing = on;
    following = on && focus < 0;
    if (on && play >= days - 0.01) jumpTo(0);
    playButton.textContent = on ? "❚❚" : "▶";
    playButton.classList.toggle("on", on);
    if (on) { dateBig.classList.add("on"); } else flashDate();
  }
  playButton.addEventListener("click", () => setPlaying(!playing));
  nowButton.addEventListener("click", () => { setPlaying(false); jumpTo(days); dateBig.classList.remove("on"); });
  document.querySelectorAll(".speed button").forEach((b) => b.addEventListener("click", () => {
    speed = Number(/** @type {HTMLElement} */ (b).dataset.speed);
    document.querySelectorAll(".speed button").forEach((o) => o.classList.toggle("on", o === b));
  }));

  // ---------- search ----------
  const search = $("search"), input = /** @type {HTMLInputElement} */ ($("search-input")), results = $("search-results");
  const haystack = nodes.map((node) => (node.k === "project" ? projects[/** @type {number} */ (node.p)] : node.l).toLowerCase());
  const kindWeight = { project: 4, task: 3, fact: 2.5, note: 1.6, event: 1 };
  let found = /** @type {number[]} */ ([]), cursor = 0;
  function openSearch() { search.hidden = false; input.value = ""; runSearch(); input.focus(); }
  function closeSearch() { search.hidden = true; }
  function runSearch() {
    const words = input.value.toLowerCase().split(/\s+/).filter(Boolean);
    if (!words.length) {
      found = [...projectNode.values()].sort((a, b) => (nodes[b].u || 0) - (nodes[a].u || 0)).slice(0, 12);
    } else {
      const scored = [];
      for (let i = 0; i < n; i++) {
        const text = haystack[i];
        if (gone[i] || !words.every((w) => text.includes(w))) continue;
        const kw = kindWeight[/** @type {keyof typeof kindWeight} */ (nodes[i].k)];
        scored.push([i, kw + (text.startsWith(words[0]) ? 2 : 0) + (nodes[i].u || 0) / (span1 || 1)]);
      }
      found = scored.sort((a, b) => b[1] - a[1]).slice(0, 30).map((s) => s[0]);
    }
    cursor = 0;
    results.replaceChildren(...found.map((i, k) => {
      const li = el("li", k === cursor ? "on" : "");
      const dot = el("span", "dot");
      dot.style.background = colorOf(nodes[i].p);
      const label = el("span", "label");
      const text = nodes[i].k === "project" ? projects[/** @type {number} */ (nodes[i].p)] : nodes[i].l;
      const at = words.length ? text.toLowerCase().indexOf(words[0]) : -1;
      if (at >= 0) label.append(text.slice(0, at), el("mark", "", text.slice(at, at + words[0].length)), text.slice(at + words[0].length));
      else label.textContent = text;
      li.append(dot, label, el("span", "meta", `${KIND_LABEL[/** @type {keyof typeof KIND_LABEL} */ (nodes[i].k)]}${nodes[i].t ? " · " + when(i) : ""}`));
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
  /** @type {{x: number, y: number, moved: boolean, vx: number, vy: number, t: number} | null} */
  let drag = null;
  let inertia = { x: 0, y: 0 };
  glCanvas.addEventListener("pointerdown", (event) => {
    drag = { x: event.clientX, y: event.clientY, moved: false, vx: 0, vy: 0, t: performance.now() };
    glCanvas.setPointerCapture(event.pointerId);
    inertia = { x: 0, y: 0 };
  });
  glCanvas.addEventListener("pointermove", (event) => {
    if (drag) {
      const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
      if (!drag.moved && Math.hypot(dx, dy) > 3) { drag.moved = true; glCanvas.classList.add("dragging"); fly = null; userMoved = true; following = false; }
      if (drag.moved) {
        cam.x += dx; cam.y += dy; goal.x = cam.x; goal.y = cam.y;
        const now = performance.now(), dt = Math.max(now - drag.t, 1);
        drag.vx = dx / dt * 16; drag.vy = dy / dt * 16; drag.t = now;
        tip.hidden = true;
      }
      drag.x = event.clientX; drag.y = event.clientY;
      return;
    }
    hovered = pick(event.clientX, event.clientY);
    glCanvas.classList.toggle("pointing", hovered >= 0);
    showTip(hovered, event.clientX, event.clientY);
  });
  glCanvas.addEventListener("pointerup", (event) => {
    if (drag && !drag.moved) {
      const i = pick(event.clientX, event.clientY);
      if (i >= 0) focusOn(i); else if (focus >= 0) clearFocus();
    } else if (drag) {
      inertia = { x: drag.vx, y: drag.vy };
    }
    drag = null;
    glCanvas.classList.remove("dragging");
  });
  glCanvas.addEventListener("pointerleave", () => { hovered = -1; tip.hidden = true; });
  glCanvas.addEventListener("dblclick", () => { clearFocus(); isolate = null; updateHighlight(); renderLegend(); fit(true); });
  glCanvas.addEventListener("wheel", (event) => {
    event.preventDefault();
    fly = null; userMoved = true; following = false;
    const factor = Math.exp(-event.deltaY * (event.ctrlKey ? 0.01 : 0.0016));
    const s = clamp(goal.s * factor, ZOOM_MIN * fitScale, ZOOM_MAX * fitScale);
    goal.x = event.clientX - (event.clientX - goal.x) * (s / goal.s);
    goal.y = event.clientY - (event.clientY - goal.y) * (s / goal.s);
    goal.s = s;
  }, { passive: false });

  // ---------- keys ----------
  window.addEventListener("keydown", (event) => {
    const typing = event.target instanceof HTMLInputElement;
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") { event.preventDefault(); search.hidden ? openSearch() : closeSearch(); return; }
    if (typing) return;
    if (event.key === "/") { event.preventDefault(); openSearch(); }
    else if (event.key === "Escape") { if (!search.hidden) closeSearch(); else if (focus >= 0) clearFocus(); else if (isolate !== null) { isolate = null; updateHighlight(); renderLegend(); } }
    else if (event.key === " ") { event.preventDefault(); setPlaying(!playing); }
    else if (event.key === "f") { clearFocus(); fit(true); }
    else if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
      setPlaying(false);
      jumpTo(play + (event.key === "ArrowLeft" ? -1 : 1) * (event.shiftKey ? 7 : 1));
      flashDate();
    }
  });

  // ---------- size ----------
  function resize() {
    const before = viewport();
    W = window.innerWidth; H = window.innerHeight; dpr = Math.min(window.devicePixelRatio || 1, 2);
    for (const canvas of [sky, glCanvas, ink]) { canvas.width = Math.round(W * dpr); canvas.height = Math.round(H * dpr); }
    timeline.resize();
    // What was in the middle of the map stays there; untouched, the map fits the new window.
    const after = viewport(), dx = after.cx - before.cx, dy = after.cy - before.cy;
    cam.x += dx; cam.y += dy; goal.x += dx; goal.y += dy;
    if (!userMoved && !fly) { fitScale = boundsScale(allBounds()); fit(false); }
  }
  window.addEventListener("resize", resize);
  resize();
  fitScale = boundsScale(allBounds());
  fit(false);

  // ---------- edges by meaning ----------
  (async () => {
    for (;;) {
      try {
        const response = await fetch("/ui/api/atlas/similar");
        const body = await response.json();
        // Stale edges come from the pass before the newest memories: shown at once,
        // while the server brings them up to date for the next visit.
        if (body.status === "ready" || ((body.status === "stale" || body.status === "failed") && body.edges.length)) {
          const at = new Map(nodes.map((n, i) => [n.id, i]));
          for (const [a, b, w, dup] of body.edges) {
            const s = at.get(a), t = at.get(b);
            if (s === undefined || t === undefined) continue; // recorded after the map was loaded, or hidden since
            edges.push({ s, t, k: EDGE.similar, w, dup });
            addAdjacency(edges.length - 1);
          }
          similarReady = true;
          uploadEdges();
          renderer.setPositions(positions);
          updateHighlight();
          renderFilters();
          if (focus >= 0) renderPanel(focus);
          return;
        }
        if (body.status === "none" || body.status === "failed") {
          similarNote = body.status === "none" ? "no vectors" : "unavailable";
          renderFilters();
          if (focus >= 0) renderPanel(focus);
          return;
        }
      } catch { /* the server restarts or is busy: try again */ }
      await sleep(3000);
    }
  })();

  // ---------- the loop ----------
  let then = performance.now();
  /** @param {number} now */
  function frame(now) {
    const dt = Math.min((now - then) / 1000, 0.05);
    then = now;
    const time = now / 1000;
    if (playing && following && now - followAt > FOLLOW_MS) {
      followAt = now;
      followGrowth();
    }
    if (playing) {
      jumpTo(play + dt * PLAY_DAYS_PER_S * speed);
      dateBig.replaceChildren(dayFormat.format(dateAt(play)), el("small", "", `${number.format(bornCount())} memories`));
      if (play >= days) { setPlaying(false); dateBig.classList.remove("on"); }
    }
    const k = 1 - Math.exp(-dt * 7);
    travel += ((play < days - 0.01 ? 1 : 0) - travel) * k;
    focusMix += ((focus >= 0 || isolate !== null ? 1 : 0) - focusMix) * k;
    similarMix += ((meaningOn && similarReady ? 1 : 0) - similarMix) * k;
    stepFly(now);
    if (!fly && !drag) {
      if (Math.abs(inertia.x) + Math.abs(inertia.y) > 0.05) {
        cam.x += inertia.x; cam.y += inertia.y; goal.x = cam.x; goal.y = cam.y;
        inertia.x *= 0.92; inertia.y *= 0.92;
      }
      const z = 1 - Math.exp(-dt * (following ? 1.6 : 14));
      cam.s += (goal.s - cam.s) * z; cam.x += (goal.x - cam.x) * z; cam.y += (goal.y - cam.y) * z;
    }
    drawSky(time);
    renderer.draw({ res: [W, H], view: [cam.s, cam.x, cam.y], dpr, time, play, travel, focus: focusMix,
                    zoom: zoomLevel(), similar: similarMix });
    drawInk(time);
    requestAnimationFrame(frame);
  }
  requestAnimationFrame(frame);
}

main();
