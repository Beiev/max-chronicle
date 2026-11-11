// @ts-check
/**
 * The memory as the Atlas draws it: nodes, the edges between them, and what
 * each looks like. Built once from what the server sends, changed only by the
 * edges by meaning that arrive later and by the operator's marks.
 *
 * No DOM here: every view reads this, and node:test reads it too.
 */

export const DAY = 86400;
export const KINDS = ["project", "task", "event", "note", "fact"];
/** @type {Record<string, string>} */
export const KIND_LABEL = { project: "Project", task: "Task", event: "Event", note: "Note", fact: "Fact" };
export const EDGE = { task: 0, project: 1, link: 2, fact: 3, similar: 4 };
export const EDGE_NAME = ["task", "project", "link", "fact", "similar"];
const SHAPE = { event: 0, task: 1, fact: 2, note: 3, project: 4 };
// How strongly each kind of line is drawn before anything is in focus. A task
// and its memories are a constellation, in thin lines. What belongs straight to
// a project is nearly silent: a thousand such lines are a blot, and the place a
// memory has on the map already says it. Authored links speak up.
const EDGE_ALPHA = [0.1, 0.03, 0.18, 0.2, 0.14];
// One lightness, one modest saturation, hues spread around the wheel: no project shouts.
const PALETTE = ["#8db7e0", "#e0a98a", "#a9c98c", "#c3a6e0", "#e3c27e", "#7fc7cc", "#e09bb0", "#98a6e6",
                 "#c9c68a", "#8fcbae", "#d9a0d6", "#dba678", "#a3bfd9", "#b8ada0", "#86d1c2", "#d6b3c4"];
const UNSCOPED = "#8d93a6";
/** The night chart's two inks: starlight for what is recorded, brass for what the reader or the meaning adds. */
export const STARLIGHT = "#ece8dd";
export const BRASS = "#d8b872";
export const SIMILAR_TINT = BRASS;
/** The day of what is never before the start of a range. */
const ALWAYS = 1e9;
const BIG_CATEGORIES = new Set(["decision", "milestone"]);
const STALE_GREY = /** @type {[number, number, number]} */ ([0.42, 0.45, 0.53]);
// Written by the machinery, not by anyone deciding anything: kept on the map, out of the lists.
const ROUTINE_CATEGORIES = new Set(["daily_summary", "git_commit", "digest_run", "maintenance"]);

/** @param {number} v @param {number} lo @param {number} hi */
export const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

/** A small seeded generator, so the same memory always draws the same sky. @param {number} seed */
export function mulberry32(seed) {
  return () => {
    seed |= 0; seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** A resemblance as a percentage; one short of identical never reads as 100%. @param {number} w between 0 and 1 */
export const percent = (w) => (w < 1 && Math.round(w * 100) >= 100 ? `${(Math.floor(w * 1000) / 10).toFixed(1)}%` : `${Math.round(w * 100)}%`);

/** @param {string} hex @returns {[number, number, number]} */
export function rgb(hex) {
  const v = parseInt(hex.slice(1), 16);
  return [((v >> 16) & 255) / 255, ((v >> 8) & 255) / 255, (v & 255) / 255];
}

/** @param {[number, number, number]} c @param {[number, number, number]} d @param {number} t */
const mix = (c, d, t) => /** @type {[number, number, number]} */ (c.map((v, k) => v + (d[k] - v) * t));

/**
 * @typedef {{id: string, k: string, l: string, p: number | null, t: number | null, u: number | null,
 *   c?: string, a?: string, pi?: number, st?: number}} Node
 * @typedef {{s: number, t: number, k: number, w: number, dup: number}} Edge
 * @typedef {{o: number, e: number, k: number, out: boolean}} Link one end's view of an edge: the other node, the edge, its kind, and whether this node is its source
 */

export class Graph {
  /** @param {any} data the answer of /ui/api/atlas */
  constructor(data) {
    /** @type {Node[]} */ this.nodes = data.nodes;
    const nodes = this.nodes, n = nodes.length;
    this.n = n;
    /** @type {string[]} */ this.projects = data.projects;
    /** @type {{node: number, value: string, from: number, to: number | null}[]} */ this.facts = data.facts || [];
    this.span0 = data.span ? data.span[0] : Date.now() / 1000;
    this.span1 = data.span ? data.span[1] : this.span0 + DAY;
    this.days = Math.max(1, Math.ceil((this.span1 - this.span0) / DAY));

    this.kind = new Int8Array(n);
    /** @type {Map<number, number>} */ this.projectNode = new Map();
    /** @type {Map<number, number>} */ this.projectSize = new Map();
    /** @type {Map<string, number>} */ this.byId = new Map();
    /** @type {Map<string, number>} */ this.byMemoryId = new Map();
    nodes.forEach((node, i) => {
      this.kind[i] = KINDS.indexOf(node.k);
      this.byId.set(node.id, i);
      if (node.k === "project" && node.p !== null) this.projectNode.set(node.p, i);
      if (node.p !== null) this.projectSize.set(node.p, (this.projectSize.get(node.p) || 0) + 1);
      const memory = this.memoryId(i);
      if (memory) this.byMemoryId.set(memory, i);
    });
    this.byProjectSize = [...this.projectSize.entries()].sort((a, b) => b[1] - a[1]).map(([p]) => p);
    this.projectColor = new Map(this.byProjectSize.map((p, rank) => [p, PALETTE[rank % PALETTE.length]]));

    /** @type {Edge[]} */
    this.edges = data.edges.map((/** @type {number[]} */ e) => ({ s: e[0], t: e[1], k: e[2], w: 1, dup: 0 }));
    this.explicitCount = this.edges.length;
    /** @type {Link[][]} */ this.adj = Array.from({ length: n }, () => []);
    this.degree = new Float32Array(n);
    this.edges.forEach((_, index) => this.#join(index));
    // What a node hangs from in the layout: its task, else its project.
    this.parent = new Int32Array(n).fill(-1);
    for (const e of this.edges) if (e.k !== EDGE.link && this.parent[e.s] < 0) this.parent[e.s] = e.t;

    // What the operator marked: pinned to the brief, stale, or hidden as a copy since the map loaded.
    this.pinned = new Uint8Array(n);
    this.stale = new Uint8Array(n);
    this.gone = new Uint8Array(n);
    nodes.forEach((node, i) => { this.pinned[i] = node.pi ? 1 : 0; this.stale[i] = node.st ? 1 : 0; });

    const random = mulberry32(20261029);
    this.random = random;
    this.birth = new Float32Array(n);
    this.last = new Float32Array(n);
    this.seed = new Float32Array(n);
    this.size = new Float32Array(n);
    this.color = new Float32Array(n * 3);
    this.shape = new Float32Array(n);
    nodes.forEach((node, i) => {
      this.birth[i] = node.t === null ? 0 : (node.t - this.span0) / DAY;
      this.last[i] = node.u === null ? this.birth[i] : (node.u - this.span0) / DAY;
      this.seed[i] = random();
      this.shape[i] = SHAPE[/** @type {keyof typeof SHAPE} */ (node.k)];
      this.paint(i);
    });
  }

  /** @param {number} index of an edge */
  #join(index) {
    const e = this.edges[index];
    this.adj[e.s].push({ o: e.t, e: index, k: e.k, out: true });
    this.adj[e.t].push({ o: e.s, e: index, k: e.k, out: false });
    if (e.k !== EDGE.similar) { this.degree[e.s]++; this.degree[e.t]++; }
  }

  /** The id the server knows a memory by (event:<id>, note:<id>); null for other nodes. @param {number} i */
  memoryId(i) {
    const node = this.nodes[i];
    return node.k === "event" ? `event:${node.id.slice(2)}` : node.k === "note" ? `note:${node.id.slice(2)}` : null;
  }

  /** @param {number | null} p */
  colorOf(p) {
    return p === null ? UNSCOPED : this.projectColor.get(p) || UNSCOPED;
  }

  /** What a node is called on the page. @param {number} i */
  title(i) {
    const node = this.nodes[i];
    return node.k === "project" && node.p !== null ? this.projects[node.p] : node.l;
  }

  /** A record the machinery wrote on its own schedule. @param {number} i */
  routine(i) {
    return this.nodes[i].k === "event" && ROUTINE_CATEGORIES.has(this.nodes[i].c || "");
  }

  /** A node's colour and size; a stale memory is greyer and smaller. @param {number} i */
  paint(i) {
    const node = this.nodes[i];
    // A project is its own hue; what belongs to it is starlight with that hue in it.
    let c = rgb(this.colorOf(node.p));
    const light = rgb(STARLIGHT);
    if (node.k === "event") c = mix(c, light, 0.42);
    if (node.k === "note") c = mix(c, light, 0.6);
    if (node.k === "task") c = mix(c, light, 0.2);
    if (node.k === "fact") c = mix(c, rgb(BRASS), 0.8);
    if (this.stale[i]) c = mix(c, STALE_GREY, 0.6);
    this.color.set(c, i * 3);
    const d = this.degree[i];
    this.size[i] = (node.k === "project" ? clamp(6 + Math.sqrt(d) * 0.32, 6, 15)
      : node.k === "task" ? clamp(1.9 + Math.sqrt(d) * 0.22, 1.9, 3.6)
      : node.k === "fact" ? 2.6
      : node.k === "note" ? 1.35
      : BIG_CATEGORIES.has(node.c || "") ? 1.9 : 1.5) * (this.stale[i] ? 0.8 : 1);
  }

  /**
   * The edges by meaning, once the server has them: [a, b, similarity, copy].
   * @param {[string, string, number, number][]} found @returns {number} how many joined the map
   */
  addSimilar(found) {
    let added = 0;
    for (const [a, b, w, dup] of found) {
      const s = this.byId.get(a), t = this.byId.get(b);
      if (s === undefined || t === undefined) continue; // recorded after the map was loaded, or hidden since
      this.edges.push({ s, t, k: EDGE.similar, w, dup });
      this.#join(this.edges.length - 1);
      added++;
    }
    return added;
  }

  /** What a node is joined to, less copies hidden since the map loaded. @param {number} i */
  neighbours(i) {
    return this.adj[i].filter((link) => !this.gone[link.o]);
  }

  /**
   * The buffers the renderer takes for the edges. A line appears with its later
   * end (birth) and is in a range that starts no later than `since`: the day
   * of its earlier end, of those that go dark before the range.
   */
  edgeBuffers() {
    const m = this.edges.length;
    const ends = new Int32Array(m * 2), color = new Float32Array(m * 4), birth = new Float32Array(m), kind = new Float32Array(m);
    const since = new Float32Array(m);
    const tint = rgb(SIMILAR_TINT);
    // Before the start of the range a memory goes dark and a project or a task stays, as the frame of the map.
    const darkBefore = (/** @type {number} */ i) => (this.nodes[i].k === "project" || this.nodes[i].k === "task" ? ALWAYS : this.birth[i]);
    this.edges.forEach((e, index) => {
      ends[index * 2] = e.s; ends[index * 2 + 1] = e.t;
      const similar = e.k === EDGE.similar;
      const c = similar ? tint : this.color.subarray(e.s * 3, e.s * 3 + 3);
      const a = similar ? EDGE_ALPHA[e.k] * clamp((e.w - 0.78) / 0.12, 0.35, 1.2) : EDGE_ALPHA[e.k];
      color.set([c[0], c[1], c[2], a], index * 4);
      birth[index] = Math.max(this.birth[e.s], this.birth[e.t]);
      since[index] = Math.min(darkBefore(e.s), darkBefore(e.t));
      kind[index] = e.k;
    });
    return { ends, color, birth, since, kind };
  }

  /**
   * Where the layout starts: suns spread by size (a spiral on the flat map, a
   * ball in space), everything else scattered around what it hangs from.
   * @param {2 | 3} dims @param {Record<string, number[]>} known positions kept from an earlier visit
   * @returns {{positions: Float32Array, warm: boolean}}
   */
  seedPositions(dims, known) {
    const n = this.n, positions = new Float32Array(n * 3), random = mulberry32(20261110);
    const deep = dims === 3;
    const count = this.byProjectSize.length;
    this.byProjectSize.forEach((p, rank) => {
      const i = this.projectNode.get(p);
      if (i === undefined) return;
      const angle = rank * 2.39996;
      if (deep) {
        // The golden-angle lattice on a ball whose shells grow with rank: the largest projects in the middle.
        const radius = 230 * Math.cbrt(rank + 0.4), up = 1 - (2 * (rank + 0.5)) / Math.max(count, 1);
        const ring = Math.sqrt(Math.max(0, 1 - up * up));
        positions.set([Math.cos(angle) * ring * radius, up * radius, Math.sin(angle) * ring * radius], i * 3);
      } else {
        const radius = 300 * Math.sqrt(rank + 0.35);
        positions.set([Math.cos(angle) * radius, Math.sin(angle) * radius, 0], i * 3);
      }
    });
    const spread = { project: 0, task: 90, event: 26, fact: 8, note: 150 };
    const rank = ["project", "task", "event", "fact", "note"];
    const order = [...this.nodes.keys()].sort((a, b) => rank.indexOf(this.nodes[a].k) - rank.indexOf(this.nodes[b].k));
    let kept = 0;
    for (const i of order) {
      const node = this.nodes[i], at = known[node.id];
      if (at && at.length === 3 && (deep || at[2] === 0)) { positions.set(at, i * 3); kept++; continue; }
      if (node.k === "project") continue;
      const anchor = this.parent[i];
      const r = spread[/** @type {keyof typeof spread} */ (node.k)] * (0.4 + random()) * (deep ? 0.7 : 1);
      const a = random() * Math.PI * 2, lift = deep ? random() * 2 - 1 : 0, ring = Math.sqrt(1 - lift * lift);
      const from = anchor >= 0 ? [positions[anchor * 3], positions[anchor * 3 + 1], positions[anchor * 3 + 2]]
        : [(random() - 0.5) * 900, (random() - 0.5) * 900, deep ? (random() - 0.5) * 900 : 0];
      positions.set([from[0] + Math.cos(a) * ring * r, from[1] + Math.sin(a) * ring * r, from[2] + lift * r], i * 3);
    }
    return { positions, warm: kept > n * 0.8 };
  }
}
