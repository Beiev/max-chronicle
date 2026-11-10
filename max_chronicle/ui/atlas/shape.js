// @ts-check
/**
 * The form of each project.
 *
 * The layout (layout.worker.js) leaves every project a ball of memories around
 * its sun, and one ball looks like another. Here each project is given a galaxy
 * of its own, and which kind is read off the project itself:
 *
 *   large, still being written     a spiral: two arms, three when its tasks are many; some have a bar
 *   large, gone quiet              an elliptical: a great ball, dense at the heart
 *   middling, still being written  a barred spiral, or a ring around a small heart
 *   middling, gone quiet           a lens, or a small elliptical
 *   small, still being written     an irregular cloud
 *   small, gone quiet              a tight round cluster
 *
 * Within a galaxy a memory's place says three things. How far it is from the
 * sun is its age: the oldest memories are the heart and the newest the rim, so
 * a galaxy grows outward as the time machine plays. Its direction around the
 * sun is the layout's: the memories of one task stand on one side, and a task
 * is an arc from its first memory to its last. How high it stands off the
 * plane of a disc is the layout's too, so a disc has the thickness of a body.
 *
 * The galaxies are not left where the layout leaves their suns, on the skin
 * of one great ball with nothing inside: each is drawn in toward the middle by
 * a share of its own, so they lie scattered through the room, nearer one
 * another than balls would need, and no two through each other.
 *
 * Galaxies turn, each about its own pole at its own pace, a spiral with its
 * arms trailing. A turn is the same for every memory of a galaxy, so nothing
 * changes its neighbours.
 *
 * What the operator marked stale leaves its galaxy. Out at the edge of the
 * room, apart from the galaxies, is a hole, there only once something has been
 * let go, and the stale memories circle it in a disc: the oldest nearest, each
 * at its own pace, the near ones faster, as things circle a black hole.
 *
 * No DOM here: the page asks this module where a memory is drawn, the sky and
 * the gas ask it for the plane of each galaxy, and node:test asks it too.
 */

import { mulberry32 } from "./model.js";

/** @typedef {[number, number, number]} Vec3 */
/** @typedef {"spiral" | "barred" | "ring" | "lens" | "ball" | "cloud" | "cluster"} Form */
/**
 * @typedef {{
 *   project: number, sun: number, all: number[], members: number[], byAge: number[], age: Float32Array, form: Form, disc: boolean,
 *   arms: number, wind: number, bunch: number, bar: number, phase: number, gather: number, wide: number, bulge: number,
 *   thin: number, axes: Vec3, lumps: number[], rate: number, angle: number,
 *   lean: Vec3, n: Vec3, u: Vec3, v: Vec3, reach: number, tall: number,
 * }} Galaxy sun and members are node indexes; all is every memory of the project and members those not let go;
 *   byAge is the members from the oldest; age is where each member stands
 *   in that order, 0 the oldest and 1 the newest; n is the pole, u and v the plane; reach is how far from the sun
 *   the galaxy goes in its plane and tall how far off it, as the last shaping left them; angle is how far it has
 *   turned and rate how fast it turns, in radians a second
 */

/** A project of fewer memories than this keeps the ball the layout gave it. */
export const SHAPED_FROM = 8;
const MIDDLING_FROM = 40;
const LARGE_FROM = 150;
const DAY = 86400;
/** A project is still being written when this share of its memories is of the last thirty days, or its newest is of the last three weeks. */
const FRESH_SHARE = 0.04;
const FRESH_DAYS = 30;
const NEWEST_DAYS = 21;
/** A spiral with this many tasks or more has three arms. */
const THREE_ARMS_FROM = 14;
/** How much of a galaxy's tilt is its own; the rest faces away from the middle of the memory. */
const OWN_TILT = 0.55;
/** What a galaxy near the middle of the memory faces, having no away: where the page first looks from. */
const MIDDLE_POLE = unit([-0.46, 0.3, 0.84]);
/** A galaxy needs less room than the ball it was: everything is drawn this much nearer the middle of the suns. */
const NEARER = 0.78;
/** And each sun by a share of its own on top of that, between this and all the way out, so the room inside is not empty. */
const DEEPEST = 0.3;
/** Two galaxies keep at least this share of their two reaches between their suns. */
const APART = 0.72;
/** What a project too small for a form of its own is given for a reach when the suns are kept apart. */
const SMALL_REACH = 26;
/** In a ball the memories of one task would stand in a straight row from the heart out; they are swept round by this much, the newest furthest. */
const SWEEP = 1.7;
/** How far the shadow of the hole reaches with one memory in it, and how much further with each (by the root of their number). */
const HOLE_SHADOW = 14;
const HOLE_GROWS = 1.6;
/** Where the hole lies from the middle of the suns, as far out as the outermost of them: low and to the left of where the page first looks from, a little nearer the reader. */
const HOLE_AWAY = unit([-0.87, -0.46, 0.19]);
const HOLE_OUT = 0.95;
/** The disc of what circles the hole, from its inner edge to its outer, in shadows; and the room the suns leave it. */
const HOLE_DISC = /** @type {[number, number]} */ ([1.7, 3.5]);
const HOLE_ROOM = 4.6;
/** How fast the inner edge of the disc turns, in radians a second; further out it is slower, as an orbit is. */
const HOLE_RATE = 0.2;
/** The pole of the hole's disc: tipped, so that the page first sees it as a ring seen aslant. */
const HOLE_POLE = unit([0.25, 0.9, 0.35]);
/**
 * @typedef {{centre: Vec3, n: Vec3, shadow: number, members: number[]}} Hole where it is, the pole of its disc, how far
 *   its shadow reaches, and the memories that circle it, the oldest first
 */
/** @type {Record<Form, number>} How the memories gather toward the heart: the larger, the nearer the heart the older half lies. */
const GATHER = { spiral: 1.5, barred: 1.5, ring: 1, lens: 1.3, ball: 1.9, cloud: 1.25, cluster: 2.3 };
/** @type {Record<Form, number>} How many times wider a galaxy is than the ball the layout left. */
const WIDE = { spiral: 1.75, barred: 1.6, ring: 1.5, lens: 1.35, ball: 1.05, cloud: 1.3, cluster: 0.8 };
/** @type {Record<Form, number>} How thick the heart of a disc is, as a share of how wide the disc is. */
const BULGE = { spiral: 0.3, barred: 0.26, ring: 0.2, lens: 0.36, ball: 0, cloud: 0, cluster: 0 };
/** @type {Record<Form, number>} How thick the rim of a disc is, as a share of how wide the disc is. */
const THIN = { spiral: 0.07, barred: 0.07, ring: 0.08, lens: 0.13, ball: 0, cloud: 0, cluster: 0 };

/** @param {number[]} v @returns {Vec3} */
function unit(v) {
  const length = Math.hypot(v[0], v[1], v[2]);
  return length > 1e-9 ? [v[0] / length, v[1] / length, v[2] / length] : [0, 0, 1];
}

/** Two directions at right angles to a pole and to each other. @param {Vec3} pole @returns {[Vec3, Vec3]} */
export function planeOf(pole) {
  const aside = Math.abs(pole[1]) < 0.9 ? [0, 1, 0] : [1, 0, 0];
  const u = unit([aside[1] * pole[2] - aside[2] * pole[1], aside[2] * pole[0] - aside[0] * pole[2], aside[0] * pole[1] - aside[1] * pole[0]]);
  return [u, [pole[1] * u[2] - pole[2] * u[1], pole[2] * u[0] - pole[0] * u[2], pole[0] * u[1] - pole[1] * u[0]]];
}

/** @param {number} lo @param {number} hi @param {number} x */
const smooth = (lo, hi, x) => { const t = Math.min(1, Math.max(0, (x - lo) / (hi - lo))); return t * t * (3 - 2 * t); };

export class Shaper {
  /**
   * @param {{nodes: {k: string, p: number | null, t?: number | null}[], projectNode: Map<number, number>, stale?: Uint8Array}} graph
   */
  constructor(graph) {
    this.graph = graph;
    /** The hole in the middle, once anything has been let go. @type {Hole | null} */ this.hole = null;
    /** Which nodes circle the hole, and for each how far out (in shadows), where on its circle it began and how fast it goes. */
    this.fallen = new Uint8Array(graph.nodes.length);
    this.orbit = new Float32Array(graph.nodes.length * 3);
    /** What the hole's clock read when each node fell: it circles from then. */
    this.fell = new Float32Array(graph.nodes.length);
    /** Seconds the sky has turned. */
    this.clock = 0;
    /** @type {Map<number, Galaxy>} */ this.galaxies = new Map();
    /** The galaxy each node is a member of. @type {(Galaxy | undefined)[]} */ this.of = new Array(graph.nodes.length);
    /** Every sun, with how deep into the room it is drawn (1 stays out, less comes in). @type {{project: number, sun: number, deep: number}[]} */
    this.suns = [...graph.projectNode].map(([project, sun]) => ({ project, sun, deep: DEEPEST + (1 - DEEPEST) * Math.sqrt(mulberry32(7717 + project * 131)()) }));
    /** The sun each node goes with: its project's, or itself. @type {Int32Array} */
    this.sunOf = Int32Array.from(graph.nodes, (node) => (node.p !== null && graph.projectNode.has(node.p) ? /** @type {number} */ (graph.projectNode.get(node.p)) : -1));
    /** @type {Map<number, number[]>} */ const members = new Map();
    let now = 0;
    graph.nodes.forEach((node, i) => {
      if (node.t) now = Math.max(now, node.t);
      if (node.p === null || node.k === "project" || !graph.projectNode.has(node.p)) return;
      const list = members.get(node.p) || [];
      list.push(i);
      members.set(node.p, list);
    });
    for (const [project, list] of members) {
      if (list.length < SHAPED_FROM) continue;
      const when = (/** @type {number} */ i) => graph.nodes[i].t || 0;
      const byAge = [...list].sort((a, b) => when(a) - when(b) || a - b);
      const age = new Float32Array(list.length);
      const place = new Map(byAge.map((i, rank) => [i, rank / Math.max(byAge.length - 1, 1)]));
      list.forEach((i, k) => { age[k] = /** @type {number} */ (place.get(i)); });
      const fresh = list.filter((i) => when(i) >= now - FRESH_DAYS * DAY).length / list.length;
      const written = fresh >= FRESH_SHARE || now - when(byAge[byAge.length - 1]) < NEWEST_DAYS * DAY;
      const tasks = list.filter((i) => graph.nodes[i].k === "task").length;

      // The same project draws the same numbers on every visit, whatever else the memory holds.
      const random = mulberry32(9001 + project * 53);
      const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
      /** @type {Vec3} */ const lean = [Math.cos(around) * ring, up, Math.sin(around) * ring];
      const hand = random() < 0.5 ? -1 : 1, pick = random(), vary = random(), more = random(), phase = random() * Math.PI * 2;
      const large = list.length >= LARGE_FROM, middling = !large && list.length >= MIDDLING_FROM;
      /** @type {Form} */
      const form = large ? (written ? (pick < 0.3 ? "barred" : "spiral") : "ball")
        : middling ? (written ? (pick < 0.42 ? "ring" : "barred") : (pick < 0.5 ? "lens" : "ball"))
        : written ? "cloud" : "cluster";
      const disc = form === "spiral" || form === "barred" || form === "ring" || form === "lens";
      const arms = form === "spiral" ? (tasks >= THREE_ARMS_FROM ? 3 : 2) : form === "barred" ? 2 : 0;
      const wind = arms ? hand * (form === "spiral" ? 1.5 + vary * 0.8 : 1.2 + vary * 0.6) : 0;
      const oval = 1.08 + vary * 0.14;
      /** @type {Vec3} */
      const axes = form === "ball" ? [1, 0.72 + vary * 0.2, 0.56 + more * 0.2] : form === "cloud" ? [1.4 + vary * 0.3, 0.9, 0.55 + more * 0.2]
        : form === "lens" ? [oval, 1 / oval, 1] : [1, 1, 1];
      /** @type {Galaxy} */
      const galaxy = {
        project, sun: /** @type {number} */ (graph.projectNode.get(project)), all: list, members: list, byAge, age, form, disc,
        arms, wind, bunch: arms ? 0.72 : 0, bar: form === "barred" ? 0.3 + more * 0.12 : 0, phase,
        gather: GATHER[form], wide: WIDE[form], bulge: BULGE[form], thin: THIN[form], axes,
        lumps: [1.7 + vary * 1.2, 2.3 + more * 1.1, 1.3 + pick * 1.4, vary * 6.28, more * 6.28, pick * 6.28],
        // A spiral turns with its arms trailing; what has no arms turns as its own numbers say, and a ball only drifts.
        rate: (arms ? -Math.sign(wind) : hand) * (disc ? 0.011 + more * 0.008 : 0.004 + more * 0.004),
        angle: 0, lean, n: [0, 0, 1], u: [1, 0, 0], v: [0, 1, 0], reach: 1, tall: 1,
      };
      this.galaxies.set(project, galaxy);
      for (const i of list) this.of[i] = galaxy;
    }
    this.restale(graph.stale || new Uint8Array(graph.nodes.length));
  }

  /**
   * Says again which memories have been let go. They leave their galaxies, whose forms stay as they were read off the
   * whole project, and circle the hole; with none let go there is no hole.
   * @param {Uint8Array} stale one per node
   */
  restale(stale) {
    const nodes = this.graph.nodes, when = (/** @type {number} */ i) => nodes[i].t || 0;
    for (const galaxy of this.galaxies.values()) {
      galaxy.members = galaxy.all.filter((i) => !stale[i]);
      galaxy.byAge = [...galaxy.members].sort((a, b) => when(a) - when(b) || a - b);
      const place = new Map(galaxy.byAge.map((i, rank) => [i, rank / Math.max(galaxy.byAge.length - 1, 1)]));
      galaxy.age = Float32Array.from(galaxy.members, (i) => /** @type {number} */ (place.get(i)));
      for (const i of galaxy.all) this.of[i] = stale[i] ? undefined : galaxy;
    }
    const members = [...nodes.keys()].filter((i) => stale[i] && nodes[i].k !== "project").sort((a, b) => when(a) - when(b) || a - b);
    const had = this.fallen;
    this.fallen = new Uint8Array(nodes.length);
    members.forEach((i, rank) => {
      this.fallen[i] = 1;
      if (!had[i]) this.fell[i] = this.clock;
      // The oldest nearest the hole; where on its circle each begins is its own, the same on every visit.
      const out = HOLE_DISC[0] + (HOLE_DISC[1] - HOLE_DISC[0]) * Math.pow((rank + 0.5) / members.length, 0.8);
      this.orbit.set([out, mulberry32(3301 + i * 17)() * Math.PI * 2, HOLE_RATE * Math.pow(out / HOLE_DISC[0], -1.5)], i * 3);
    });
    this.hole = members.length ? { centre: this.hole ? this.hole.centre : [0, 0, 0], n: this.hole ? this.hole.n : HOLE_POLE,
                                   shadow: HOLE_SHADOW + Math.sqrt(members.length) * HOLE_GROWS, members } : null;
  }

  /**
   * Where every node is drawn before any galaxy has turned, given where the layout put it.
   * @param {Float32Array} laid x, y, z per node, as the layout left them
   * @param {2 | 3} dims on the flat map every galaxy lies in the page
   * @param {Float32Array} [out] @returns {Float32Array}
   */
  shape(laid, dims, out = new Float32Array(laid.length)) {
    const deep = dims === 3, suns = this.suns;
    // The middle of the suns: the galaxies are scattered about it, and each turns its face away from it.
    let mx = 0, my = 0, mz = 0;
    for (const { sun } of suns) { mx += laid[sun * 3]; my += laid[sun * 3 + 1]; mz += laid[sun * 3 + 2]; }
    const count = Math.max(suns.length, 1);
    mx /= count; my /= count; mz /= count;

    // How large each galaxy is: as far as the layout keeps its memories from the sun, on the whole, and wider by its form.
    for (const galaxy of this.galaxies.values()) {
      const sx = laid[galaxy.sun * 3], sy = laid[galaxy.sun * 3 + 1], sz = laid[galaxy.sun * 3 + 2];
      let mean = 0;
      for (const i of galaxy.members) mean += Math.hypot(laid[i * 3] - sx, laid[i * 3 + 1] - sy, laid[i * 3 + 2] - sz);
      const size = Math.max(mean / Math.max(galaxy.members.length, 1), 12) * galaxy.wide;
      galaxy.reach = size * Math.max(galaxy.axes[0], galaxy.axes[1]);
      galaxy.tall = galaxy.disc ? size * (galaxy.bulge + galaxy.thin) : size * galaxy.axes[2];
    }

    // Where each sun is drawn: in toward the middle by its own share, then apart from any sun it would lie through.
    const at = new Float64Array(suns.length * 3), room = suns.map(({ project }) => this.galaxies.get(project)?.reach ?? SMALL_REACH);
    suns.forEach(({ sun, deep: share }, k) => {
      at[k * 3] = mx + (laid[sun * 3] - mx) * NEARER * share;
      at[k * 3 + 1] = my + (laid[sun * 3 + 1] - my) * NEARER * share;
      at[k * 3 + 2] = mz + (laid[sun * 3 + 2] - mz) * NEARER * share;
    });
    const hole = this.hole;
    let hx = mx, hy = my, hz = mz;
    if (hole) {
      let out = 0;
      for (let k = 0; k < suns.length; k++) out = Math.max(out, Math.hypot(at[k * 3] - mx, at[k * 3 + 1] - my, at[k * 3 + 2] - mz));
      const away = deep ? HOLE_AWAY : unit([HOLE_AWAY[0], HOLE_AWAY[1], 0]);
      hx = mx + away[0] * out * HOLE_OUT; hy = my + away[1] * out * HOLE_OUT; hz = mz + away[2] * out * HOLE_OUT;
      hole.centre = [hx, hy, hz];
      hole.n = deep ? HOLE_POLE : [0, 0, 1];
    }
    for (let pass = 0; pass < 14; pass++) {
      for (let a = 0; hole && a < suns.length; a++) {
        // The hole does not give way: a sun that would lie through its disc moves off, all the way.
        let dx = at[a * 3] - hx, dy = at[a * 3 + 1] - hy, dz = deep ? at[a * 3 + 2] - hz : 0, far = Math.hypot(dx, dy, dz);
        const need = (room[a] + hole.shadow * HOLE_ROOM) * APART;
        if (far >= need) continue;
        if (far < 1e-6) { dx = Math.cos(a * 2.4); dy = Math.sin(a * 2.4); dz = 0; far = 1; }
        const push = (need - far) / far;
        at[a * 3] += dx * push; at[a * 3 + 1] += dy * push; at[a * 3 + 2] += dz * push;
      }
      for (let a = 0; a < suns.length; a++) {
        for (let b = a + 1; b < suns.length; b++) {
          let dx = at[b * 3] - at[a * 3], dy = at[b * 3 + 1] - at[a * 3 + 1], dz = at[b * 3 + 2] - at[a * 3 + 2];
          let far = Math.hypot(dx, dy, dz);
          const need = (room[a] + room[b]) * APART;
          if (far >= need) continue;
          // Two suns in one place part along a line of their own, the same on every visit.
          if (far < 1e-6) { dx = Math.cos(a * 2.4 + b); dy = Math.sin(a * 2.4 + b); dz = 0; far = 1; }
          // The larger of the two gives way less.
          const push = (need - far) / far, toA = room[b] / (room[a] + room[b]), toB = 1 - toA;
          at[a * 3] -= dx * push * toA; at[a * 3 + 1] -= dy * push * toA;
          at[b * 3] += dx * push * toB; at[b * 3 + 1] += dy * push * toB;
          if (deep) { at[a * 3 + 2] -= dz * push * toA; at[b * 3 + 2] += dz * push * toB; }
        }
      }
    }
    /** @type {Map<number, number>} */ const drawnSun = new Map(suns.map(({ sun }, k) => [sun, k]));
    // Whatever has no galaxy goes with its sun, keeping its place beside it; what has no sun at all comes nearer the middle.
    for (let i = 0, n = laid.length / 3; i < n; i++) {
      const own = drawnSun.get(i), with_ = own !== undefined ? own : drawnSun.get(this.sunOf[i]);
      if (with_ === undefined) {
        out[i * 3] = mx + (laid[i * 3] - mx) * NEARER; out[i * 3 + 1] = my + (laid[i * 3 + 1] - my) * NEARER; out[i * 3 + 2] = mz + (laid[i * 3 + 2] - mz) * NEARER;
      } else {
        const sun = suns[with_].sun * 3;
        out[i * 3] = at[with_ * 3] + (laid[i * 3] - laid[sun]) * NEARER;
        out[i * 3 + 1] = at[with_ * 3 + 1] + (laid[i * 3 + 1] - laid[sun + 1]) * NEARER;
        out[i * 3 + 2] = at[with_ * 3 + 2] + (laid[i * 3 + 2] - laid[sun + 2]) * NEARER;
      }
    }

    if (hole) {
      // What has been let go: on its circle about the hole, a little off the plane of the disc.
      const [u, v] = planeOf(hole.n), n = hole.n;
      for (const i of hole.members) {
        const r = this.orbit[i * 3] * hole.shadow, turn = this.orbit[i * 3 + 1], x = Math.cos(turn) * r, y = Math.sin(turn) * r;
        const h = deep ? (mulberry32(911 + i * 29)() - 0.5) * hole.shadow * 0.3 : 0;
        for (let a = 0; a < 3; a++) out[i * 3 + a] = hole.centre[a] + u[a] * x + v[a] * y + n[a] * h;
      }
    }

    for (const galaxy of this.galaxies.values()) {
      const sx = laid[galaxy.sun * 3], sy = laid[galaxy.sun * 3 + 1], sz = laid[galaxy.sun * 3 + 2];
      if (deep) {
        const away = unit([sx - mx, sy - my, sz - mz]), far = Math.hypot(sx - mx, sy - my, sz - mz) * NEARER;
        // A sun near the middle has no away to speak of: its galaxy faces where the page first looks from.
        const pull = Math.min(far / 320, 1), rest = 1 - pull;
        galaxy.n = unit([0, 1, 2].map((a) => away[a] * pull + MIDDLE_POLE[a] * rest + galaxy.lean[a] * OWN_TILT));
      } else {
        galaxy.n = [0, 0, 1];
      }
      const [u, v] = deep ? planeOf(galaxy.n) : /** @type {[Vec3, Vec3]} */ ([[1, 0, 0], [0, 1, 0]]);
      galaxy.u = u; galaxy.v = v;
      const n = galaxy.n, list = galaxy.members, axes = galaxy.axes, lumps = galaxy.lumps;
      // where the sun itself is drawn, and the measure of the galaxy
      const ox = out[galaxy.sun * 3], oy = out[galaxy.sun * 3 + 1], oz = out[galaxy.sun * 3 + 2];
      const size = galaxy.reach / Math.max(axes[0], axes[1]), core = size * 0.2, barred = galaxy.bar * size;

      for (let k = 0; k < list.length; k++) {
        const i = list[k], age = galaxy.age[k];
        const dx = laid[i * 3] - sx, dy = laid[i * 3 + 1] - sy, dz = laid[i * 3 + 2] - sz;
        const laidAt = Math.hypot(dx, dy, dz) || 1;
        // The layout's word on where the memory stands: which way round the pole, and how far up toward it.
        const high = deep ? Math.max(-1, Math.min(1, (dx * n[0] + dy * n[1] + dz * n[2]) / laidAt)) : 0;
        const a = dx * u[0] + dy * u[1] + dz * u[2], b = dx * v[0] + dy * v[1] + dz * v[2];
        let x, y, h;
        if (galaxy.disc) {
          let turn = Math.atan2(b, a);
          // The oldest at the heart, the newest at the rim. A ring keeps its oldest sixth as a heart and the rest far out.
          let r = size * (0.05 + 0.95 * Math.pow(age, 1 / galaxy.gather));
          if (galaxy.form === "ring") r = age < 0.16 ? size * 0.2 * Math.sqrt(age / 0.16) : size * (0.7 + 0.3 * ((age - 0.16) / 0.84));
          if (galaxy.arms) {
            // An arm is a spiral through the disc, straight inside a bar. A memory's angle is measured from the arm
            // at its own radius and drawn toward the nearest one, by less than the arms are apart: nobody overtakes.
            const arm = galaxy.wind * Math.log(1 + Math.max(r - barred, 0) / core) + galaxy.phase, off = turn - arm;
            const pull = galaxy.bunch + (0.88 - galaxy.bunch) * (barred ? 1 - smooth(barred * 0.85, barred * 1.15, r) : 0);
            turn = off - (pull / galaxy.arms) * Math.sin(galaxy.arms * off) + arm;
          }
          x = r * Math.cos(turn) * axes[0];
          y = r * Math.sin(turn) * axes[1];
          // Thick at the heart, thin at the rim, and every memory at the height the layout gave it within that.
          const thick = galaxy.form === "ring" ? (r < size * 0.3 ? galaxy.bulge : galaxy.thin)
            : galaxy.bulge * Math.exp(-((r / (size * 0.3)) ** 2)) + galaxy.thin;
          h = high * size * thick;
        } else {
          // A ball: the layout's direction, the age as the distance, and the three axes each of its own length.
          // The direction is swept round the pole with age, so a task is a curl through the ball and not a spoke.
          const r = (size * (0.06 + 0.94 * Math.pow(age, 1 / galaxy.gather))) / laidAt;
          const sweep = Math.sign(galaxy.rate) * SWEEP * (age - 0.5), cos = Math.cos(sweep), sin = Math.sin(sweep);
          x = (a * cos - b * sin) * r * axes[0];
          y = (a * sin + b * cos) * r * axes[1];
          h = high * laidAt * r * axes[2];
          if (galaxy.form === "cloud") {
            // An irregular cloud is that ball pushed about by a slow swell, the same for neighbours.
            const swell = size * 0.2, px = x / size, py = y / size, pz = h / size;
            x += swell * Math.sin(lumps[0] * py + lumps[3]);
            y += swell * Math.sin(lumps[1] * (deep ? pz : px) + lumps[4]);
            if (deep) h += swell * 0.6 * Math.sin(lumps[2] * px + lumps[5]);
          }
        }
        out[i * 3] = ox + u[0] * x + v[0] * y + n[0] * h;
        out[i * 3 + 1] = oy + u[1] * x + v[1] * y + n[1] * h;
        out[i * 3 + 2] = oz + u[2] * x + v[2] * y + n[2] * h;
      }
    }
    return out;
  }

  /** Lets every galaxy turn for a while, and what circles the hole go on. @param {number} seconds */
  spin(seconds) {
    for (const galaxy of this.galaxies.values()) galaxy.angle += galaxy.rate * seconds;
    this.clock += seconds;
  }

  /**
   * A point turned about a pole through a centre.
   * @param {number} x @param {number} y @param {number} z the point, from the centre @param {Vec3} pole @param {number} angle
   * @returns {Vec3} where it is then, from the centre
   */
  static about(x, y, z, pole, angle) {
    const cos = Math.cos(angle), sin = Math.sin(angle), [nx, ny, nz] = pole;
    // What lies along the pole stays, the rest goes round it.
    const along = (nx * x + ny * y + nz * z) * (1 - cos);
    return [x * cos + (ny * z - nz * y) * sin + nx * along, y * cos + (nz * x - nx * z) * sin + ny * along,
            z * cos + (nx * y - ny * x) * sin + nz * along];
  }

  /** How far round the hole a fallen node has gone since it fell. @param {number} i */
  #round(i) {
    return this.orbit[i * 3 + 2] * (this.clock - this.fell[i]);
  }

  /**
   * Where every node is drawn now that its galaxy has turned as far as it has.
   * @param {Float32Array} shaped x, y, z per node before any turn; suns are what the galaxies turn about
   * @param {Float32Array} [out] @returns {Float32Array}
   */
  turn(shaped, out = new Float32Array(shaped.length)) {
    out.set(shaped);
    for (const galaxy of this.galaxies.values()) {
      if (!galaxy.angle) continue;
      const cos = Math.cos(galaxy.angle), sin = Math.sin(galaxy.angle), [nx, ny, nz] = galaxy.n;
      const sx = shaped[galaxy.sun * 3], sy = shaped[galaxy.sun * 3 + 1], sz = shaped[galaxy.sun * 3 + 2];
      for (const i of galaxy.members) {
        const x = shaped[i * 3] - sx, y = shaped[i * 3 + 1] - sy, z = shaped[i * 3 + 2] - sz;
        // A turn about the pole: what lies along the pole stays, the rest goes round it.
        const along = (nx * x + ny * y + nz * z) * (1 - cos);
        out[i * 3] = sx + x * cos + (ny * z - nz * y) * sin + nx * along;
        out[i * 3 + 1] = sy + y * cos + (nz * x - nx * z) * sin + ny * along;
        out[i * 3 + 2] = sz + z * cos + (nx * y - ny * x) * sin + nz * along;
      }
    }
    const hole = this.hole;
    if (hole) {
      const [cx, cy, cz] = hole.centre;
      for (const i of hole.members) {
        const at = Shaper.about(shaped[i * 3] - cx, shaped[i * 3 + 1] - cy, shaped[i * 3 + 2] - cz, hole.n, this.#round(i));
        out[i * 3] = cx + at[0]; out[i * 3 + 1] = cy + at[1]; out[i * 3 + 2] = cz + at[2];
      }
    }
    return out;
  }

  /**
   * Where one node is drawn now, given where every node is before any turn.
   * @param {Float32Array} shaped @param {number} i @returns {Vec3}
   */
  turned(shaped, i) {
    const galaxy = this.of[i], hole = this.hole;
    /** @type {Vec3} */ const at = [shaped[i * 3], shaped[i * 3 + 1], shaped[i * 3 + 2]];
    if (hole && this.fallen[i]) {
      const [cx, cy, cz] = hole.centre, round = Shaper.about(at[0] - cx, at[1] - cy, at[2] - cz, hole.n, this.#round(i));
      return [cx + round[0], cy + round[1], cz + round[2]];
    }
    if (!galaxy || !galaxy.angle) return at;
    const cos = Math.cos(galaxy.angle), sin = Math.sin(galaxy.angle), [nx, ny, nz] = galaxy.n;
    const sx = shaped[galaxy.sun * 3], sy = shaped[galaxy.sun * 3 + 1], sz = shaped[galaxy.sun * 3 + 2];
    const x = at[0] - sx, y = at[1] - sy, z = at[2] - sz, along = (nx * x + ny * y + nz * z) * (1 - cos);
    return [sx + x * cos + (ny * z - nz * y) * sin + nx * along, sy + y * cos + (nz * x - nx * z) * sin + ny * along,
            sz + z * cos + (nx * y - ny * x) * sin + nz * along];
  }
}
