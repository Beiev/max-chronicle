// @ts-check
/**
 * What glows in the sky and is not a memory: the gas of each galaxy, the
 * bridges between galaxies, and the specks of dust in the room they hang in.
 *
 * The gas is a few dozen soft lights to a galaxy, each hung on one of its
 * memories: spaced through them by age, so the gas lies where the memories
 * lie, turns as they turn, and comes with them as the time machine plays.
 * Being lights in space and not a picture laid on a plane, they give a galaxy
 * its body: from the side a disc is a spindle with a bright heart.
 *
 * Its colours are those of real galaxies, not of a chart. The heart is the
 * cream of old stars, the inner disc gold, the arms the blue-white of young
 * ones, a task a knot of rose as the places where stars are born; a galaxy
 * gone quiet is warm all through, cream to amber. Along the arms of a spiral
 * lie lanes of dark dust that cover what is behind them. Over all of that a
 * galaxy wears a tone of its project's own colour, enough to know it by.
 *
 * Through the gas lies the dust of stars: thousands of points too faint to be
 * anyone's memory, scattered close about the memories, warm near the heart
 * and blue-white along the arms. They are what makes a galaxy a crowd of
 * stars and not a fog. A memory is always the larger light: it has a shape,
 * a name, and answers the pointer; the dust does none of that.
 *
 * A bridge joins two galaxies whose memories are joined: by links written in
 * notes, and by resemblance in meaning. The more they share, the brighter the
 * bridge. A slow light travels along it.
 *
 * The specks are scattered through the room. Nothing in them is a memory and
 * none can be pointed at: they pass in front of the galaxies and behind them
 * when the sky turns.
 *
 * No DOM and no drawing here: this module says where the lights are and what
 * they look like; the renderer draws them, and node:test reads them too.
 */

import { rgb, mulberry32, clamp, STARLIGHT } from "./model.js";

/** @typedef {[number, number, number]} Vec3 */
/** @typedef {{start: number, count: number}} Range a run of lights, as the renderer draws them */

const CREAM = /** @type {Vec3} */ ([1.0, 0.94, 0.82]);
const GOLD = /** @type {Vec3} */ ([1.0, 0.84, 0.62]);
const AMBER = /** @type {Vec3} */ ([1.0, 0.74, 0.5]);
const ICE = /** @type {Vec3} */ ([0.76, 0.85, 1.0]);
const AZURE = /** @type {Vec3} */ ([0.6, 0.73, 1.0]);
const ROSE = /** @type {Vec3} */ ([1.0, 0.64, 0.72]);
const UMBER = /** @type {Vec3} */ ([0.05, 0.035, 0.035]);
const DUSK = /** @type {Vec3} */ ([0.3, 0.34, 0.56]);
/** How much of its project's colour a galaxy's gas wears. */
const TONE = 0.26;
const SPECKS = 1500;
/** The specks reach this many times further out than the suns do. */
const SPECK_REACH = 1.35;
const BRIDGES = 40;
const BEADS = 11;
/** Two galaxies are bridged when their memories are joined at least this much: a link counts one, a resemblance half. */
const BRIDGED_FROM = 2;
/** Floats per light in what never changes (red, green, blue, strength, share of the reach, seed, most pixels) and in what does (x, y, z, radius, how much shows). */
export const LOOK = 7;
export const AT = 5;
/** A light of gas may be as large on the screen as it comes; a speck stays a speck. */
const NO_CAP = 1e6;
const SPECK_PX = 1.7;
/** Points of star dust to a memory, and the fewest and the most a galaxy has. */
const DUST_PER_MEMORY = 2.4;
const DUST_LEAST = 90;
const DUST_MOST = 1400;
const DUST_PX = 1.45;

/** @param {Vec3} c @param {Vec3} d @param {number} t @returns {Vec3} */
const mix = (c, d, t) => [c[0] + (d[0] - c[0]) * t, c[1] + (d[1] - c[1]) * t, c[2] + (d[2] - c[2]) * t];
/** @param {number} lo @param {number} hi @param {number} x */
const smooth = (lo, hi, x) => { const t = clamp((x - lo) / (hi - lo), 0, 1); return t * t * (3 - 2 * t); };

export class Gas {
  /**
   * @param {{nodes: {k: string, p: number | null}[], birth: Float32Array, colorOf: (p: number | null) => string,
   *   edges: {s: number, t: number, k: number, w: number}[], projectNode: Map<number, number>}} graph
   * @param {import("./shape.js").Shaper} shaper
   */
  constructor(graph, shaper) {
    this.graph = graph;
    /**
     * Per light that hangs on a node: the node, its galaxy, how large it is as a share of the galaxy's reach, how far
     * off its node it hangs (in reaches: of gas any way in space, of dust along the plane of the galaxy and off it),
     * and how far toward the sun it is drawn.
     */
    /** @type {number[]} */ const anchor = [];
    /** @type {import("./shape.js").Galaxy[]} */ const home = [];
    /** @type {number[]} */ const share = [];
    /** @type {number[]} */ const off = [];
    /** @type {number[]} */ const inward = [];
    /** @type {number[]} */ const inPlane = [];
    /** @type {number[]} */ const look = [];
    /** The lights of each galaxy, by project: the renderer draws a galaxy's gas and then its memories. @type {Map<number, Range>} */
    this.ranges = new Map();
    for (const [project, galaxy] of shaper.galaxies) {
      const random = mulberry32(4242 + project * 97), hue = rgb(graph.colorOf(project)), start = anchor.length;
      const toned = (/** @type {Vec3} */ natural, tone = TONE) => mix(natural, hue, tone);
      const old = galaxy.form === "ball" || galaxy.form === "cluster", armed = galaxy.arms > 0;
      /**
       * @param {number} node @param {number} size @param {Vec3} tint @param {number} strength @param {number} drift
       * @param {number} [toSun] @param {boolean} [wisp] whether it carries the noise that tears a cloud; the halo and
       *   the heart are plain light
       */
      const hang = (node, size, tint, strength, drift, toSun = 0, wisp = true) => {
        anchor.push(node); home.push(galaxy); share.push(size); inward.push(toSun); inPlane.push(0);
        const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
        off.push(Math.cos(around) * ring * drift, up * drift, Math.sin(around) * ring * drift);
        look.push(tint[0], tint[1], tint[2], strength, size, wisp ? random() : -1, NO_CAP);
      };
      // the halo, behind everything of the galaxy
      hang(galaxy.sun, 1.25, mix(DUSK, hue, 0.4), 0.075, 0, 0, false);
      const members = galaxy.byAge, count = clamp(Math.round(Math.sqrt(members.length) * 2.4), 10, 56);
      /** @type {number[]} */ const lanes = [];
      for (let k = 0; k < count; k++) {
        const at = Math.floor(((k + 0.5) / count) * members.length), node = members[at], age = at / Math.max(members.length - 1, 1);
        if (graph.nodes[node].k === "task" && !old) { hang(node, 0.11 + random() * 0.06, toned(ROSE, 0.18), 0.24, 0.02); continue; }
        // gold near the heart, blue-white toward the rim; a galaxy gone quiet is cream to amber all through
        const natural = old ? mix(CREAM, AMBER, random() * 0.7)
          : galaxy.disc ? mix(mix(GOLD, ICE, smooth(0.12, 0.55, age)), AZURE, random() * 0.5 * smooth(0.3, 0.8, age))
          : mix(ICE, AZURE, random());
        hang(node, 0.18 + random() * 0.2, toned(natural, old ? 0.2 : TONE), 0.13 + random() * 0.11, 0.03 + random() * 0.07);
        if (armed && age > 0.2 && k % 3 === 0) lanes.push(node);
      }
      // lanes of dark dust on the inner side of the arms: they cover the gas behind them
      for (const node of lanes) hang(node, 0.09 + random() * 0.06, UMBER, 0.2 + random() * 0.1, 0.01, 0.13);
      // the heart: a wide warm glow, and its hot middle
      hang(galaxy.sun, old ? 0.52 : 0.36, toned(GOLD, 0.18), old ? 0.34 : 0.28, 0, 0, false);
      hang(galaxy.sun, 0.15, toned(CREAM, 0.08), 0.42, 0, 0, false);
      this.ranges.set(project, { start, count: anchor.length - start });
    }
    // the dust of stars, galaxy by galaxy: each point close by one memory, lying as the galaxy lies
    /** The dust of each galaxy, by project: drawn sharp, over its gas and under its memories. @type {Map<number, Range>} */
    this.dust = new Map();
    for (const [project, galaxy] of shaper.galaxies) {
      const random = mulberry32(5151 + project * 61), hue = rgb(graph.colorOf(project)), start = anchor.length;
      const old = galaxy.form === "ball" || galaxy.form === "cluster", members = galaxy.byAge;
      const count = clamp(Math.round(members.length * DUST_PER_MEMORY), DUST_LEAST, DUST_MOST);
      for (let k = 0; k < count; k++) {
        const at = Math.floor(random() * members.length), age = at / Math.max(members.length - 1, 1);
        const natural = old ? mix(CREAM, AMBER, random() * 0.8) : mix(mix(GOLD, ICE, smooth(0.1, 0.6, age)), AZURE, random() * 0.6 * smooth(0.3, 0.9, age));
        const tint = mix(natural, hue, 0.16);
        // close about its memory: mostly very close, a few further; a disc keeps its dust near its plane
        const far = 0.012 + 0.1 * random() * random(), around = random() * Math.PI * 2, lift = (random() + random() - 1) * (galaxy.disc ? 0.03 : far);
        anchor.push(members[at]); home.push(galaxy); share.push(0.006 + random() * 0.006); inward.push(0); inPlane.push(1);
        off.push(Math.cos(around) * far, Math.sin(around) * far, lift);
        // many faint, few bright
        look.push(tint[0], tint[1], tint[2], 0.16 + 0.7 * Math.pow(random(), 2.6), 1, -1, DUST_PX);
      }
      this.dust.set(project, { start, count: anchor.length - start });
    }
    this.motes = anchor.length;
    this.anchor = Int32Array.from(anchor);
    this.home = home;
    this.share = Float32Array.from(share);
    this.off = Float32Array.from(off);
    this.inward = Float32Array.from(inward);
    this.inPlane = Uint8Array.from(inPlane);

    // the bridges: the pairs of galaxies whose memories are joined the most
    /** @type {Map<string, {a: number, b: number, weight: number}>} */ const pairs = new Map();
    for (const e of graph.edges) {
      const pa = graph.nodes[e.s].p, pb = graph.nodes[e.t].p;
      if (pa === null || pb === null || pa === pb || !graph.projectNode.has(pa) || !graph.projectNode.has(pb)) continue;
      const a = Math.min(pa, pb), b = Math.max(pa, pb), key = `${a}:${b}`, pair = pairs.get(key) || { a, b, weight: 0 };
      pair.weight += e.k === 4 ? 0.5 : 1;
      pairs.set(key, pair);
    }
    const light = rgb(STARLIGHT), random = mulberry32(20261111);
    /** @type {{a: number, b: number, pa: number, pb: number, bow: Vec3, phase: number}[]} */
    this.bridges = [...pairs.values()].filter((pair) => pair.weight >= BRIDGED_FROM).sort((p, q) => q.weight - p.weight || p.a - q.a || p.b - q.b)
      .slice(0, BRIDGES).map((pair) => {
        const tint = mix(mix(rgb(graph.colorOf(pair.a)), rgb(graph.colorOf(pair.b)), 0.5), ICE, 0.62);
        const strength = clamp(0.035 + Math.log2(pair.weight) * 0.012, 0.035, 0.1);
        for (let k = 0; k < BEADS; k++) look.push(tint[0], tint[1], tint[2], strength, 1, random(), NO_CAP);
        const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
        return { a: /** @type {number} */ (graph.projectNode.get(pair.a)), b: /** @type {number} */ (graph.projectNode.get(pair.b)),
                 pa: pair.a, pb: pair.b, bow: [Math.cos(around) * ring, up, Math.sin(around) * ring], phase: random() };
      });

    // the specks: anywhere in a ball, more of them toward the middle of it
    this.speck = new Float32Array(SPECKS * 3);
    for (let k = 0; k < SPECKS; k++) {
      const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up), far = Math.pow(random(), 0.45);
      this.speck.set([Math.cos(around) * ring * far, up * far, Math.sin(around) * ring * far], k * 3);
      const tint = mix(light, random() < 0.35 ? ICE : GOLD, random() * 0.4);
      look.push(tint[0], tint[1], tint[2], 0.12 + random() * random() * 0.6, 1, -1, SPECK_PX);
    }
    this.count = this.motes + this.bridges.length * BEADS + SPECKS;
    /** What lies between the galaxies and belongs to none: the bridges and the specks, drawn before any galaxy. @type {Range} */
    this.field = { start: this.motes, count: this.count - this.motes };
    /** What never changes of each light. */
    this.look = Float32Array.from(look);
    /** Where each light is this frame, how large, and how much of it shows. */
    this.at = new Float32Array(this.count * AT);
    /** How much of each galaxy exists on the day the time machine shows. @type {Map<number, number>} */
    this.born = new Map();
  }

  /** Counts how much of each galaxy exists on a day. @param {number} play */
  updateBorn(play) {
    for (const galaxy of new Set(this.home)) {
      let born = 0;
      for (const i of galaxy.members) if (this.graph.birth[i] <= play) born++;
      this.born.set(galaxy.project, born / galaxy.members.length);
    }
  }

  /**
   * Puts every light where it is this frame.
   * @param {Float32Array} drawn x, y, z per node, as drawn
   * @param {{time: number, play: number, flat: boolean, centre: Vec3, radius: number, beside: number,
   *   shows: (project: number) => number}} view time in seconds; play is the day the time machine shows; centre and
   *   radius are of the ball the suns hang in; shows says how much of a galaxy's gas shows (less beside a focus, or
   *   beside a project shown alone) and beside the same for the specks
   * @returns {Float32Array}
   */
  place(drawn, view) {
    const at = this.at, birth = this.graph.birth;
    for (let j = 0; j < this.motes; j++) {
      const node = this.anchor[j] * 3, galaxy = this.home[j], sun = galaxy.sun * 3, reach = galaxy.reach, o = j * AT, to = this.inward[j];
      let ox = this.off[j * 3] * reach, oy = this.off[j * 3 + 1] * reach, oz = this.off[j * 3 + 2] * reach;
      if (this.inPlane[j]) {
        // along the plane of the galaxy and off it, wherever that plane now lies
        const { u, v, n } = galaxy, a = ox, b = oy, c = oz;
        ox = u[0] * a + v[0] * b + n[0] * c; oy = u[1] * a + v[1] * b + n[1] * c; oz = u[2] * a + v[2] * b + n[2] * c;
      }
      at[o] = drawn[node] + (drawn[sun] - drawn[node]) * to + ox;
      at[o + 1] = drawn[node + 1] + (drawn[sun + 1] - drawn[node + 1]) * to + oy;
      at[o + 2] = drawn[node + 2] + (drawn[sun + 2] - drawn[node + 2]) * to + (view.flat ? 0 : oz);
      at[o + 3] = this.share[j] * reach;
      // The gas of a memory comes with the memory, over a day or two; the heart and the halo grow with the galaxy.
      const here = node === sun ? Math.sqrt(this.born.get(galaxy.project) ?? 1) : clamp((view.play - birth[this.anchor[j]]) / 1.5 + 0.35, 0, 1);
      at[o + 4] = here * view.shows(galaxy.project);
    }
    let o = this.motes * AT;
    for (const bridge of this.bridges) {
      const a = bridge.a * 3, b = bridge.b * 3;
      const dx = drawn[b] - drawn[a], dy = drawn[b + 1] - drawn[a + 1], dz = drawn[b + 2] - drawn[a + 2], far = Math.hypot(dx, dy, dz);
      // A bridge is there once both its galaxies are, and shows as much as the fainter of the two.
      const both = Math.min(view.shows(bridge.pa) * Math.sqrt(this.born.get(bridge.pa) ?? 1), view.shows(bridge.pb) * Math.sqrt(this.born.get(bridge.pb) ?? 1))
        * (birth[bridge.a] <= view.play && birth[bridge.b] <= view.play ? 1 : 0);
      for (let k = 0; k < BEADS; k++, o += AT) {
        const t = (k + 0.5) / BEADS, sag = Math.sin(Math.PI * t) * far * 0.1;
        at[o] = drawn[a] + dx * t + bridge.bow[0] * sag;
        at[o + 1] = drawn[a + 1] + dy * t + bridge.bow[1] * sag;
        at[o + 2] = drawn[a + 2] + dz * t + (view.flat ? 0 : bridge.bow[2] * sag);
        // thin in the middle, wide where it meets a galaxy; a slow light travels along it
        at[o + 3] = far * (0.05 + 0.05 * Math.abs(2 * t - 1));
        at[o + 4] = both * (0.55 + 0.45 * Math.sin(Math.PI * 2 * (t * 1.5 - view.time * 0.05 + bridge.phase)));
      }
    }
    const far = view.radius * SPECK_REACH, [cx, cy, cz] = view.centre, deep = view.flat ? 0.2 : 1;
    for (let k = 0; k < SPECKS; k++, o += AT) {
      at[o] = cx + this.speck[k * 3] * far;
      at[o + 1] = cy + this.speck[k * 3 + 1] * far;
      at[o + 2] = cz + this.speck[k * 3 + 2] * far * deep;
      at[o + 3] = 2;
      at[o + 4] = view.beside;
    }
    return at;
  }
}
