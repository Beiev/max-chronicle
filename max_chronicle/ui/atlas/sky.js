// @ts-check
/**
 * What lies behind the memory: the night, the far sky that turns with the
 * camera, the globe the projects hang on, and around each project a nebula
 * of its colour and a hairline ring.
 *
 * The far sky is at no distance at all: only the camera's direction moves it,
 * so turning turns the stars and coming closer does not. Sliding the map turns
 * it a little too, or the flat map would hang over a painted backdrop. It
 * holds scattered dust, a denser band of it along one great circle, and a few
 * very faint clouds along that band.
 *
 * The globe is three great circles through the middle of the memory, as far
 * out as its outermost suns. A project's ring is the same idea at its own
 * scale: one great circle of the ball its memories form, in a plane of its
 * own. Of both, the half behind the middle is fainter, so a turn of the sky
 * reads as a turn. On the flat map they all lie in the plane.
 */

import { rgb, mulberry32, STARLIGHT } from "./model.js";

const DUST = 1000;
const BAND = 1500;
const FAR_CLOUDS = 8;
const SLIDE_TURN = 0.00009; // radians of far sky per world unit the target slides
const NIGHT = "#05070d";
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const CIRCLE_SEGMENTS = 96;
const RING_SEGMENTS = 64;
const ORBIT_MIN_NODES = 8;
const ORBIT_MIN_PX = 9;
const CLOUD_PX = 192;
/** A nebula reaches this many times further than the ring it is drawn around. */
const NEBULA_REACH = 2.3;
/** The far clouds: cold blues and violets, one green, one rose. */
const FAR_TINTS = ["#4f6fd0", "#6f5fc4", "#3f8fa6", "#8a5fb8", "#4a78c8", "#b0688f"];
/** The pole of the band of far dust. */
const BAND_POLE = unit([0.38, 0.84, -0.39]);

/**
 * @typedef {[number, number, number]} Vec3
 * @typedef {{centre: Vec3, radius: number}} Globe the middle of the memory and how far out it reaches
 * @typedef {{x: number, y: number, r: number}} Ring where the ball of a project was last drawn, in pixels
 * @typedef {{cloud: HTMLCanvasElement, u: Vec3, v: Vec3}} Dress what a project wears whatever the layout: its nebula and the plane of its ring
 */

/** @param {number[]} v @returns {Vec3} */
function unit(v) {
  const length = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / length, v[1] / length, v[2] / length];
}

/** Two directions at right angles to a pole and to each other. @param {Vec3} pole @returns {[Vec3, Vec3]} */
function planeOf(pole) {
  const aside = Math.abs(pole[1]) < 0.9 ? [0, 1, 0] : [1, 0, 0];
  const u = unit([aside[1] * pole[2] - aside[2] * pole[1], aside[2] * pole[0] - aside[0] * pole[2], aside[0] * pole[1] - aside[1] * pole[0]]);
  return [u, [pole[1] * u[2] - pole[2] * u[1], pole[2] * u[0] - pole[0] * u[2], pole[0] * u[1] - pole[1] * u[0]]];
}

/** @param {() => number} random @returns {Vec3} a direction, any as likely as another */
function anywhere(random) {
  const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
  return [Math.cos(around) * ring, up, Math.sin(around) * ring];
}

/**
 * A cloud to draw as a nebula: soft lobes in a few tints, lying along a lean
 * of its own, fading to nothing before the edge of its square.
 * @param {() => number} random @param {number[][]} tints red, green and blue of each, 0 to 255
 */
function cloud(random, tints) {
  const canvas = document.createElement("canvas");
  canvas.width = canvas.height = CLOUD_PX;
  const paint = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
  paint.globalCompositeOperation = "lighter";
  const mid = CLOUD_PX / 2, lean = random() * Math.PI, flat = 0.45 + random() * 0.4;
  const lobes = 9 + Math.floor(random() * 6);
  for (let k = 0; k < lobes; k++) {
    const along = (random() * 2 - 1) * CLOUD_PX * 0.24, across = (random() * 2 - 1) * CLOUD_PX * 0.24 * flat;
    const x = mid + Math.cos(lean) * along - Math.sin(lean) * across, y = mid + Math.sin(lean) * along + Math.cos(lean) * across;
    const [r, g, b] = tints[k % tints.length];
    const lobe = paint.createRadialGradient(x, y, 0, x, y, CLOUD_PX * (0.1 + random() * 0.2));
    lobe.addColorStop(0, `rgba(${r}, ${g}, ${b}, ${(0.1 + random() * 0.12).toFixed(3)})`);
    lobe.addColorStop(1, `rgba(${r}, ${g}, ${b}, 0)`);
    paint.fillStyle = lobe;
    paint.fillRect(0, 0, CLOUD_PX, CLOUD_PX);
  }
  paint.globalCompositeOperation = "destination-in";
  const fade = paint.createRadialGradient(mid, mid, CLOUD_PX * 0.1, mid, mid, mid);
  fade.addColorStop(0, "rgba(0, 0, 0, 1)");
  fade.addColorStop(1, "rgba(0, 0, 0, 0)");
  paint.fillStyle = fade;
  paint.fillRect(0, 0, CLOUD_PX, CLOUD_PX);
  return canvas;
}

/** @param {string} hex @param {number[]} toward 0 to 255 @param {number} t @returns {number[]} 0 to 255 */
const tint = (hex, toward, t) => rgb(hex).map((v, k) => Math.round(v * 255 + (toward[k] - v * 255) * t));

export class Sky {
  /**
   * @param {HTMLCanvasElement} canvas @param {import("./model.js").Graph} graph
   */
  constructor(canvas, graph) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.graph = graph;
    const random = mulberry32(20261110);
    const [across, along] = planeOf(BAND_POLE);
    /** @param {Vec3} at @param {boolean} inBand */
    const speck = (at, inBand) => ({
      x: at[0], y: at[1], z: at[2], r: 0.3 + random() * random() * (inBand ? 0.7 : 1.0),
      a: (inBand ? 0.04 : 0.05) + random() * random() * (inBand ? 0.3 : 0.42), f: 0.3 + random() * 1.2, ph: random() * 6.28,
    });
    /** A direction near the band: anywhere along it, a little off it, mostly less. */
    const inBand = (/** @type {number} */ spread) => {
      const turn = random() * Math.PI * 2, off = (random() + random() + random() - 1.5) * spread;
      return unit([0, 1, 2].map((k) => Math.cos(turn) * across[k] + Math.sin(turn) * along[k] + BAND_POLE[k] * off));
    };
    this.dust = [
      ...Array.from({ length: DUST }, () => speck(anywhere(random), false)),
      ...Array.from({ length: BAND }, () => speck(inBand(0.3), true)),
    ];
    /** Clouds too far to come closer to: most along the band, a couple anywhere. */
    this.far = Array.from({ length: FAR_CLOUDS }, (_, k) => {
      const at = k < FAR_CLOUDS - 2 ? inBand(0.22) : anywhere(random), hue = FAR_TINTS[k % FAR_TINTS.length];
      return { x: at[0], y: at[1], z: at[2], reach: 0.4 + random() * 0.4, a: 0.1 + random() * 0.1,
               cloud: cloud(random, [tint(hue, [0, 0, 0], 0), tint(hue, [40, 60, 140], 0.4), tint(hue, [0, 0, 0], 0.3)]) };
    });
    /** @type {{x: number, y: number, z: number, r: number, color: string, p: number, total: number, dress: Dress}[]} */
    this.orbits = [];
    /** @type {Map<number, Dress>} */ this.dresses = new Map();
    /** Where the ball of each project was drawn this frame, for the names that sit under it. @type {Map<number, Ring>} */
    this.rings = new Map();
    /** @type {Map<number, number>} */ this.bornShare = new Map();
    this.W = 0; this.H = 0; this.dpr = 1;
  }

  /** @param {number} W @param {number} H @param {number} dpr */
  resize(W, H, dpr) {
    this.W = W; this.H = H; this.dpr = dpr;
    this.canvas.width = Math.round(W * dpr);
    this.canvas.height = Math.round(H * dpr);
  }

  /** The nebula and the ring plane of a project: made once, the same on every visit. @param {number} project @returns {Dress} */
  #dress(project) {
    let dress = this.dresses.get(project);
    if (!dress) {
      const random = mulberry32(7000 + project * 31), hue = this.graph.colorOf(project);
      const [u, v] = planeOf(anywhere(random));
      dress = { u, v, cloud: cloud(random, [tint(hue, [0, 0, 0], 0), tint(hue, rgb(STARLIGHT).map((c) => c * 255), 0.3), tint(hue, [110, 100, 215], 0.4)]) };
      this.dresses.set(project, dress);
    }
    return dress;
  }

  /** How far the memories of each project of eight nodes or more keep from its sun. @param {Float32Array} positions */
  updateOrbits(positions) {
    const graph = this.graph, nodes = graph.nodes, sums = new Map();
    for (let i = 0; i < nodes.length; i++) {
      const p = nodes[i].p, sun = p === null ? undefined : graph.projectNode.get(p);
      if (p === null || sun === undefined || sun === i) continue;
      const s = sums.get(p) || { spread: 0, count: 0 };
      const dx = positions[i * 3] - positions[sun * 3], dy = positions[i * 3 + 1] - positions[sun * 3 + 1],
            dz = positions[i * 3 + 2] - positions[sun * 3 + 2];
      s.spread += dx * dx + dy * dy + dz * dz; s.count++;
      sums.set(p, s);
    }
    this.orbits = [...sums.entries()].filter(([, s]) => s.count >= ORBIT_MIN_NODES).map(([p, s]) => {
      const sun = /** @type {number} */ (graph.projectNode.get(p));
      const [r, g, b] = rgb(graph.colorOf(p)).map((v) => Math.round(v * 255));
      return { x: positions[sun * 3], y: positions[sun * 3 + 1], z: positions[sun * 3 + 2],
               r: Math.sqrt(s.spread / s.count) * 1.1 + 4, color: `${r}, ${g}, ${b}`, p, total: s.count, dress: this.#dress(p) };
    });
  }

  /** How much of each project exists on the day the time machine shows. @param {number} play */
  updateBornShare(play) {
    const counts = new Map(), nodes = this.graph.nodes, birth = this.graph.birth;
    for (let i = 0; i < nodes.length; i++) {
      if (nodes[i].p !== null && nodes[i].k !== "project" && birth[i] <= play) counts.set(nodes[i].p, (counts.get(nodes[i].p) || 0) + 1);
    }
    for (const orbit of this.orbits) this.bornShare.set(orbit.p, (counts.get(orbit.p) || 0) / orbit.total);
  }

  /**
   * @param {number} time seconds @param {import("./camera.js").Camera} camera
   * @param {{focus: number, travel: number, isolate: number | null, globe: Globe | null}} view focus and travel are eased 0..1
   */
  draw(time, camera, view) {
    const ctx = this.ctx, W = this.W, H = this.H;
    ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    ctx.globalCompositeOperation = "source-over";
    ctx.globalAlpha = 1;
    ctx.fillStyle = NIGHT;
    ctx.fillRect(0, 0, W, H);
    // The night is a shade lighter where the memory is, so the edges of the window fall away.
    const lift = ctx.createRadialGradient(camera.frame.cx, camera.frame.cy, 0, camera.frame.cx, camera.frame.cy, Math.max(W, H) * 0.7);
    lift.addColorStop(0, "rgba(20, 27, 50, 0.6)");
    lift.addColorStop(1, "rgba(5, 7, 13, 0)");
    ctx.fillStyle = lift;
    ctx.fillRect(0, 0, W, H);

    // The far sky sees the camera's direction, plus a slight turn for every slide of the target.
    const yaw = camera.yaw + (camera.target[0] * camera.right[0] + camera.target[2] * camera.right[2]) * SLIDE_TURN;
    const pitch = camera.pitch + camera.target[1] * SLIDE_TURN;
    const cp = Math.cos(pitch), sp = Math.sin(pitch), cy = Math.cos(yaw), sy = Math.sin(yaw);
    const focal = camera.focal * 0.62, cx = W / 2, cyc = H / 2;
    /** Where a direction of the far sky is on the screen: x, y and how much a radian spans there; null behind the reader. */
    const farAt = (/** @type {{x: number, y: number, z: number}} */ d) => {
      const depth = -(cp * sy * d.x + sp * d.y + cp * cy * d.z);
      if (depth < 0.12) return null;
      return [cx + ((cy * d.x - sy * d.z) / depth) * focal, cyc - ((-sp * sy * d.x + cp * d.y - sp * cy * d.z) / depth) * focal, focal / depth];
    };
    const dim = 1 - 0.7 * view.focus;
    ctx.globalCompositeOperation = "lighter";
    for (const far of this.far) {
      const at = farAt(far);
      if (!at) continue;
      const r = Math.min(far.reach * at[2], Math.max(W, H) * 1.2);
      if (at[0] + r < 0 || at[0] - r > W || at[1] + r < 0 || at[1] - r > H) continue;
      ctx.globalAlpha = far.a * (0.6 + 0.4 * dim);
      ctx.drawImage(far.cloud, at[0] - r, at[1] - r, r * 2, r * 2);
    }
    ctx.globalAlpha = 1;
    for (const star of this.dust) {
      const at = farAt(star);
      if (!at || at[0] < -2 || at[0] > W + 2 || at[1] < -2 || at[1] > H + 2) continue;
      const a = star.a * (0.8 + 0.2 * Math.sin(time * star.f + star.ph));
      ctx.fillStyle = `rgba(${INK}, ${a.toFixed(3)})`;
      ctx.fillRect(at[0], at[1], star.r, star.r);
    }

    ctx.globalCompositeOperation = "source-over";
    if (view.globe) {
      ctx.lineWidth = 1;
      const [u, v, w] = /** @type {Vec3[]} */ ([[1, 0, 0], [0, 1, 0], [0, 0, 1]]);
      const { centre, radius } = view.globe;
      /** @type {[Vec3, Vec3, number][]} */
      const circles = camera.flat ? [[u, v, radius], [u, v, radius / 2]] : [[u, w, radius], [u, v, radius], [w, v, radius]];
      for (const [a, b, r] of circles) this.#circle(camera, centre, a, b, r, CIRCLE_SEGMENTS, `rgba(${INK}, ${(0.085 * dim).toFixed(3)})`, `rgba(${INK}, ${(0.035 * dim).toFixed(3)})`);
    }
    this.#projects(camera, view, dim);
    ctx.globalAlpha = 1;
    ctx.globalCompositeOperation = "source-over";
  }

  /**
   * A circle in space, the half behind its middle in a fainter stroke.
   * @param {import("./camera.js").Camera} camera @param {Vec3} centre @param {Vec3} u @param {Vec3} v two axes of its plane
   * @param {number} radius @param {number} segments @param {string} near @param {string} behind strokes of the two halves
   */
  #circle(camera, centre, u, v, radius, segments, near, behind) {
    const ctx = this.ctx, at = [0, 0, 0];
    const middle = camera.project(centre[0], centre[1], centre[2], [0, 0, 0]);
    const here = middle ? middle[2] : 0;  // pixels per world unit at the middle: less than this is further away
    for (const far of camera.flat ? [false] : [true, false]) {
      ctx.beginPath();
      let pen = false;
      for (let step = 0; step <= segments; step++) {
        const turn = (step / segments) * Math.PI * 2, c = Math.cos(turn) * radius, s = Math.sin(turn) * radius;
        const seen = camera.project(centre[0] + u[0] * c + v[0] * s, centre[1] + u[1] * c + v[1] * s, centre[2] + u[2] * c + v[2] * s, at);
        if (!seen || (!camera.flat && (at[2] < here) !== far)) { pen = false; continue; }
        if (pen) ctx.lineTo(at[0], at[1]); else { ctx.moveTo(at[0], at[1]); pen = true; }
      }
      ctx.strokeStyle = far ? behind : near;
      ctx.stroke();
    }
  }

  /**
   * The nebula of each project, then its ring.
   * @param {import("./camera.js").Camera} camera
   * @param {{travel: number, isolate: number | null}} view @param {number} dim
   */
  #projects(camera, view, dim) {
    const ctx = this.ctx, W = this.W, H = this.H, at = [0, 0, 0];
    const here = camera.focal / camera.dist, window = Math.min(W, H);
    this.rings.clear();
    /** @type {{orbit: Sky["orbits"][number], alpha: number}[]} */ const ringed = [];
    ctx.globalCompositeOperation = "lighter";
    for (const orbit of this.orbits) {
      if (!camera.project(orbit.x, orbit.y, orbit.z, at)) continue;
      const x = at[0], y = at[1], r = orbit.r * at[2], reach = r * NEBULA_REACH;
      if (x + reach < 0 || x - reach > W || y + reach < 0 || y - reach > H) continue;
      const share = view.travel > 0.01 ? (this.bornShare.get(orbit.p) || 0) : 1;
      if (share <= 0) continue;
      if (r >= ORBIT_MIN_PX) this.rings.set(orbit.p, { x, y, r });
      const lit = view.isolate === null || view.isolate === orbit.p ? 1 : 0.3;
      // What is further than the point in view is fainter, as the lights are.
      const depth = Math.min(1, Math.max(0.35, at[2] / here));
      const strength = dim * lit * Math.sqrt(share) * depth;
      // A nebula the reader is inside of is a haze, not a wall: it thins as it outgrows the window.
      ctx.globalAlpha = Math.min(1, 0.5 * strength * Math.max(0.3, Math.min(1, window / reach)));
      ctx.drawImage(orbit.dress.cloud, x - reach, y - reach, reach * 2, reach * 2);
      // A ring wider than the window is no ring any more.
      const within = Math.min(1, (window * 0.6) / r);
      if (r >= ORBIT_MIN_PX && strength * within > 0.05) ringed.push({ orbit, alpha: strength * within * within });
    }
    ctx.globalAlpha = 1;
    ctx.globalCompositeOperation = "source-over";
    ctx.lineWidth = 0.6;
    const flatU = /** @type {Vec3} */ ([1, 0, 0]), flatV = /** @type {Vec3} */ ([0, 1, 0]);
    for (const { orbit, alpha } of ringed) {
      this.#circle(camera, [orbit.x, orbit.y, orbit.z], camera.flat ? flatU : orbit.dress.u, camera.flat ? flatV : orbit.dress.v,
                   orbit.r, RING_SEGMENTS, `rgba(${orbit.color}, ${(0.42 * alpha).toFixed(3)})`, `rgba(${orbit.color}, ${(0.16 * alpha).toFixed(3)})`);
    }
  }
}
