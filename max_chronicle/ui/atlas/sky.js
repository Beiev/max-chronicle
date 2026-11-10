// @ts-check
/**
 * What lies behind the memory: the night, the far sky that turns with the
 * camera, and the globe the galaxies hang in.
 *
 * The far sky is at no distance at all: only the camera's direction moves it,
 * so turning turns the stars and coming closer does not. Sliding the map turns
 * it a little too, or the flat map would hang over a painted backdrop. It
 * holds scattered dust, a denser band of it along one great circle, a few
 * very faint clouds along that band, and far galaxies too small to be more
 * than a smudge.
 *
 * The globe is three great circles through the middle of the memory, as far
 * out as its outermost suns. It shows while the sky turns and all but goes
 * when it rests: of each circle the half behind the middle is fainter, so a
 * turn reads as a turn.
 *
 * The galaxies themselves are not drawn here: their gas and their memories are
 * lights in space (gas.js, render.js). This module only measures where each
 * one lies on the screen, for the names that hang by it.
 */

import { rgb, mulberry32, STARLIGHT } from "./model.js";
import { planeOf } from "./shape.js";

const DUST = 1100;
const BAND = 1700;
const FAR_CLOUDS = 8;
const FAR_GALAXIES = 70;
const SLIDE_TURN = 0.00009; // radians of far sky per world unit the target slides
const NIGHT = "#05070d";
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const CIRCLE_SEGMENTS = 96;
/** A galaxy smaller than this on the screen is not a place to hang a name from. */
const BODY_MIN_PX = 9;
const CLOUD_PX = 192;
/** The far clouds: cold blues and violets, one green, one rose. */
const FAR_TINTS = ["#4f6fd0", "#6f5fc4", "#3f8fa6", "#8a5fb8", "#4a78c8", "#b0688f"];
/** The pole of the band of far dust. */
const BAND_POLE = unit([0.38, 0.84, -0.39]);

/**
 * @typedef {[number, number, number]} Vec3
 * @typedef {{centre: Vec3, radius: number}} Globe the middle of the memory and how far out it reaches
 * @typedef {{x: number, y: number, r: number, rx: number, ry: number}} Ring where a galaxy lies on the screen, in
 *   pixels: its middle, how far it reaches, and its half width and half height as it is turned to the reader
 */

/** @param {number[]} v @returns {Vec3} */
function unit(v) {
  const length = Math.hypot(v[0], v[1], v[2]) || 1;
  return [v[0] / length, v[1] / length, v[2] / length];
}

/** @param {() => number} random @returns {Vec3} a direction, any as likely as another */
function anywhere(random) {
  const up = random() * 2 - 1, around = random() * Math.PI * 2, ring = Math.sqrt(1 - up * up);
  return [Math.cos(around) * ring, up, Math.sin(around) * ring];
}

/**
 * A cloud to draw far off: soft lobes in a few tints, lying along a lean
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
   * @param {HTMLCanvasElement} canvas @param {import("./shape.js").Shaper} shaper whose galaxies are measured for the names
   */
  constructor(canvas, shaper) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.shaper = shaper;
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
    /** Galaxies too far to be anyone's: a smudge each, longer than wide, a few pixels of it. */
    this.smudges = Array.from({ length: FAR_GALAXIES }, () => {
      const at = random() < 0.4 ? inBand(0.5) : anywhere(random), hue = FAR_TINTS[Math.floor(random() * FAR_TINTS.length)];
      return { x: at[0], y: at[1], z: at[2], long: 2.2 + random() * 4.2, flat: 0.25 + random() * 0.6, lean: random() * Math.PI,
               a: 0.1 + random() * 0.2, color: tint(hue, [236, 232, 221], 0.45 + random() * 0.4).join(", ") };
    });
    /** Where each galaxy lies on the screen this frame, for the names that hang by it. @type {Map<number, Ring>} */
    this.rings = new Map();
    this.W = 0; this.H = 0; this.dpr = 1;
  }

  /** @param {number} W @param {number} H @param {number} dpr */
  resize(W, H, dpr) {
    this.W = W; this.H = H; this.dpr = dpr;
    this.canvas.width = Math.round(W * dpr);
    this.canvas.height = Math.round(H * dpr);
  }

  /**
   * @param {number} time seconds @param {import("./camera.js").Camera} camera
   * @param {{focus: number, turning: number, globe: Globe | null, positions: Float32Array, shown: (i: number) => boolean}} view
   *   focus and turning are eased 0..1; positions are the nodes as drawn; shown says whether a sun is on the map now
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
    // With a memory in focus the rest of the sky steps back, and stays a sky.
    const dim = 1 - 0.4 * view.focus;
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
    for (const smudge of this.smudges) {
      const at = farAt(smudge);
      if (!at || at[0] < -8 || at[0] > W + 8 || at[1] < -8 || at[1] > H + 8) continue;
      ctx.fillStyle = `rgba(${smudge.color}, ${(smudge.a * dim).toFixed(3)})`;
      ctx.beginPath();
      ctx.ellipse(at[0], at[1], smudge.long, smudge.long * smudge.flat, smudge.lean, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = `rgba(${smudge.color}, ${(smudge.a * 1.6 * dim).toFixed(3)})`;
      ctx.fillRect(at[0] - 0.5, at[1] - 0.5, 1, 1);
    }
    for (const star of this.dust) {
      const at = farAt(star);
      if (!at || at[0] < -2 || at[0] > W + 2 || at[1] < -2 || at[1] > H + 2) continue;
      const a = star.a * (0.8 + 0.2 * Math.sin(time * star.f + star.ph));
      ctx.fillStyle = `rgba(${INK}, ${a.toFixed(3)})`;
      ctx.fillRect(at[0], at[1], star.r, star.r);
    }

    ctx.globalCompositeOperation = "source-over";
    if (view.globe) {
      ctx.lineWidth = 0.6;
      const [u, v, w] = /** @type {Vec3[]} */ ([[1, 0, 0], [0, 1, 0], [0, 0, 1]]);
      const { centre, radius } = view.globe, shows = dim * (0.2 + 0.8 * view.turning);
      /** @type {[Vec3, Vec3, number][]} */
      const circles = camera.flat ? [[u, v, radius], [u, v, radius / 2]] : [[u, w, radius], [u, v, radius], [w, v, radius]];
      for (const [a, b, r] of circles) this.#circle(camera, centre, a, b, r, CIRCLE_SEGMENTS, `rgba(${INK}, ${(0.11 * shows).toFixed(3)})`, `rgba(${INK}, ${(0.045 * shows).toFixed(3)})`);
    }
    this.#measure(camera, view);
    ctx.globalAlpha = 1;
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
   * Where each galaxy lies on the screen: a disc as it is turned to the reader, never thinner than it is tall.
   * @param {import("./camera.js").Camera} camera
   * @param {{positions: Float32Array, shown: (i: number) => boolean}} view
   */
  #measure(camera, view) {
    const at = [0, 0, 0], right = camera.right, up = camera.up, positions = view.positions;
    this.rings.clear();
    for (const [project, galaxy] of this.shaper.galaxies) {
      const sun = galaxy.sun * 3;
      if (!view.shown(galaxy.sun) || !camera.project(positions[sun], positions[sun + 1], positions[sun + 2], at)) continue;
      const scale = at[2], r = galaxy.reach * scale, tall = galaxy.tall * scale;
      if (r < BODY_MIN_PX) continue;
      const { u, v } = galaxy;
      const ux = u[0] * right[0] + u[1] * right[1] + u[2] * right[2], uy = u[0] * up[0] + u[1] * up[1] + u[2] * up[2];
      const vx = v[0] * right[0] + v[1] * right[1] + v[2] * right[2], vy = v[0] * up[0] + v[1] * up[1] + v[2] * up[2];
      this.rings.set(project, { x: at[0], y: at[1], r, rx: Math.max(Math.hypot(ux, vx) * r, tall), ry: Math.max(Math.hypot(uy, vy) * r, tall) });
    }
  }
}
