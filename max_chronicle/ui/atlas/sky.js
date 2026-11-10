// @ts-check
/**
 * What lies under the lights: the night itself, and the globe the galaxies
 * hang in.
 *
 * The night is a shade lighter where the memory is, so the edges of the window
 * fall away. The far sky (its stars, its band, its clouds) is drawn over it by
 * the renderer (far.js, render.js).
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

import { rgb, STARLIGHT } from "./model.js";

const NIGHT = "#05070d";
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const CIRCLE_SEGMENTS = 96;
/** A galaxy smaller than this on the screen is not a place to hang a name from. */
const BODY_MIN_PX = 9;

/**
 * @typedef {[number, number, number]} Vec3
 * @typedef {{centre: Vec3, radius: number}} Globe the middle of the memory and how far out it reaches
 * @typedef {{x: number, y: number, r: number, rx: number, ry: number}} Ring where a galaxy lies on the screen, in
 *   pixels: its middle, how far it reaches, and its half width and half height as it is turned to the reader
 */

export class Sky {
  /**
   * @param {HTMLCanvasElement} canvas @param {import("./shape.js").Shaper} shaper whose galaxies are measured for the names
   */
  constructor(canvas, shaper) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.shaper = shaper;
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
   * @param {number} _time seconds @param {import("./camera.js").Camera} camera
   * @param {{focus: number, turning: number, globe: Globe | null, positions: Float32Array, shown: (i: number) => boolean}} view
   *   focus and turning are eased 0..1; positions are the nodes as drawn; shown says whether a sun is on the map now
   */
  draw(_time, camera, view) {
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

    // With a memory in focus the globe steps back with the rest of the sky.
    const dim = 1 - 0.4 * view.focus;
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
