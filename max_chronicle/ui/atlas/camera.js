// @ts-check
/**
 * The Atlas camera: an orbit around a point of the memory.
 *
 * Drag turns the sky around the target, a second gesture slides the target,
 * the wheel moves closer or away. In the flat mode the camera looks straight
 * down the z axis at a map laid out with z = 0, and drag slides instead of
 * turning: the same projection, so nothing else has to know which mode it is.
 *
 * A point is projected the simple way a pinhole does it. In camera space
 * (x right, y up, the camera looking down -z) a point at depth d = -z lands at
 *     screen = centre + focal * (x, -y) / d
 * where focal is in pixels. The centre is not the middle of the window: the
 * panels cover its sides, and the map is framed in what they leave.
 *
 * Pure numbers, no DOM: the renderer, the labels and the picking all ask this
 * module where a point is, and node:test asks it too.
 */

const FOV = (40 * Math.PI) / 180;
const PITCH_MAX = 1.45;
const NEAR = 4;

/** @typedef {[number, number, number]} Vec3 */
/** @typedef {{cx: number, cy: number, w: number, h: number, height: number}} Frame the framed part of the window; height is the whole window's */

/** @param {number} v @param {number} lo @param {number} hi */
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
/** @param {number} t */
const ease = (t) => (t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2);
/** The shortest way round from angle a to angle b. @param {number} a @param {number} b */
const turn = (a, b) => { const d = (b - a) % (Math.PI * 2); return d > Math.PI ? d - Math.PI * 2 : d < -Math.PI ? d + Math.PI * 2 : d; };

export class Camera {
  constructor() {
    /** @type {Vec3} */ this.target = [0, 0, 0];
    this.yaw = 0;
    this.pitch = 0;
    this.dist = 1000;
    this.flat = false;
    /** @type {Frame} */ this.frame = { cx: 400, cy: 300, w: 800, h: 600, height: 600 };
    this.focal = 600 / 2 / Math.tan(FOV / 2);
    /** The distance at which the whole memory fits the frame. */
    this.fitDist = 1000;
    /** @type {{from: {target: Vec3, yaw: number, pitch: number, dist: number}, to: {target: Vec3, yaw: number, pitch: number, dist: number}, start: number, ms: number, dip: number} | null} */
    this.flight = null;
    this.spin = { yaw: 0, pitch: 0 };
    /** Camera axes in world space, kept current by update(). */
    this.right = /** @type {Vec3} */ ([1, 0, 0]);
    this.up = /** @type {Vec3} */ ([0, 1, 0]);
    this.back = /** @type {Vec3} */ ([0, 0, 1]);
    this.eye = /** @type {Vec3} */ ([0, 0, 1000]);
    /** World to camera, column-major, as WebGL takes it. */
    this.view = new Float32Array(16);
    this.update();
  }

  /** @param {Frame} frame */
  setFrame(frame) {
    this.frame = frame;
    this.focal = frame.height / 2 / Math.tan(FOV / 2);
  }

  /** Recomputes the axes and the matrix from target, yaw, pitch and dist. */
  update() {
    const cp = Math.cos(this.pitch), sp = Math.sin(this.pitch), cy = Math.cos(this.yaw), sy = Math.sin(this.yaw);
    /** @type {Vec3} */ const b = [cp * sy, sp, cp * cy];
    /** @type {Vec3} */ const r = [cy, 0, -sy];
    /** @type {Vec3} */ const u = [-sp * sy, cp, -sp * cy];
    this.back = b; this.right = r; this.up = u;
    const [tx, ty, tz] = this.target;
    /** @type {Vec3} */ const e = [tx + b[0] * this.dist, ty + b[1] * this.dist, tz + b[2] * this.dist];
    this.eye = e;
    const m = this.view;
    m[0] = r[0]; m[4] = r[1]; m[8] = r[2]; m[12] = -(r[0] * e[0] + r[1] * e[1] + r[2] * e[2]);
    m[1] = u[0]; m[5] = u[1]; m[9] = u[2]; m[13] = -(u[0] * e[0] + u[1] * e[1] + u[2] * e[2]);
    m[2] = b[0]; m[6] = b[1]; m[10] = b[2]; m[14] = -(b[0] * e[0] + b[1] * e[1] + b[2] * e[2]);
    m[3] = 0; m[7] = 0; m[11] = 0; m[15] = 1;
  }

  /** Pixels per world unit at the target: what "zoom" means here. */
  get scale() {
    return this.focal / this.dist;
  }

  /** How far in the view is, 1 when the whole memory fits. */
  get zoom() {
    return this.fitDist / this.dist;
  }

  /**
   * Where a world point is on the screen: [x, y, pixels per world unit there],
   * or null when it is behind the camera.
   * @param {number} x @param {number} y @param {number} z @param {Float32Array | number[]} [out]
   * @returns {Float32Array | number[] | null}
   */
  project(x, y, z, out = [0, 0, 0]) {
    const m = this.view;
    const depth = -(m[2] * x + m[6] * y + m[10] * z + m[14]);
    if (depth < NEAR) return null;
    const s = this.focal / depth;
    out[0] = this.frame.cx + (m[0] * x + m[4] * y + m[8] * z + m[12]) * s;
    out[1] = this.frame.cy - (m[1] * x + m[5] * y + m[9] * z + m[13]) * s;
    out[2] = s;
    return out;
  }

  /**
   * Every node at once, for the labels and the picking of one frame:
   * out[i*3..] = x, y, scale; scale 0 marks a point behind the camera.
   * @param {Float32Array} positions x, y, z per node @param {Float32Array} out
   */
  projectAll(positions, out) {
    const m = this.view, f = this.focal, cx = this.frame.cx, cy = this.frame.cy;
    for (let i = 0, n = positions.length / 3; i < n; i++) {
      const x = positions[i * 3], y = positions[i * 3 + 1], z = positions[i * 3 + 2];
      const depth = -(m[2] * x + m[6] * y + m[10] * z + m[14]);
      if (depth < NEAR) { out[i * 3 + 2] = 0; continue; }
      const s = f / depth;
      out[i * 3] = cx + (m[0] * x + m[4] * y + m[8] * z + m[12]) * s;
      out[i * 3 + 1] = cy - (m[1] * x + m[5] * y + m[9] * z + m[13]) * s;
      out[i * 3 + 2] = s;
    }
  }

  /**
   * The world point under a screen point, on the plane through the target
   * that faces the camera.
   * @param {number} sx @param {number} sy @returns {Vec3}
   */
  unproject(sx, sy) {
    const dx = (sx - this.frame.cx) / this.scale, dy = -(sy - this.frame.cy) / this.scale;
    const [tx, ty, tz] = this.target, r = this.right, u = this.up;
    return [tx + r[0] * dx + u[0] * dy, ty + r[1] * dx + u[1] * dy, tz + r[2] * dx + u[2] * dy];
  }

  /** A drag: turns the sky, or slides it in the flat mode. @param {number} dx @param {number} dy pixels */
  drag(dx, dy) {
    if (this.flat) { this.slide(dx, dy); return; }
    this.flight = null;
    this.yaw -= dx * 0.0055;
    this.pitch = clamp(this.pitch + dy * 0.0055, -PITCH_MAX, PITCH_MAX);
    this.update();
  }

  /** Slides the target so the sky follows the pointer. @param {number} dx @param {number} dy pixels */
  slide(dx, dy) {
    this.flight = null;
    const k = 1 / this.scale, r = this.right, u = this.up;
    for (let a = 0; a < 3; a++) this.target[a] += (-dx * r[a] + dy * u[a]) * k;
    this.update();
  }

  /**
   * Moves closer or away by a factor, keeping the world point under the
   * pointer where it is.
   * @param {number} factor below 1 comes closer @param {number} sx @param {number} sy
   */
  dolly(factor, sx, sy) {
    this.flight = null;
    const dist = clamp(this.dist * factor, this.fitDist / 60, this.fitDist * 6);
    const under = this.unproject(sx, sy), keep = dist / this.dist;
    for (let a = 0; a < 3; a++) this.target[a] = under[a] + (this.target[a] - under[a]) * keep;
    this.dist = dist;
    this.update();
  }

  /**
   * The distance from which a ball of this radius fits the frame.
   * @param {number} radius
   */
  distanceToFit(radius) {
    return (Math.max(radius, 1) * this.focal) / (Math.min(this.frame.w, this.frame.h) / 2);
  }

  /**
   * Starts a flight to a point and a distance; yaw and pitch stay unless given.
   * @param {Vec3} target @param {number} dist @param {number} now
   * @param {{yaw?: number, pitch?: number, ms?: number}} [options]
   */
  flyTo(target, dist, now, options = {}) {
    const to = { target: /** @type {Vec3} */ ([...target]), dist,
                 yaw: options.yaw ?? this.yaw, pitch: options.pitch ?? this.pitch };
    const far = Math.hypot(target[0] - this.target[0], target[1] - this.target[1], target[2] - this.target[2]);
    // A long way goes by a higher road: pull back in the middle of the flight.
    const dip = clamp(Math.log1p((far * Math.min(this.scale, this.focal / dist)) / Math.max(this.frame.w, 1)) * 0.9, 0, 1.4);
    this.flight = { from: { target: [...this.target], yaw: this.yaw, pitch: this.pitch, dist: this.dist },
                    to, start: now, ms: options.ms ?? 1100, dip };
    this.spin = { yaw: 0, pitch: 0 };
  }

  /** Jumps there at once. @param {Vec3} target @param {number} dist */
  jumpTo(target, dist) {
    this.flight = null;
    this.target = [...target];
    this.dist = dist;
    this.update();
  }

  /**
   * One frame of flight, or of the turn a released drag left behind.
   * @param {number} now ms @param {number} dt seconds @returns {boolean} whether the camera moved
   */
  step(now, dt) {
    // The flat map is looked at straight on. Whatever turn the sky had eases
    // away on its own: a flight, a jump or a wheel in the meantime cannot keep it.
    let levelling = false;
    if (this.flat && (this.yaw !== 0 || this.pitch !== 0)) {
      const k = 1 - Math.exp(-dt * 9);
      this.yaw += turn(this.yaw, 0) * k;
      this.pitch -= this.pitch * k;
      if (Math.abs(turn(this.yaw, 0)) + Math.abs(this.pitch) < 0.0005) { this.yaw = 0; this.pitch = 0; }
      levelling = true;
    }
    const f = this.flight;
    if (f) {
      const t = clamp((now - f.start) / f.ms, 0, 1), e = ease(t);
      for (let a = 0; a < 3; a++) this.target[a] = f.from.target[a] + (f.to.target[a] - f.from.target[a]) * e;
      if (!this.flat) {
        this.yaw = f.from.yaw + turn(f.from.yaw, f.to.yaw) * e;
        this.pitch = f.from.pitch + (f.to.pitch - f.from.pitch) * e;
      }
      this.dist = Math.exp(Math.log(f.from.dist) + (Math.log(f.to.dist) - Math.log(f.from.dist)) * e + f.dip * Math.sin(Math.PI * e));
      if (t >= 1) this.flight = null;
      this.update();
      return true;
    }
    if (this.flat) {
      this.spin = { yaw: 0, pitch: 0 };
      if (levelling) this.update();
      return levelling;
    }
    if (Math.abs(this.spin.yaw) + Math.abs(this.spin.pitch) > 0.00002) {
      this.yaw += this.spin.yaw * dt * 60;
      this.pitch = clamp(this.pitch + this.spin.pitch * dt * 60, -PITCH_MAX, PITCH_MAX);
      const fade = Math.pow(0.92, dt * 60);
      this.spin.yaw *= fade; this.spin.pitch *= fade;
      this.update();
      return true;
    }
    return false;
  }

  /** Flat mode looks straight at the map (step() levels the camera); leaving it keeps where the camera is. @param {boolean} flat */
  setFlat(flat) {
    this.flat = flat;
    this.spin = { yaw: 0, pitch: 0 };
  }
}
