// @ts-check
/**
 * The time machine's river: how much each project recorded per day, as a
 * centred streamgraph, with two handles on it.
 *
 * The right handle is the playhead: the sky shows what was known on that day.
 * The left handle is where the range starts: memories older than it go dark,
 * so the two together frame a stretch of time. Dragging anywhere else moves
 * the playhead. The wheel zooms the axis around the pointer, a sideways scroll
 * slides it, a double click shows all of time again.
 */

import { rgb, STARLIGHT, BRASS } from "./model.js";

const DAY = 86400;
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const STREAMS = 9;
const SMOOTH_DAYS = 2.2;
const PAD_X = 14;
const AXIS = 16;
const GRIP_PX = 9;
const MIN_VIEW_DAYS = 5;

/**
 * @typedef {{t: number, p: number | null}} Moment
 * @typedef {{span: [number, number], moments: Moment[], color: (project: number | null) => string,
 *   onScrub: (day: number) => void, onRange: (from: number) => void}} TimelineOptions
 */

/**
 * The moment a point of the axis stands for. The axis counts whole days from
 * the first memory, so its end lies up to a day past the last one; a moment
 * on it never goes past the last memory.
 * @param {[number, number]} span @param {number} day
 */
export function momentAt(span, day) {
  return Math.min(span[0] + day * DAY, span[1]);
}

/**
 * The part of the axis in view after a zoom around a point of it, never
 * narrower than a few days and never outside the memory's own span.
 * @param {[number, number]} view @param {number} factor below 1 zooms in @param {number} at day under the pointer
 * @param {number} days the whole axis @returns {[number, number]}
 */
export function zoomView(view, factor, at, days) {
  const width = Math.min(days, Math.max(Math.min(MIN_VIEW_DAYS, days), (view[1] - view[0]) * factor));
  const share = view[1] > view[0] ? (at - view[0]) / (view[1] - view[0]) : 0.5;
  return slideView([at - width * share, at - width * share + width], 0, days);
}

/**
 * The view moved by some days and kept inside the axis.
 * @param {[number, number]} view @param {number} by @param {number} days @returns {[number, number]}
 */
export function slideView(view, by, days) {
  const width = Math.min(view[1] - view[0], days);
  const start = Math.min(Math.max(view[0] + by, 0), days - width);
  return [start, start + width];
}

export class Timeline {
  /** @param {HTMLCanvasElement} canvas @param {TimelineOptions} options */
  constructor(canvas, options) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.options = options;
    this.start = options.span[0];
    this.days = Math.max(1, Math.ceil((options.span[1] - options.span[0]) / DAY));
    this.play = this.days;
    this.from = 0;
    /** @type {[number, number]} */ this.view = [0, this.days];
    /** @type {number | null} */ this.hover = null;
    this.width = 0; this.height = 0;
    this.streams = this.#streams(options.moments);
    this.#bind();
    this.resize();
  }

  /** How many memories each day holds, for the hint under the pointer. @param {number} day */
  countOn(day) {
    return this.streams.perDay[Math.min(this.days, Math.max(0, Math.floor(day)))] || 0;
  }

  /** @param {Moment[]} moments */
  #streams(moments) {
    const perProject = new Map();
    for (const m of moments) perProject.set(m.p, (perProject.get(m.p) || 0) + 1);
    const top = [...perProject.entries()].filter(([p]) => p !== null).sort((a, b) => b[1] - a[1])
      .slice(0, STREAMS).map(([p]) => p);
    const lanes = [...top, -1];
    const raw = lanes.map(() => new Float32Array(this.days + 1));
    const perDay = new Float32Array(this.days + 1);
    for (const m of moments) {
      const lane = top.indexOf(m.p);
      const day = Math.min(this.days, Math.max(0, Math.floor((m.t - this.start) / DAY)));
      raw[lane < 0 ? lanes.length - 1 : lane][day] += 1;
      perDay[day] += 1;
    }
    const radius = Math.ceil(SMOOTH_DAYS * 3);
    const kernel = Array.from({ length: radius * 2 + 1 }, (_, k) => Math.exp(-((k - radius) ** 2) / (2 * SMOOTH_DAYS ** 2)));
    const norm = kernel.reduce((a, b) => a + b, 0);
    const smooth = raw.map((series) => {
      const out = new Float32Array(series.length);
      for (let d = 0; d < series.length; d++) {
        let s = 0;
        for (let k = -radius; k <= radius; k++) {
          const j = d + k;
          if (j >= 0 && j < series.length) s += series[j] * kernel[k + radius];
        }
        out[d] = s / norm;
      }
      return out;
    });
    // A day's height grows with the square root of its total, so one busy week
    // does not flatten the quiet months into a line.
    const totals = new Float32Array(this.days + 1);
    let peak = 0;
    for (let d = 0; d <= this.days; d++) {
      for (const series of smooth) totals[d] += series[d];
      peak = Math.max(peak, Math.sqrt(totals[d]));
    }
    return { lanes, smooth, totals, perDay, peak: peak || 1 };
  }

  /** The day under a pointer event. @param {{clientX: number}} event */
  #dayAt(event) {
    const rect = this.canvas.getBoundingClientRect();
    const x = Math.min(Math.max(event.clientX - rect.left - PAD_X, 0), rect.width - PAD_X * 2);
    return this.view[0] + (x / (rect.width - PAD_X * 2)) * (this.view[1] - this.view[0]);
  }

  #bind() {
    /** @type {"play" | "from" | null} */ let dragging = null;
    this.canvas.addEventListener("pointerdown", (event) => {
      const rect = this.canvas.getBoundingClientRect(), x = event.clientX - rect.left;
      // The start of the range is grabbed by its grip; everything else moves the playhead.
      dragging = Math.abs(x - this.x(this.from)) <= GRIP_PX && Math.abs(x - this.x(this.play)) > GRIP_PX ? "from" : "play";
      this.canvas.setPointerCapture(event.pointerId);
      this.#drag(dragging, this.#dayAt(event));
    });
    this.canvas.addEventListener("pointermove", (event) => {
      this.hover = this.#dayAt(event);
      const rect = this.canvas.getBoundingClientRect(), x = event.clientX - rect.left;
      this.canvas.style.cursor = Math.abs(x - this.x(this.from)) <= GRIP_PX || Math.abs(x - this.x(this.play)) <= GRIP_PX
        ? "ew-resize" : "pointer";
      if (dragging) this.#drag(dragging, this.hover);
      this.draw();
    });
    const stop = () => { dragging = null; };
    this.canvas.addEventListener("pointerup", stop);
    this.canvas.addEventListener("pointercancel", stop);
    this.canvas.addEventListener("pointerleave", () => { this.hover = null; this.draw(); });
    this.canvas.addEventListener("wheel", (event) => {
      event.preventDefault();
      const width = this.view[1] - this.view[0];
      if (Math.abs(event.deltaX) > Math.abs(event.deltaY) || event.shiftKey) {
        this.view = slideView(this.view, ((event.deltaX || event.deltaY) / Math.max(this.width, 1)) * width, this.days);
      } else {
        this.view = zoomView(this.view, Math.exp(event.deltaY * 0.0022), this.#dayAt(event), this.days);
      }
      this.draw();
    }, { passive: false });
    this.canvas.addEventListener("dblclick", () => { this.view = [0, this.days]; this.draw(); });
  }

  /** @param {"play" | "from"} which @param {number} day */
  #drag(which, day) {
    if (which === "from") this.options.onRange(Math.min(Math.max(day, 0), Math.max(this.play - 1, 0)));
    else this.options.onScrub(day);
  }

  resize() {
    const rect = this.canvas.getBoundingClientRect();
    const dpr = Math.min(window.devicePixelRatio || 1, 2);
    this.canvas.width = Math.round(rect.width * dpr);
    this.canvas.height = Math.round(rect.height * dpr);
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    this.width = rect.width;
    this.height = rect.height;
    this.draw();
  }

  /** @param {number} day */
  setPlay(day) {
    this.play = day;
    // A playhead that leaves the part in view takes the view with it.
    const width = this.view[1] - this.view[0];
    if (width < this.days && (day < this.view[0] || day > this.view[1])) {
      this.view = slideView([day - width / 2, day + width / 2], 0, this.days);
    }
    this.draw();
  }

  /** @param {number} day */
  setFrom(day) {
    this.from = day;
    this.draw();
  }

  /** Shows these days, with room around them. @param {number} first @param {number} last */
  show(first, last) {
    const pad = Math.max((last - first) * 0.15, 2);
    this.view = slideView([first - pad, Math.max(last + pad, first - pad + MIN_VIEW_DAYS)], 0, this.days);
    this.draw();
  }

  /** @param {number} day */
  x(day) {
    return PAD_X + ((day - this.view[0]) / (this.view[1] - this.view[0])) * (this.width - PAD_X * 2);
  }

  draw() {
    const ctx = this.ctx, w = this.width, h = this.height;
    if (!w || !h) return;
    ctx.clearRect(0, 0, w, h);
    const { lanes, smooth, totals, peak } = this.streams;
    const mid = (h - AXIS) / 2, room = h - AXIS - 8;
    const first = Math.max(0, Math.floor(this.view[0]) - 1), last = Math.min(this.days, Math.ceil(this.view[1]) + 1);
    ctx.save();
    ctx.beginPath();
    ctx.rect(PAD_X - 2, 0, w - PAD_X * 2 + 4, h);
    ctx.clip();
    // per day, pixels per recorded event: the square-root height shared by the lanes
    const unit = new Float32Array(this.days + 1), base = new Float32Array(this.days + 1);
    for (let d = first; d <= last; d++) {
      const height = (Math.sqrt(totals[d]) / peak) * room;
      unit[d] = totals[d] > 0 ? height / totals[d] : 0;
      base[d] = mid - height / 2;
    }
    lanes.forEach((project, lane) => {
      const series = smooth[lane];
      ctx.beginPath();
      ctx.moveTo(this.x(first), base[first]);
      for (let d = first + 1; d <= last; d++) ctx.lineTo(this.x(d), base[d]);
      for (let d = last; d >= first; d--) ctx.lineTo(this.x(d), base[d] + series[d] * unit[d]);
      ctx.closePath();
      ctx.globalAlpha = project < 0 ? 0.22 : 0.8;
      ctx.fillStyle = this.options.color(project < 0 ? null : project);
      ctx.fill();
      for (let d = first; d <= last; d++) base[d] += series[d] * unit[d];
    });
    ctx.globalAlpha = 1;

    // the axis: months, and days once the view is narrow enough to tell them apart
    ctx.font = "10px -apple-system, BlinkMacSystemFont, sans-serif";
    ctx.textBaseline = "alphabetic";
    const visible = this.view[1] - this.view[0];
    const month = new Intl.DateTimeFormat("en", { month: "short" });
    const dayAt = (/** @type {Date} */ date) => (date.getTime() / 1000 - this.start) / DAY;
    const origin = new Date(this.start * 1000);
    ctx.fillStyle = "rgba(135, 137, 147, 0.95)";
    for (const first = new Date(origin.getFullYear(), origin.getMonth(), 1); dayAt(first) < this.days; first.setMonth(first.getMonth() + 1)) {
      const x = this.x(dayAt(first));
      if (x > PAD_X - 30 && x < w) {
        ctx.fillRect(x, h - AXIS + 1, 1, 4);
        ctx.fillText(month.format(first), x + 4, h - 4);
      }
    }
    if (visible <= 75) {
      // Every day, every other day, or the Mondays, as the room allows; the first of a month has its own mark.
      const marked = (/** @type {Date} */ date) => (visible <= 16 ? true : visible <= 40 ? date.getDate() % 2 === 1 : date.getDay() === 1);
      ctx.fillStyle = "rgba(135, 137, 147, 0.6)";
      const midnight = new Date((this.start + this.view[0] * DAY) * 1000);
      midnight.setHours(0, 0, 0, 0);
      for (; dayAt(midnight) <= this.view[1]; midnight.setDate(midnight.getDate() + 1)) {
        if (midnight.getDate() === 1 || !marked(midnight) || dayAt(midnight) < this.view[0]) continue;
        const x = this.x(dayAt(midnight));
        ctx.fillRect(x, h - AXIS + 1, 1, 2);
        ctx.fillText(String(midnight.getDate()), x + 3, h - 4);
      }
    }

    // outside the range: before its start, and the future while travelling
    const px = this.x(this.play), fx = this.x(this.from);
    ctx.fillStyle = "rgba(5, 7, 13, 0.72)";
    if (this.play < this.days - 0.01) ctx.fillRect(px, 0, w - px, h - AXIS);
    if (this.from > 0.01) ctx.fillRect(0, 0, fx, h - AXIS);
    if (this.hover !== null) {
      ctx.fillStyle = `rgba(${INK}, 0.25)`;
      ctx.fillRect(this.x(this.hover), 0, 1, h - AXIS);
    }
    ctx.restore();

    // the start of the range: a quiet grip, brighter once it is in use
    const used = this.from > 0.01;
    if (fx >= PAD_X - 1 && fx <= w - PAD_X + 1) {
      ctx.fillStyle = used ? `rgba(${INK}, 0.9)` : `rgba(${INK}, 0.32)`;
      ctx.fillRect(fx - 0.75, 6, 1.5, h - AXIS - 12);
      ctx.beginPath();
      ctx.roundRect(fx - 3, mid - 9, 6, 18, 3);
      ctx.fill();
    }
    // the playhead
    if (px >= PAD_X - 1 && px <= w - PAD_X + 1) {
      ctx.fillStyle = BRASS;
      ctx.fillRect(px - 0.75, 0, 1.5, h - AXIS);
      ctx.beginPath();
      ctx.arc(px, 4, 3.5, 0, Math.PI * 2);
      ctx.fill();
    }
    // zoomed in: a thin bar says which part of all time this is
    if (visible < this.days - 0.01) {
      const track = w - PAD_X * 2;
      ctx.fillStyle = `rgba(${INK}, 0.1)`;
      ctx.fillRect(PAD_X, h - 1.5, track, 1.5);
      ctx.fillStyle = `rgba(${INK}, 0.6)`;
      ctx.fillRect(PAD_X + (this.view[0] / this.days) * track, h - 1.5, Math.max((visible / this.days) * track, 6), 1.5);
    }
  }
}
