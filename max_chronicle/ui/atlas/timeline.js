// @ts-check
/**
 * The time machine's river: how much each project recorded per day, as a
 * centred streamgraph, with a playhead to drag through time.
 */

const DAY = 86400;
const STREAMS = 9;
const SMOOTH_DAYS = 2.2;
const PAD_X = 14;

/**
 * @typedef {{t: number, p: number | null}} Moment
 * @typedef {{span: [number, number], moments: Moment[], color: (project: number | null) => string,
 *   onScrub: (day: number) => void}} TimelineOptions
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

export class Timeline {
  /** @param {HTMLCanvasElement} canvas @param {TimelineOptions} options */
  constructor(canvas, options) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.options = options;
    this.start = options.span[0];
    this.days = Math.max(1, Math.ceil((options.span[1] - options.span[0]) / DAY));
    this.play = this.days;
    /** @type {number | null} */ this.hover = null;
    this.streams = this.#streams(options.moments);
    this.#bind();
    this.resize();
  }

  /** @param {Moment[]} moments */
  #streams(moments) {
    const perProject = new Map();
    for (const m of moments) perProject.set(m.p, (perProject.get(m.p) || 0) + 1);
    const top = [...perProject.entries()].filter(([p]) => p !== null).sort((a, b) => b[1] - a[1])
      .slice(0, STREAMS).map(([p]) => p);
    const lanes = [...top, -1];
    const raw = lanes.map(() => new Float32Array(this.days + 1));
    for (const m of moments) {
      const lane = top.indexOf(m.p);
      const day = Math.min(this.days, Math.max(0, Math.floor((m.t - this.start) / DAY)));
      raw[lane < 0 ? lanes.length - 1 : lane][day] += 1;
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
    return { lanes, smooth, totals, peak: peak || 1 };
  }

  #bind() {
    let dragging = false;
    const dayAt = (/** @type {PointerEvent} */ event) => {
      const rect = this.canvas.getBoundingClientRect();
      const x = Math.min(Math.max(event.clientX - rect.left - PAD_X, 0), rect.width - PAD_X * 2);
      return (x / (rect.width - PAD_X * 2)) * this.days;
    };
    this.canvas.addEventListener("pointerdown", (event) => {
      dragging = true;
      this.canvas.setPointerCapture(event.pointerId);
      this.options.onScrub(dayAt(event));
    });
    this.canvas.addEventListener("pointermove", (event) => {
      this.hover = dayAt(event);
      if (dragging) this.options.onScrub(this.hover);
      this.draw();
    });
    const stop = () => { dragging = false; };
    this.canvas.addEventListener("pointerup", stop);
    this.canvas.addEventListener("pointercancel", stop);
    this.canvas.addEventListener("pointerleave", () => { this.hover = null; this.draw(); });
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
    this.draw();
  }

  /** @param {number} day */
  x(day) {
    return PAD_X + (day / this.days) * ((this.width || 0) - PAD_X * 2);
  }

  draw() {
    const ctx = this.ctx, w = this.width || 0, h = this.height || 0;
    if (!w || !h) return;
    ctx.clearRect(0, 0, w, h);
    const { lanes, smooth, totals, peak } = this.streams;
    const axis = 16;
    const mid = (h - axis) / 2;
    const room = h - axis - 8;
    // per day, pixels per recorded event: the square-root height shared by the lanes
    const unit = new Float32Array(this.days + 1);
    const base = new Float32Array(this.days + 1);
    for (let d = 0; d <= this.days; d++) {
      const height = (Math.sqrt(totals[d]) / peak) * room;
      unit[d] = totals[d] > 0 ? height / totals[d] : 0;
      base[d] = mid - height / 2;
    }
    lanes.forEach((project, lane) => {
      const series = smooth[lane];
      const top = new Float32Array(this.days + 1);
      for (let d = 0; d <= this.days; d++) top[d] = base[d];
      const bottom = new Float32Array(this.days + 1);
      for (let d = 0; d <= this.days; d++) bottom[d] = base[d] + series[d] * unit[d];
      ctx.beginPath();
      ctx.moveTo(this.x(0), top[0]);
      for (let d = 1; d <= this.days; d++) ctx.lineTo(this.x(d), top[d]);
      for (let d = this.days; d >= 0; d--) ctx.lineTo(this.x(d), bottom[d]);
      ctx.closePath();
      const color = this.options.color(project < 0 ? null : project);
      ctx.globalAlpha = project < 0 ? 0.35 : 0.78;
      ctx.fillStyle = color;
      ctx.fill();
      for (let d = 0; d <= this.days; d++) base[d] = bottom[d];
    });
    ctx.globalAlpha = 1;

    // months
    ctx.font = "10px -apple-system, BlinkMacSystemFont, sans-serif";
    ctx.fillStyle = "rgba(125, 134, 163, 0.9)";
    ctx.textBaseline = "alphabetic";
    const month = new Intl.DateTimeFormat("en", { month: "short" });
    const first = new Date(this.start * 1000);
    const cursor = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 1));
    while (cursor.getTime() / 1000 < this.start + this.days * DAY) {
      const day = (cursor.getTime() / 1000 - this.start) / DAY;
      const x = this.x(day);
      ctx.fillRect(x, h - axis + 1, 1, 4);
      ctx.fillText(month.format(cursor), x + 4, h - 4);
      cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    }

    // the future, while travelling
    const px = this.x(this.play);
    if (this.play < this.days - 0.01) {
      ctx.fillStyle = "rgba(3, 4, 10, 0.62)";
      ctx.fillRect(px, 0, w - px, h - axis);
    }
    if (this.hover !== null) {
      ctx.fillStyle = "rgba(200, 210, 255, 0.25)";
      ctx.fillRect(this.x(this.hover), 0, 1, h - axis);
    }
    // the playhead
    const glow = ctx.createLinearGradient(px - 10, 0, px + 10, 0);
    glow.addColorStop(0, "rgba(150, 175, 255, 0)");
    glow.addColorStop(0.5, "rgba(170, 195, 255, 0.35)");
    glow.addColorStop(1, "rgba(150, 175, 255, 0)");
    ctx.fillStyle = glow;
    ctx.fillRect(px - 10, 0, 20, h - axis);
    ctx.fillStyle = "#eef2ff";
    ctx.fillRect(px - 0.75, 0, 1.5, h - axis);
    ctx.beginPath();
    ctx.arc(px, 4, 3.5, 0, Math.PI * 2);
    ctx.fill();
  }
}
