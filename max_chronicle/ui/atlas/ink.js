// @ts-check
/**
 * What is written and drawn over the sky: names, rings around what the
 * pointer and the focus are on, the path walked, and the line under the
 * pointer made bright.
 *
 * Names are lettered as on a chart: a project in a serif, under the ball of its memories;
 * everything smaller in a sans beside its light. They are placed greedily,
 * the important first, and one that would cover another is left out: a name
 * nobody can read is worse than no name. From afar only the larger projects
 * are named; the rest get their names as the reader comes closer.
 */

import { clamp, rgb, EDGE, SIMILAR_TINT, STARLIGHT, BRASS } from "./model.js";

const SANS = "-apple-system, BlinkMacSystemFont, \"Segoe UI\", system-ui, sans-serif";
const SERIF = "\"Iowan Old Style\", \"Palatino Linotype\", Palatino, \"Book Antiqua\", Georgia, serif";
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const GOLD = rgb(BRASS).map((v) => Math.round(v * 255)).join(", ");
const PLATE = "5, 7, 13";
const NEIGHBOUR_NAMES = 14;
/** How many projects, the largest first, are named when the whole memory is in view. */
const NAMED_FROM_AFAR = 14;
/** An orbit wider than this is too far from its sun to carry the name: it goes under the sun instead. */
const ORBIT_NAME_MAX_PX = 150;

/**
 * @typedef {{
 *   projected: Float32Array, fit: number, lights: number, zoom: number, focus: number, hovered: number, hoveredEdge: number,
 *   chosenEdge: number, isolate: number | null, focusMix: number, trail: number[], pins: Set<number>,
 *   rings: Map<number, {x: number, y: number, r: number}>,
 *   shown: (i: number) => boolean, lit: (i: number) => boolean,
 * }} View what the page shows this frame; fit is the pixels per world unit when everything fits, lights the scale of
 *   every light, rings the orbits the sky drew
 */

/** @param {string} text @param {number} max */
export const cut = (text, max) => (text.length > max ? text.slice(0, max - 1) + "…" : text);

export class Ink {
  /** @param {HTMLCanvasElement} canvas @param {import("./model.js").Graph} graph */
  constructor(canvas, graph) {
    this.canvas = canvas;
    this.ctx = /** @type {CanvasRenderingContext2D} */ (canvas.getContext("2d"));
    this.graph = graph;
    /** A project's place by size, the largest first. @type {Map<number, number>} */
    this.rank = new Map(graph.byProjectSize.map((project, rank) => [project, rank]));
    this.W = 0; this.H = 0; this.dpr = 1;
  }

  /** @param {number} W @param {number} H @param {number} dpr */
  resize(W, H, dpr) {
    this.W = W; this.H = H; this.dpr = dpr;
    this.canvas.width = Math.round(W * dpr);
    this.canvas.height = Math.round(H * dpr);
  }

  /** A node's drawn radius in pixels: the renderer's own rule. @param {number} i @param {View} view */
  radius(i, view) {
    return this.graph.size[i] * view.lights * clamp(Math.pow(view.projected[i * 3 + 2] / view.fit, 0.55), 0.45, 7);
  }

  /** @param {number} time seconds @param {View} view */
  draw(time, view) {
    const ctx = this.ctx, W = this.W, H = this.H, graph = this.graph, nodes = graph.nodes, p = view.projected;
    ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    ctx.clearRect(0, 0, W, H);
    const sx = (/** @type {number} */ i) => p[i * 3], sy = (/** @type {number} */ i) => p[i * 3 + 1];
    const seen = (/** @type {number} */ i) => p[i * 3 + 2] > 0 && sx(i) > -40 && sx(i) < W + 40 && sy(i) > -20 && sy(i) < H + 20;
    /** How far in a node is: 1 when the whole memory fits and the node is at the target's depth. @param {number} i */
    const near = (i) => p[i * 3 + 2] / view.fit;
    const chosen = view.chosenEdge >= 0 ? graph.edges[view.chosenEdge] : null;
    const narrowed = view.focus >= 0 || chosen !== null;

    // the path walked so far
    const path = view.trail.filter((i) => p[i * 3 + 2] > 0);
    if (path.length > 1) {
      ctx.save();
      ctx.setLineDash([1, 5]);
      ctx.lineCap = "round";
      ctx.lineDashOffset = -time * 10;
      ctx.strokeStyle = `rgba(${INK}, 0.5)`;
      ctx.lineWidth = 1.2;
      ctx.beginPath();
      path.forEach((i, k) => (k ? ctx.lineTo(sx(i), sy(i)) : ctx.moveTo(sx(i), sy(i))));
      ctx.stroke();
      ctx.restore();
    }

    // the line under the pointer, and the one open in the panel
    for (const [index, alpha] of /** @type {[number, number][]} */ ([[view.chosenEdge, 0.95], [view.hoveredEdge, 0.7]])) {
      if (index < 0 || (index === view.chosenEdge && alpha < 0.9)) continue;
      const e = graph.edges[index];
      if (!e || p[e.s * 3 + 2] === 0 || p[e.t * 3 + 2] === 0) continue;
      ctx.save();
      ctx.strokeStyle = e.k === EDGE.similar ? SIMILAR_TINT : graph.colorOf(nodes[e.s].p);
      ctx.globalAlpha = alpha;
      ctx.lineWidth = 1.5;
      if (e.k === EDGE.similar) ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(sx(e.s), sy(e.s));
      ctx.lineTo(sx(e.t), sy(e.t));
      ctx.stroke();
      ctx.restore();
    }

    // rings: what is in focus is held in brass, what the pointer is on in starlight
    const ring = (/** @type {number} */ i, /** @type {string} */ stroke, /** @type {number} */ grow) => {
      if (p[i * 3 + 2] === 0) return;
      ctx.beginPath();
      ctx.arc(sx(i), sy(i), this.radius(i, view) * 1.9 + 5 + grow, 0, Math.PI * 2);
      ctx.strokeStyle = stroke;
      ctx.lineWidth = 1;
      ctx.stroke();
    };
    if (view.focus >= 0) ring(view.focus, `rgba(${GOLD}, ${(0.75 + 0.2 * Math.sin(time * 2.2)).toFixed(3)})`, 2);
    if (view.hovered >= 0 && view.hovered !== view.focus) ring(view.hovered, `rgba(${INK}, 0.55)`, 0);
    for (const index of [view.chosenEdge, view.hoveredEdge]) {
      if (index < 0) continue;
      const stroke = index === view.chosenEdge ? `rgba(${GOLD}, 0.8)` : `rgba(${INK}, 0.45)`;
      ring(graph.edges[index].s, stroke, 0);
      ring(graph.edges[index].t, stroke, 0);
    }
    // pinned to the brief: a small brass mark above the light
    ctx.fillStyle = `rgba(${GOLD}, 0.9)`;
    for (const i of view.pins) {
      if (!view.shown(i) || !seen(i) || (narrowed && !view.lit(i) && view.focusMix > 0.5)) continue;
      const y = sy(i) - this.radius(i, view) * 1.6 - 5;
      ctx.beginPath();
      ctx.moveTo(sx(i), y + 3); ctx.lineTo(sx(i) - 2.5, y - 1.5); ctx.lineTo(sx(i) + 2.5, y - 1.5);
      ctx.closePath();
      ctx.fill();
    }

    // names
    /** @type {[number, number, number, number][]} */
    const boxes = [];
    const free = (/** @type {number} */ x, /** @type {number} */ y, /** @type {number} */ w, /** @type {number} */ h) => {
      for (const b of boxes) if (x < b[0] + b[2] && x + w > b[0] && y < b[1] + b[3] && y + h > b[1]) return false;
      return true;
    };
    ctx.textBaseline = "middle";
    /**
     * @param {number} i @param {string} text @param {string} font @param {string} fill
     * @param {{force?: boolean, below?: boolean, under?: [number, number], plate?: number}} [how] under: the middle of
     *   the name and its line, when it hangs from something other than the node
     */
    const name = (i, text, font, fill, how = {}) => {
      ctx.font = font;
      const w = ctx.measureText(text).width, r = this.radius(i, view);
      const x = how.under ? how.under[0] - w / 2 : how.below ? sx(i) - w / 2 : sx(i) + r * 1.6 + 7;
      const y = how.under ? how.under[1] : how.below ? sy(i) + r * 2.2 + 12 : sy(i);
      // A name that would run off the side of the window is tried elsewhere, or left out.
      if (!how.force && (x < 4 || x + w > W - 4 || !free(x - 5, y - 10, w + 10, 20))) return false;
      boxes.push([x - 5, y - 10, w + 10, 20]);
      if (how.plate) {
        ctx.fillStyle = `rgba(${PLATE}, ${how.plate})`;
        ctx.beginPath();
        ctx.roundRect(x - 6, y - 10, w + 12, 20, 4);
        ctx.fill();
      } else {
        // No plate: the night is drawn around the letters, so the lights behind do not run through them.
        ctx.lineJoin = "round";
        ctx.lineWidth = 3.5;
        ctx.strokeStyle = `rgba(${PLATE}, 0.82)`;
        ctx.strokeText(text, x, y);
      }
      ctx.fillStyle = fill;
      ctx.fillText(text, x, y);
      return true;
    };

    if (view.focus >= 0 && p[view.focus * 3 + 2] > 0) {
      name(view.focus, cut(graph.title(view.focus), 64), `600 13px ${SANS}`, `rgb(${INK})`, { force: true, plate: 0.8 });
      // The neighbours a reader most likely came for: the latest first, the routine ones last.
      const around = graph.neighbours(view.focus).map((link) => link.o).filter((i) => view.shown(i) && seen(i))
        .sort((a, b) => Number(graph.routine(a)) - Number(graph.routine(b)) || (nodes[b].t || 0) - (nodes[a].t || 0));
      let written = 0;
      for (const i of around) {
        if (written >= NEIGHBOUR_NAMES) break;
        if (name(i, cut(graph.title(i), 44), `12px ${SANS}`, `rgba(${INK}, 0.86)`, { plate: 0.66 })) written++;
      }
    }
    if (chosen) {
      // Both ends of the line are named; the second goes under its node when the first is in its way.
      const say = (/** @type {number} */ i, /** @type {{force?: boolean, below?: boolean}} */ how) =>
        p[i * 3 + 2] > 0 && name(i, cut(graph.title(i), 56), `600 12px ${SANS}`, `rgb(${INK})`, { ...how, plate: 0.8 });
      say(chosen.s, { force: true });
      if (!say(chosen.t, {})) say(chosen.t, { force: true, below: true });
    }
    if (view.hovered >= 0 && view.hovered !== view.focus && p[view.hovered * 3 + 2] > 0) {
      name(view.hovered, cut(graph.title(view.hovered), 56), `500 12px ${SANS}`, `rgb(${INK})`, { force: true, plate: 0.8 });
    }

    // Project names: the larger projects from afar, every one from close by. The
    // largest are lettered first, so the names that matter most are never the ones left out.
    const rankOf = (/** @type {number} */ project) => this.rank.get(project) ?? NAMED_FROM_AFAR;
    const suns = [...graph.projectNode].filter(([, i]) => view.shown(i) && seen(i))
      .sort((a, b) => Math.min(rankOf(a[0]), NAMED_FROM_AFAR) - Math.min(rankOf(b[0]), NAMED_FROM_AFAR) || p[b[1] * 3 + 2] - p[a[1] * 3 + 2]);
    // No name is written over a sun.
    for (const [, i] of suns) {
      const r = this.radius(i, view) * 2 + 2;
      boxes.push([sx(i) - r, sy(i) - r, r * 2, r * 2]);
    }
    for (const [project, i] of suns) {
      const rank = rankOf(project), z = near(i);
      const alone = view.isolate === project, held = narrowed && view.lit(i);
      if (rank >= NAMED_FROM_AFAR && z < 1.45 && !alone && !held) continue;
      const dimmed = (narrowed && !view.lit(i)) || (view.isolate !== null && !alone);
      // Far suns are named more faintly than near ones, small projects more faintly than large.
      const alpha = (rank < 6 ? 0.96 : 0.8) * (dimmed ? 0.22 : 1) * clamp(z * 1.5, 0.38, 1);
      const text = graph.projects[project], font = `500 ${rank < 6 ? 14 : 12.5}px ${SERIF}`;
      ctx.font = font;
      const half = ctx.measureText(text).width / 2;
      // A name hangs under its orbit; when that place is taken it goes over it, or beside it.
      const orbit = view.rings.get(project);
      const hold = orbit && orbit.r < ORBIT_NAME_MAX_PX ? orbit : { x: sx(i), y: sy(i), r: this.radius(i, view) * 2.2 };
      /** @type {[number, number][]} */
      const places = [[hold.x, hold.y + hold.r + 13], [hold.x, hold.y - hold.r - 13],
                      [hold.x + hold.r + 9 + half, hold.y], [hold.x - hold.r - 9 - half, hold.y]];
      const fill = `rgba(${INK}, ${alpha.toFixed(3)})`;
      if (!places.some((under) => name(i, text, font, fill, { under })) && (rank < 6 || alone)) {
        name(i, text, font, fill, { under: places[0], force: true });
      }
    }
    if (narrowed) return;

    // tasks, then single memories, as room and nearness allow
    const tasks = [], singles = [];
    for (let i = 0; i < graph.n; i++) {
      if (!view.shown(i) || !seen(i)) continue;
      if (view.isolate !== null && nodes[i].p !== view.isolate) continue;
      const z = near(i);
      if (nodes[i].k === "task") { if (z > 1.9) tasks.push(i); }
      else if (nodes[i].k !== "project" && z > 5 && !graph.routine(i)) singles.push(i);
    }
    tasks.sort((a, b) => graph.degree[b] * near(b) - graph.degree[a] * near(a));
    for (const i of tasks.slice(0, 48)) {
      const alpha = clamp((near(i) - 1.9) / 1.2, 0, 1) * 0.74;
      name(i, cut(nodes[i].l, 34), `11px ${SANS}`, `rgba(${INK}, ${alpha.toFixed(3)})`);
    }
    singles.sort((a, b) => near(b) - near(a));
    for (const i of singles.slice(0, 70)) {
      const alpha = clamp((near(i) - 5) / 2, 0, 1) * 0.7;
      name(i, cut(nodes[i].l, 42), `11px ${SANS}`, `rgba(${INK}, ${alpha.toFixed(3)})`);
    }
  }
}
