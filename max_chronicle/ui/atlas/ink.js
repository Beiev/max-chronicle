// @ts-check
/**
 * What is written and drawn over the sky: names, rings around what the
 * pointer and the focus are on, the lines of the memory in focus, the path
 * walked, and the line under the pointer made bright.
 *
 * A memory in focus holds its lines in brass: each leaves it heavy and arrives
 * light, all bowed the same way round. A memory under the pointer shows its
 * lines thin, in starlight, for as long as the pointer stays.
 *
 * Names are lettered as on a chart: a project in a serif, by the disc of its
 * galaxy; everything smaller in a sans beside its light. They are placed
 * greedily, the important first, and one that would cover another is left
 * out: a name nobody can read is worse than no name. How much is named goes
 * with how close the reader is: from afar the larger projects, closer every
 * project and its tasks, close by the memories that matter most. A long name
 * is cut short until the pointer or the focus is on it.
 */

import { clamp, rgb, EDGE, SIMILAR_TINT, STARLIGHT, BRASS } from "./model.js";
import { bowPoint } from "./pick.js";

const SANS = "-apple-system, BlinkMacSystemFont, \"Segoe UI\", system-ui, sans-serif";
const SERIF = "\"Iowan Old Style\", \"Palatino Linotype\", Palatino, \"Book Antiqua\", Georgia, serif";
const INK = rgb(STARLIGHT).map((v) => Math.round(v * 255)).join(", ");
const GOLD = rgb(BRASS).map((v) => Math.round(v * 255)).join(", ");
const PLATE = "5, 7, 13";
const NEIGHBOUR_NAMES = 9;
/** How many projects, the largest first, are named when the whole memory is in view. */
const NAMED_FROM_AFAR = 14;
/** A disc wider than this is too far from its sun to carry the name: it goes under the sun instead. */
const ORBIT_NAME_MAX_PX = 170;
/** How far a brass line is bowed, as a share of its length. */
export const LINK_BOW = 0.1;
/** A memory under the pointer shows its lines when it has no more than this many. */
const HOVER_LINES = 48;
const TASK_NAMES = 24;
const SINGLE_NAMES = 28;

/**
 * @typedef {{
 *   projected: Float32Array, fit: number, lights: number, zoom: number, focus: number, hovered: number, hoveredEdge: number,
 *   chosenEdge: number, isolate: number | null, focusMix: number, trail: number[], pins: Set<number>,
 *   rings: Map<number, import("./sky.js").Ring>, inked: number[], span: [number, number],
 *   shown: (i: number) => boolean, lit: (i: number) => boolean,
 * }} View what the page shows this frame; fit is the pixels per world unit when everything fits, lights the scale of
 *   every light, rings the discs the sky drew, inked the edges of the focus drawn here in brass, span the stretch of
 *   the window the panels leave free, from its left to its right
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

    // what the memory under the pointer is joined to: thin, for as long as the pointer stays
    if (view.hovered >= 0 && view.hovered !== view.focus && p[view.hovered * 3 + 2] > 0) {
      const links = graph.neighbours(view.hovered);
      if (links.length <= HOVER_LINES) {
        ctx.save();
        ctx.lineWidth = 0.8;
        ctx.strokeStyle = `rgba(${INK}, 0.34)`;
        ctx.beginPath();
        for (const link of links) {
          if (!view.shown(link.o) || p[link.o * 3 + 2] === 0) continue;
          ctx.moveTo(sx(view.hovered), sy(view.hovered));
          ctx.lineTo(sx(link.o), sy(link.o));
        }
        ctx.stroke();
        ctx.restore();
      }
    }

    // what the memory in focus is joined to: brass, heavy where it leaves and light where it arrives
    if (view.focus >= 0 && view.inked.length && p[view.focus * 3 + 2] > 0) {
      const fx = sx(view.focus), fy = sy(view.focus);
      ctx.save();
      ctx.lineCap = "round";
      for (const index of view.inked) {
        const e = graph.edges[index], o = e.s === view.focus ? e.t : e.s;
        if (!view.shown(o) || p[o * 3 + 2] === 0) continue;
        const ox = sx(o), oy = sy(o), [bx, by] = bowPoint(fx, fy, ox, oy, LINK_BOW);
        const trace = () => { ctx.beginPath(); ctx.moveTo(fx, fy); ctx.quadraticCurveTo(bx, by, ox, oy); ctx.stroke(); };
        const along = ctx.createLinearGradient(fx, fy, ox, oy);
        along.addColorStop(0, `rgba(${GOLD}, 0.95)`);
        along.addColorStop(1, `rgba(${GOLD}, 0.34)`);
        ctx.strokeStyle = along;
        ctx.lineWidth = 1.1;
        ctx.setLineDash(e.k === EDGE.similar ? [4, 4] : []);
        trace();
        const leaving = ctx.createLinearGradient(fx, fy, ox, oy);
        leaving.addColorStop(0, `rgba(${GOLD}, 0.9)`);
        leaving.addColorStop(0.32, `rgba(${GOLD}, 0)`);
        ctx.strokeStyle = leaving;
        ctx.lineWidth = 2.6;
        ctx.setLineDash([]);
        trace();
      }
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
      if (view.inked.includes(index)) {
        // A brass line under the pointer is drawn over along its own bow.
        const bow = e.s === view.focus ? LINK_BOW : -LINK_BOW;
        const [bx, by] = bowPoint(sx(e.s), sy(e.s), sx(e.t), sy(e.t), bow);
        ctx.quadraticCurveTo(bx, by, sx(e.t), sy(e.t));
      } else ctx.lineTo(sx(e.t), sy(e.t));
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
    if (view.focus >= 0) {
      ring(view.focus, `rgba(${GOLD}, ${(0.78 + 0.18 * Math.sin(time * 2.2)).toFixed(3)})`, 2);
      ring(view.focus, `rgba(${GOLD}, 0.2)`, 7);
    }
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
      const w = ctx.measureText(text).width, r = this.radius(i, view), [left, right] = view.span;
      let x = how.under ? how.under[0] - w / 2 : how.below ? sx(i) - w / 2 : sx(i) + r * 1.6 + 7;
      let y = how.under ? how.under[1] : how.below ? sy(i) + r * 2.2 + 12 : sy(i);
      // A name that would run under a panel is tried elsewhere, or left out. One that must be written goes to the
      // other side of its light, or under it, and is held inside the free stretch whatever happens.
      if (!how.force && (x < left + 4 || x + w > right - 4 || !free(x - 8, y - 12, w + 16, 24))) return false;
      if (how.force && !how.under && !how.below && x + w > right - 8) {
        x = sx(i) - r * 1.6 - 7 - w;
        if (x < left + 8) { x = sx(i) - w / 2; y = sy(i) + r * 2.2 + 14; }
      }
      if (how.force) x = clamp(x, left + 8, Math.max(left + 8, right - 8 - w));
      boxes.push([x - 8, y - 12, w + 16, 24]);
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
      name(view.focus, cut(graph.title(view.focus), 44), `600 13px ${SANS}`, `rgb(${INK})`, { force: true, plate: 0.8 });
      // The neighbours a reader most likely came for: the latest first, the routine ones last.
      // A sun among them is named as the projects are, further down.
      const around = graph.neighbours(view.focus).map((link) => link.o).filter((i) => view.shown(i) && seen(i) && nodes[i].k !== "project")
        .sort((a, b) => Number(graph.routine(a)) - Number(graph.routine(b)) || (nodes[b].t || 0) - (nodes[a].t || 0));
      let written = 0;
      for (const i of around) {
        if (written >= NEIGHBOUR_NAMES) break;
        if (name(i, cut(graph.title(i), 34), `12px ${SANS}`, `rgba(${INK}, 0.86)`, { plate: 0.66 })) written++;
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
      // A name hangs under the disc of its galaxy; when that place is taken it goes over it, or beside it.
      const disc = view.rings.get(project), own = this.radius(i, view) * 2.2;
      const hold = disc && disc.r < ORBIT_NAME_MAX_PX ? disc : { x: sx(i), y: sy(i), rx: own, ry: own };
      /** @type {[number, number][]} */
      const places = [[hold.x, hold.y + hold.ry + 14], [hold.x, hold.y - hold.ry - 14],
                      [hold.x + hold.rx + 10 + half, hold.y], [hold.x - hold.rx - 10 - half, hold.y]];
      const fill = `rgba(${INK}, ${alpha.toFixed(3)})`, plate = 0.42 * Math.min(alpha * 1.4, 1);
      if (!places.some((under) => name(i, text, font, fill, { under, plate })) && (rank < 6 || alone)) {
        name(i, text, font, fill, { under: places[0], force: true, plate });
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
    for (const i of tasks.slice(0, TASK_NAMES)) {
      const alpha = clamp((near(i) - 1.9) / 1.2, 0, 1) * 0.74;
      name(i, cut(nodes[i].l, 28), `11px ${SANS}`, `rgba(${INK}, ${alpha.toFixed(3)})`);
    }
    // Of single memories the pinned and the weighty (a decision, a milestone) are named before the merely near.
    const weight = (/** @type {number} */ i) => near(i) * graph.size[i] * (view.pins.has(i) ? 2 : 1);
    singles.sort((a, b) => weight(b) - weight(a));
    for (const i of singles.slice(0, SINGLE_NAMES)) {
      const alpha = clamp((near(i) - 5) / 2, 0, 1) * 0.7;
      name(i, cut(nodes[i].l, 32), `11px ${SANS}`, `rgba(${INK}, ${alpha.toFixed(3)})`);
    }
  }
}
