// @ts-check
/**
 * Force layout for the Atlas, run off the main thread.
 *
 * A velocity-Verlet simulation in the manner of d3-force: springs along the
 * recorded edges, a Barnes–Hut many-body repulsion, and a weak pull to the
 * centre. Edges by meaning take no part, so the geography stays the memory's
 * own structure and does not move when they arrive. Positions are posted every
 * few ticks, so the page can show the map forming.
 *
 * In:  {n, kinds: Int8Array, edges: Int32Array (s,t,kind)*, init?: Float32Array, ticks}
 * Out: {positions: Float32Array, tick, done}
 */

// Repulsion per node kind: project, task, event, note, fact.
const CHARGE = [-1400, -70, -16, -13, -10];
// Spring length per edge kind: task, project, link, fact.
const LENGTH = [16, 70, 54, 12];
const THETA2 = 0.81;
const DISTANCE_MAX2 = 2600 * 2600;
const VELOCITY_DECAY = 0.42;
const CENTER = 0.012;
const POST_EVERY = 6;

/** @param {MessageEvent} message */
self.onmessage = (message) => {
  const { n, kinds, edges, init, ticks, warm } = message.data;
  const x = new Float32Array(n), y = new Float32Array(n), vx = new Float32Array(n), vy = new Float32Array(n);
  const charge = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    charge[i] = CHARGE[kinds[i]];
    x[i] = init[i * 2];
    y[i] = init[i * 2 + 1];
  }

  const m = edges.length / 3;
  const degree = new Float32Array(n);
  for (let e = 0; e < m; e++) { degree[edges[e * 3]]++; degree[edges[e * 3 + 1]]++; }
  const strength = new Float32Array(m), bias = new Float32Array(m), length = new Float32Array(m);
  for (let e = 0; e < m; e++) {
    const s = edges[e * 3], t = edges[e * 3 + 1], kind = edges[e * 3 + 2];
    strength[e] = 1 / Math.min(degree[s], degree[t]);
    bias[e] = degree[s] / (degree[s] + degree[t]);
    // A busy hub keeps its children further out, so a big task reads as a disc, not a dot.
    length[e] = LENGTH[kind] * (1 + Math.sqrt(Math.max(degree[s], degree[t])) * (kind === 0 ? 0.18 : 0.06));
  }

  let alpha = warm ? 0.35 : 1;
  const alphaMin = 0.002;
  const alphaDecay = 1 - Math.pow(alphaMin, 1 / ticks);
  for (let tick = 0; tick < ticks; tick++) {
    alpha += (0 - alpha) * alphaDecay;
    links(edges, m, x, y, vx, vy, strength, bias, length, alpha);
    manyBody(n, x, y, vx, vy, charge, alpha);
    for (let i = 0; i < n; i++) {
      vx[i] += -x[i] * CENTER * alpha;
      vy[i] += -y[i] * CENTER * alpha;
      x[i] += vx[i] *= 1 - VELOCITY_DECAY;
      y[i] += vy[i] *= 1 - VELOCITY_DECAY;
    }
    if (tick % POST_EVERY === 0 || tick === ticks - 1) post(n, x, y, tick, tick === ticks - 1);
  }
};

/**
 * @param {number} n @param {Float32Array} x @param {Float32Array} y @param {number} tick @param {boolean} done
 */
function post(n, x, y, tick, done) {
  const positions = new Float32Array(n * 2);
  for (let i = 0; i < n; i++) { positions[i * 2] = x[i]; positions[i * 2 + 1] = y[i]; }
  self.postMessage({ positions, tick, done }, { transfer: [positions.buffer] });
}

/**
 * @param {Int32Array} edges @param {number} m
 * @param {Float32Array} x @param {Float32Array} y @param {Float32Array} vx @param {Float32Array} vy
 * @param {Float32Array} strength @param {Float32Array} bias @param {Float32Array} length @param {number} alpha
 */
function links(edges, m, x, y, vx, vy, strength, bias, length, alpha) {
  for (let e = 0; e < m; e++) {
    const s = edges[e * 3], t = edges[e * 3 + 1];
    let dx = x[t] + vx[t] - x[s] - vx[s] || 1e-3;
    let dy = y[t] + vy[t] - y[s] - vy[s] || 1e-3;
    let l = Math.sqrt(dx * dx + dy * dy);
    l = (l - length[e]) / l * alpha * strength[e];
    dx *= l; dy *= l;
    const b = bias[e];
    vx[t] -= dx * b; vy[t] -= dy * b;
    vx[s] += dx * (1 - b); vy[s] += dy * (1 - b);
  }
}

/** A quadtree cell: total charge, its centre of charge, and four children or one body. */
class Cell {
  /** @param {number} x0 @param {number} y0 @param {number} size */
  constructor(x0, y0, size) {
    this.x0 = x0; this.y0 = y0; this.size = size;
    this.charge = 0; this.cx = 0; this.cy = 0;
    /** @type {Cell[] | null} */ this.kids = null;
    this.body = -1;
  }
}

/**
 * @param {number} n @param {Float32Array} x @param {Float32Array} y
 * @param {Float32Array} vx @param {Float32Array} vy @param {Float32Array} charge @param {number} alpha
 */
function manyBody(n, x, y, vx, vy, charge, alpha) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (let i = 0; i < n; i++) {
    if (x[i] < minX) minX = x[i]; if (x[i] > maxX) maxX = x[i];
    if (y[i] < minY) minY = y[i]; if (y[i] > maxY) maxY = y[i];
  }
  const root = new Cell(minX - 1, minY - 1, Math.max(maxX - minX, maxY - minY) + 2);
  for (let i = 0; i < n; i++) insert(root, i, x, y, 0);
  accumulate(root, x, y, charge);
  for (let i = 0; i < n; i++) apply(root, i, x, y, vx, vy, alpha);
}

/** @param {Cell} cell @param {number} i @param {Float32Array} x @param {Float32Array} y @param {number} depth */
function insert(cell, i, x, y, depth) {
  if (!cell.kids && cell.body < 0) { cell.body = i; return; }
  if (!cell.kids) {
    if (depth > 24) return; // coincident bodies: the extra one is skipped this tick
    const half = cell.size / 2;
    cell.kids = [new Cell(cell.x0, cell.y0, half), new Cell(cell.x0 + half, cell.y0, half),
                 new Cell(cell.x0, cell.y0 + half, half), new Cell(cell.x0 + half, cell.y0 + half, half)];
    const old = cell.body; cell.body = -1;
    insert(child(cell, old, x, y), old, x, y, depth + 1);
  }
  insert(child(cell, i, x, y), i, x, y, depth + 1);
}

/** @param {Cell} cell @param {number} i @param {Float32Array} x @param {Float32Array} y */
function child(cell, i, x, y) {
  const half = cell.size / 2;
  const kids = /** @type {Cell[]} */ (cell.kids);
  return kids[(x[i] >= cell.x0 + half ? 1 : 0) + (y[i] >= cell.y0 + half ? 2 : 0)];
}

/** @param {Cell} cell @param {Float32Array} x @param {Float32Array} y @param {Float32Array} charge */
function accumulate(cell, x, y, charge) {
  if (cell.kids) {
    let q = 0, sx = 0, sy = 0, weight = 0;
    for (const kid of cell.kids) {
      accumulate(kid, x, y, charge);
      const w = Math.abs(kid.charge);
      q += kid.charge; sx += kid.cx * w; sy += kid.cy * w; weight += w;
    }
    cell.charge = q; cell.cx = weight ? sx / weight : 0; cell.cy = weight ? sy / weight : 0;
  } else if (cell.body >= 0) {
    cell.charge = charge[cell.body]; cell.cx = x[cell.body]; cell.cy = y[cell.body];
  }
}

/**
 * @param {Cell} cell @param {number} i @param {Float32Array} x @param {Float32Array} y
 * @param {Float32Array} vx @param {Float32Array} vy @param {number} alpha
 */
function apply(cell, i, x, y, vx, vy, alpha) {
  if (!cell.charge) return;
  const dx = cell.cx - x[i], dy = cell.cy - y[i];
  let l = dx * dx + dy * dy;
  const w = cell.size;
  if (!cell.kids || (w * w) / THETA2 < l) {
    if (cell.body === i || l > DISTANCE_MAX2) return;
    if (l < 1) l = Math.sqrt(l) + 1;
    vx[i] += dx * cell.charge * alpha / l;
    vy[i] += dy * cell.charge * alpha / l;
    return;
  }
  for (const kid of /** @type {Cell[]} */ (cell.kids)) apply(kid, i, x, y, vx, vy, alpha);
}
