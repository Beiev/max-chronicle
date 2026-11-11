// @ts-check
/**
 * Force layout for the Atlas, run off the main thread, in two or three
 * dimensions.
 *
 * A velocity-Verlet simulation in the manner of d3-force: springs along the
 * recorded edges, a Barnes–Hut many-body repulsion, and a weak pull to the
 * centre. Edges by meaning take no part, so the geography stays the memory's
 * own structure and does not move when they arrive. Positions are posted every
 * few ticks, so the page can show the map forming.
 *
 * The flat map is the same simulation with z held at zero: the tree then
 * splits space in four instead of eight.
 *
 * In:  {n, dims: 2 | 3, kinds: Int8Array, edges: Int32Array (s,t,kind)*, init: Float32Array (x,y,z)*, ticks, warm}
 * Out: {positions: Float32Array (x,y,z)*, tick, ticks, done}
 */

// Repulsion per node kind: project, task, event, note, fact.
const CHARGE = [-1400, -70, -16, -13, -10];
// Spring length per edge kind: task, project, link, fact.
const LENGTH = [16, 70, 54, 12];
// A volume has more room than a plane: the same charges would scatter the
// memory thin, so in three dimensions they pull their punches.
const CHARGE_3D = 0.42;
const LENGTH_3D = 0.8;
const THETA2 = 0.81;
const DISTANCE_MAX2 = 2600 * 2600;
const VELOCITY_DECAY = 0.42;
const CENTER = 0.012;
const POST_EVERY = 6;

/** @param {MessageEvent} message */
self.onmessage = (message) => {
  const { n, dims, kinds, edges, init, ticks, warm } = message.data;
  const deep = dims === 3;
  const x = new Float32Array(n), y = new Float32Array(n), z = new Float32Array(n);
  const vx = new Float32Array(n), vy = new Float32Array(n), vz = new Float32Array(n);
  const charge = new Float32Array(n);
  for (let i = 0; i < n; i++) {
    charge[i] = CHARGE[kinds[i]] * (deep ? CHARGE_3D : 1);
    x[i] = init[i * 3];
    y[i] = init[i * 3 + 1];
    z[i] = deep ? init[i * 3 + 2] : 0;
  }

  const m = edges.length / 3;
  const degree = new Float32Array(n);
  for (let e = 0; e < m; e++) { degree[edges[e * 3]]++; degree[edges[e * 3 + 1]]++; }
  const strength = new Float32Array(m), bias = new Float32Array(m), length = new Float32Array(m);
  for (let e = 0; e < m; e++) {
    const s = edges[e * 3], t = edges[e * 3 + 1], kind = edges[e * 3 + 2];
    strength[e] = 1 / Math.min(degree[s], degree[t]);
    bias[e] = degree[s] / (degree[s] + degree[t]);
    // A busy hub keeps its children further out, so a big task reads as a ball, not a dot.
    length[e] = LENGTH[kind] * (deep ? LENGTH_3D : 1)
      * (1 + Math.sqrt(Math.max(degree[s], degree[t])) * (kind === 0 ? 0.18 : 0.06));
  }

  let alpha = warm ? 0.35 : 1;
  const alphaMin = 0.002;
  const alphaDecay = 1 - Math.pow(alphaMin, 1 / ticks);
  for (let tick = 0; tick < ticks; tick++) {
    alpha += (0 - alpha) * alphaDecay;
    // springs
    for (let e = 0; e < m; e++) {
      const s = edges[e * 3], t = edges[e * 3 + 1];
      let dx = x[t] + vx[t] - x[s] - vx[s] || 1e-3;
      let dy = y[t] + vy[t] - y[s] - vy[s] || 1e-3;
      let dz = deep ? (z[t] + vz[t] - z[s] - vz[s] || 1e-3) : 0;
      let l = Math.sqrt(dx * dx + dy * dy + dz * dz);
      l = (l - length[e]) / l * alpha * strength[e];
      dx *= l; dy *= l; dz *= l;
      const b = bias[e];
      vx[t] -= dx * b; vy[t] -= dy * b; vz[t] -= dz * b;
      vx[s] += dx * (1 - b); vy[s] += dy * (1 - b); vz[s] += dz * (1 - b);
    }
    manyBody(n, deep, x, y, z, vx, vy, vz, charge, alpha);
    for (let i = 0; i < n; i++) {
      vx[i] += -x[i] * CENTER * alpha;
      vy[i] += -y[i] * CENTER * alpha;
      x[i] += vx[i] *= 1 - VELOCITY_DECAY;
      y[i] += vy[i] *= 1 - VELOCITY_DECAY;
      if (deep) {
        vz[i] += -z[i] * CENTER * alpha;
        z[i] += vz[i] *= 1 - VELOCITY_DECAY;
      }
    }
    if (tick % POST_EVERY === 0 || tick === ticks - 1) {
      const positions = new Float32Array(n * 3);
      for (let i = 0; i < n; i++) { positions[i * 3] = x[i]; positions[i * 3 + 1] = y[i]; positions[i * 3 + 2] = z[i]; }
      self.postMessage({ positions, tick, ticks, done: tick === ticks - 1 }, { transfer: [positions.buffer] });
    }
  }
};

/** A cell of the tree: total charge, its centre of charge, and its children or one body. */
class Cell {
  /** @param {number} x0 @param {number} y0 @param {number} z0 @param {number} size */
  constructor(x0, y0, z0, size) {
    this.x0 = x0; this.y0 = y0; this.z0 = z0; this.size = size;
    this.charge = 0; this.cx = 0; this.cy = 0; this.cz = 0;
    /** @type {Cell[] | null} */ this.kids = null;
    this.body = -1;
  }
}

/**
 * @param {number} n @param {boolean} deep
 * @param {Float32Array} x @param {Float32Array} y @param {Float32Array} z
 * @param {Float32Array} vx @param {Float32Array} vy @param {Float32Array} vz
 * @param {Float32Array} charge @param {number} alpha
 */
function manyBody(n, deep, x, y, z, vx, vy, vz, charge, alpha) {
  let lo = Infinity, hi = -Infinity;
  for (let i = 0; i < n; i++) {
    lo = Math.min(lo, x[i], y[i], z[i]);
    hi = Math.max(hi, x[i], y[i], z[i]);
  }
  const root = new Cell(lo - 1, lo - 1, deep ? lo - 1 : 0, hi - lo + 2);

  /** @param {Cell} cell @param {number} i */
  const child = (cell, i) => {
    const half = cell.size / 2;
    return /** @type {Cell[]} */ (cell.kids)[(x[i] >= cell.x0 + half ? 1 : 0) + (y[i] >= cell.y0 + half ? 2 : 0)
      + (deep && z[i] >= cell.z0 + half ? 4 : 0)];
  };
  /** @param {Cell} cell @param {number} i @param {number} depth */
  const insert = (cell, i, depth) => {
    if (!cell.kids && cell.body < 0) { cell.body = i; return; }
    if (!cell.kids) {
      if (depth > 24) return; // coincident bodies: the extra one is skipped this tick
      const half = cell.size / 2;
      cell.kids = [];
      for (let k = 0; k < (deep ? 8 : 4); k++) {
        cell.kids.push(new Cell(cell.x0 + (k & 1 ? half : 0), cell.y0 + (k & 2 ? half : 0),
                                cell.z0 + (k & 4 ? half : 0), half));
      }
      const old = cell.body; cell.body = -1;
      insert(child(cell, old), old, depth + 1);
    }
    insert(child(cell, i), i, depth + 1);
  };
  /** @param {Cell} cell */
  const accumulate = (cell) => {
    if (cell.kids) {
      let q = 0, sx = 0, sy = 0, sz = 0, weight = 0;
      for (const kid of cell.kids) {
        accumulate(kid);
        const w = Math.abs(kid.charge);
        q += kid.charge; sx += kid.cx * w; sy += kid.cy * w; sz += kid.cz * w; weight += w;
      }
      cell.charge = q;
      cell.cx = weight ? sx / weight : 0; cell.cy = weight ? sy / weight : 0; cell.cz = weight ? sz / weight : 0;
    } else if (cell.body >= 0) {
      cell.charge = charge[cell.body]; cell.cx = x[cell.body]; cell.cy = y[cell.body]; cell.cz = z[cell.body];
    }
  };
  /** @param {Cell} cell @param {number} i */
  const apply = (cell, i) => {
    if (!cell.charge) return;
    const dx = cell.cx - x[i], dy = cell.cy - y[i], dz = cell.cz - z[i];
    let l = dx * dx + dy * dy + dz * dz;
    const w = cell.size;
    if (!cell.kids || (w * w) / THETA2 < l) {
      if (cell.body === i || l > DISTANCE_MAX2) return;
      if (l < 1) l = Math.sqrt(l) + 1;
      const k = cell.charge * alpha / l;
      vx[i] += dx * k; vy[i] += dy * k;
      if (deep) vz[i] += dz * k;
      return;
    }
    for (const kid of cell.kids) apply(kid, i);
  };

  for (let i = 0; i < n; i++) insert(root, i, 0);
  accumulate(root);
  for (let i = 0; i < n; i++) apply(root, i);
}
