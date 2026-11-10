// @ts-check
/**
 * What is under the pointer: a memory, or a line between two.
 *
 * Both work on the screen, on the positions the camera projected for this
 * frame (x, y, scale per node; scale 0 is behind the camera). A node wins over
 * a line: the lines run into the nodes, and the node is the smaller target.
 */

const NODE_REACH_PX = 12;
const EDGE_REACH_PX = 6;

/**
 * The node under the pointer, or -1.
 * @param {number} mx @param {number} my @param {Float32Array} projected
 * @param {(i: number) => number} radius a node's drawn radius in pixels
 * @param {(i: number) => boolean} eligible
 */
export function pickNode(mx, my, projected, radius, eligible) {
  let best = -1, bestScore = Infinity;
  for (let i = 0, n = projected.length / 3; i < n; i++) {
    if (projected[i * 3 + 2] === 0) continue;
    const dx = projected[i * 3] - mx, dy = projected[i * 3 + 1] - my;
    if (dx > 80 || dx < -80 || dy > 80 || dy < -80 || !eligible(i)) continue;
    const r = radius(i), d = Math.sqrt(dx * dx + dy * dy);
    if (d < NODE_REACH_PX + r * 1.4 && d - r < bestScore) { best = i; bestScore = d - r; }
  }
  return best;
}

/**
 * How far a point is from a segment, and how far along it the nearest point lies (0..1).
 * @param {number} px @param {number} py @param {number} ax @param {number} ay @param {number} bx @param {number} by
 * @returns {[number, number]}
 */
export function distanceToSegment(px, py, ax, ay, bx, by) {
  const dx = bx - ax, dy = by - ay, length2 = dx * dx + dy * dy;
  const t = length2 ? Math.min(1, Math.max(0, ((px - ax) * dx + (py - ay) * dy) / length2)) : 0;
  return [Math.hypot(px - (ax + dx * t), py - (ay + dy * t)), t];
}

/**
 * The edge under the pointer, or -1. The ends of a line belong to its nodes,
 * so the pointer must be on the stretch between them.
 * @param {number} mx @param {number} my @param {{s: number, t: number}[]} edges @param {Float32Array} projected
 * @param {(index: number) => boolean} eligible
 */
export function pickEdge(mx, my, edges, projected, eligible) {
  let best = -1, bestDistance = EDGE_REACH_PX;
  for (let index = 0; index < edges.length; index++) {
    const s = edges[index].s * 3, t = edges[index].t * 3;
    if (projected[s + 2] === 0 || projected[t + 2] === 0) continue;
    const ax = projected[s], ay = projected[s + 1], bx = projected[t], by = projected[t + 1];
    if ((mx < ax - EDGE_REACH_PX && mx < bx - EDGE_REACH_PX) || (mx > ax + EDGE_REACH_PX && mx > bx + EDGE_REACH_PX)
        || (my < ay - EDGE_REACH_PX && my < by - EDGE_REACH_PX) || (my > ay + EDGE_REACH_PX && my > by + EDGE_REACH_PX)) continue;
    if (!eligible(index)) continue;
    const [distance, along] = distanceToSegment(mx, my, ax, ay, bx, by);
    if (distance < bestDistance && along > 0.04 && along < 0.96) { best = index; bestDistance = distance; }
  }
  return best;
}
