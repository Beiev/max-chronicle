// @ts-check
/**
 * WebGL2 renderer for the Atlas: edges as thin lines of light, memories as
 * small drawn lights (a star, a ringed point, a diamond, a square, a sun).
 *
 * Positions are in three dimensions and the camera is a matrix and a focal
 * length (camera.js). Nothing is depth-sorted: lines are drawn first, each
 * symbol covers what is under it, and what is far is dimmer and smaller.
 *
 * Time travel costs no buffer upload: every node and edge carries its birth
 * day, and the playhead and the start of the range are two uniforms. Focus is
 * one float per node and edge (-1 hidden, 0 plain, 1 neighbour, 2 the focus)
 * and one eased uniform.
 */

// The projection both programs share. With w = depth the GPU clips what is
// behind the camera, lines included.
const PROJECT = `
uniform mat4 u_view;
uniform vec2 u_k;        // 2 * focal / resolution
uniform vec2 u_off;      // where the frame's centre is, in clip space
uniform vec3 u_fog;      // depth where dimming starts, where it ends, how dim it gets
const float NEAR = 4.0;
const float FAR = 200000.0;
float depthOf(vec3 p, out vec4 clip) {
  vec4 c = u_view * vec4(p, 1.0);
  float depth = -c.z;
  clip = vec4(u_off.x * depth + u_k.x * c.x, u_off.y * depth + u_k.y * c.y,
              ((depth - NEAR) / (FAR - NEAR) * 2.0 - 1.0) * depth, depth);
  return depth;
}
float fog(float depth) { return mix(1.0, u_fog.z, smoothstep(u_fog.x, u_fog.y, depth)); }
`;

const NODE_VS = `#version 300 es
precision highp float;
layout(location=0) in vec3 a_pos;
layout(location=1) in vec3 a_color;
layout(location=2) in float a_size;
layout(location=3) in float a_shape;
layout(location=4) in float a_birth;
layout(location=5) in float a_last;
layout(location=6) in float a_seed;
layout(location=7) in float a_hl;
uniform float u_dpr, u_time, u_play, u_from, u_travel, u_focus, u_focal, u_fit, u_lights, u_maxPoint;
${PROJECT}
out vec3 v_color;
out float v_alpha;
out float v_shape;
out float v_flash;
out float v_px;
void main() {
  float depth = depthOf(a_pos, gl_Position);
  float age = u_play - a_birth;
  float born = smoothstep(-0.5, 0.0, age);
  float flash = u_travel * step(0.0, age) * exp(-age * 1.1);
  float since = max(u_play - min(a_last, u_play), 0.0);
  float glow = mix(0.42, 1.0, exp(-since / 45.0));
  float twinkle = 0.94 + 0.06 * sin(u_time * (0.5 + fract(a_seed * 7.13)) + a_seed * 6.2831);
  float keep = a_hl > 0.5 ? 1.0 : mix(1.0, 0.06, u_focus);
  float shown = a_hl < -0.5 ? 0.0 : 1.0;
  // Before the start of the range: memories go dark, suns and tasks stay as a faint frame.
  bool frame = a_shape > 3.5 || (a_shape > 0.5 && a_shape < 1.5);
  float ranged = a_birth >= u_from - 0.01 ? 1.0 : (frame ? 0.3 : 0.0);
  v_alpha = born * glow * twinkle * keep * shown * ranged * fog(depth);
  v_color = a_color;
  v_shape = a_shape;
  v_flash = flash;
  // What is near is larger, but less than geometry would have it: a sky, not a room.
  float grow = clamp(pow(u_focal / max(depth, NEAR) / u_fit, 0.55), 0.45, 7.0);
  float px = a_size * grow * u_lights * (1.0 + 1.4 * flash) * (a_hl > 1.5 ? 1.45 : 1.0);
  v_px = min(px * 4.0 * u_dpr, u_maxPoint);
  gl_PointSize = v_px * shown * step(0.001, born * ranged);
}`;

const NODE_FS = `#version 300 es
precision highp float;
in vec3 v_color;
in float v_alpha;
in float v_shape;
in float v_flash;
in float v_px;
out vec4 o;
const vec3 STARLIGHT = vec3(0.925, 0.91, 0.867);
// 1 inside the edge at r, 0 outside, with a device pixel of softness.
float inside(float value, float r, float aa) { return 1.0 - smoothstep(r - aa, r + aa, value); }
void main() {
  vec2 p = gl_PointCoord * 2.0 - 1.0;
  float d = length(p);
  if (d > 1.0) discard;
  float aa = 2.0 / max(v_px, 2.0);   // one device pixel, in the sprite's own units
  float core;
  float heat = 0.25;              // how far the heart of the light goes from its colour to starlight
  float halo = exp(-d * d * 5.5) * 0.3;
  if (v_shape < 0.5) {            // event: a star, a point in a small glow
    core = inside(d, 0.26, aa);
  } else if (v_shape < 1.5) {     // task: a point inside a thin ring
    core = inside(d, 0.15, aa) + inside(abs(d - 0.5), max(0.05, aa * 0.6), aa) * 0.6;
    halo *= 0.5;
  } else if (v_shape < 2.5) {     // fact: a small diamond
    core = inside(abs(p.x) + abs(p.y), 0.44, aa * 1.4);
    halo *= 0.8;
  } else if (v_shape < 3.5) {     // note: a small square, quieter than an event
    core = inside(max(abs(p.x), abs(p.y)), 0.24, aa) * 0.7;
    halo *= 0.35;
  } else {                        // project: a sun, a hot heart in a wide soft glow
    core = inside(d, 0.14, aa * 1.5);
    halo = exp(-d * d * 11.0) * 0.8 + exp(-d * d * 2.6) * 0.34;
    heat = 0.85;
  }
  core = min(core, 1.0);
  halo *= 1.0 - smoothstep(0.7, 1.0, d);   // nothing is left of the glow where its square ends
  vec3 col = mix(v_color, STARLIGHT, clamp(core * heat + v_flash * 0.6, 0.0, 1.0));
  // Light laid over what is behind it, not added to it: a thousand memories
  // side by side stay a thousand points and never sum to a white blot.
  float light = min(core + halo, 1.0) * v_alpha;
  o = vec4(col * light, light);
}`;

const EDGE_VS = `#version 300 es
precision highp float;
layout(location=0) in vec3 a_pos;
layout(location=1) in vec4 a_color;
layout(location=2) in float a_along;
layout(location=3) in float a_birth;
layout(location=4) in float a_kind;
layout(location=5) in float a_hl;
layout(location=6) in float a_since;
uniform float u_play, u_from, u_focus, u_similar;
${PROJECT}
out vec4 v_color;
out float v_along;
out float v_hl;
void main() {
  float depth = depthOf(a_pos, gl_Position);
  float born = smoothstep(-0.5, 0.0, u_play - a_birth) * step(u_from - 0.01, a_since);
  bool lit = a_hl > 0.5;
  float meaning = a_kind > 3.5 ? (lit ? 1.0 : u_similar) : 1.0;
  float keep = lit ? 1.0 : mix(1.0, 0.03, u_focus);
  float shown = a_hl < -0.5 ? 0.0 : 1.0;
  float boost = lit ? (a_kind > 3.5 ? 5.0 : 4.0) : 1.0;
  v_color = vec4(a_color.rgb, min(a_color.a * boost, 0.7) * born * meaning * keep * shown * fog(depth));
  v_along = a_along;
  v_hl = a_hl;
}`;

const EDGE_FS = `#version 300 es
precision highp float;
in vec4 v_color;
in float v_along;
in float v_hl;
uniform float u_time;
out vec4 o;
void main() {
  float a = v_color.a;
  if (v_hl > 0.5) {
    float bead = smoothstep(0.88, 1.0, fract(v_along * 1.0 - u_time * 0.32));
    a = min(a + bead * 0.55, 1.0);
  }
  o = vec4(v_color.rgb * a, a);
}`;

/** @param {WebGL2RenderingContext} gl @param {number} type @param {string} source */
function shader(gl, type, source) {
  const s = /** @type {WebGLShader} */ (gl.createShader(type));
  gl.shaderSource(s, source);
  gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(s) || "shader");
  return s;
}

/** @param {WebGL2RenderingContext} gl @param {string} vs @param {string} fs */
function program(gl, vs, fs) {
  const p = /** @type {WebGLProgram} */ (gl.createProgram());
  gl.attachShader(p, shader(gl, gl.VERTEX_SHADER, vs));
  gl.attachShader(p, shader(gl, gl.FRAGMENT_SHADER, fs));
  gl.linkProgram(p);
  if (!gl.getProgramParameter(p, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(p) || "program");
  /** @type {Record<string, WebGLUniformLocation | null>} */
  const uniforms = {};
  const count = gl.getProgramParameter(p, gl.ACTIVE_UNIFORMS);
  for (let i = 0; i < count; i++) {
    const info = /** @type {WebGLActiveInfo} */ (gl.getActiveUniform(p, i));
    uniforms[info.name] = gl.getUniformLocation(p, info.name);
  }
  return { p, uniforms };
}

/**
 * @typedef {{res: [number, number], camera: import("./camera.js").Camera, radius: number, dpr: number, time: number,
 *   play: number, from: number, travel: number, focus: number, similar: number, lights: number}} Frame
 *   lights scales every light: a small window holds the same sky in less room
 */

export class Renderer {
  /** @param {HTMLCanvasElement} canvas */
  constructor(canvas) {
    const gl = canvas.getContext("webgl2", { antialias: true, premultipliedAlpha: true, alpha: true });
    if (!gl) throw new Error("WebGL2 is not available in this browser");
    this.gl = gl;
    this.nodeProgram = program(gl, NODE_VS, NODE_FS);
    this.edgeProgram = program(gl, EDGE_VS, EDGE_FS);
    this.maxPoint = /** @type {Float32Array} */ (gl.getParameter(gl.ALIASED_POINT_SIZE_RANGE))[1] || 64;
    this.nodeVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.edgeVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.nodeCount = 0;
    this.edgeCount = 0;
    /** @type {Int32Array} */ this.edgeEnds = new Int32Array(0);
    this.buffers = {
      nodePos: gl.createBuffer(), nodeStatic: gl.createBuffer(), nodeHl: gl.createBuffer(),
      edgePos: gl.createBuffer(), edgeStatic: gl.createBuffer(), edgeHl: gl.createBuffer(),
    };
  }

  /**
   * @param {{color: Float32Array, size: Float32Array, shape: Float32Array, birth: Float32Array,
   *   last: Float32Array, seed: Float32Array}} nodes
   */
  setNodes(nodes) {
    const gl = this.gl, n = nodes.size.length;
    this.nodeCount = n;
    const packed = new Float32Array(n * 8);
    for (let i = 0; i < n; i++) {
      packed.set([nodes.color[i * 3], nodes.color[i * 3 + 1], nodes.color[i * 3 + 2], nodes.size[i],
                  nodes.shape[i], nodes.birth[i], nodes.last[i], nodes.seed[i]], i * 8);
    }
    gl.bindVertexArray(this.nodeVao);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodePos);
    gl.bufferData(gl.ARRAY_BUFFER, n * 12, gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 3, gl.FLOAT, false, 0, 0);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodeStatic);
    gl.bufferData(gl.ARRAY_BUFFER, packed, gl.STATIC_DRAW);
    const stride = 32;
    [[1, 3, 0], [2, 1, 12], [3, 1, 16], [4, 1, 20], [5, 1, 24], [6, 1, 28]].forEach(([loc, size, offset]) => {
      gl.enableVertexAttribArray(loc);
      gl.vertexAttribPointer(loc, size, gl.FLOAT, false, stride, offset);
    });
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodeHl);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(n), gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(7);
    gl.vertexAttribPointer(7, 1, gl.FLOAT, false, 0, 0);
    gl.bindVertexArray(null);
  }

  /**
   * @param {Int32Array} ends source and target per edge
   * @param {{color: Float32Array, birth: Float32Array, since: Float32Array, kind: Float32Array}} edges rgba per edge,
   *   the day it appears, the latest start of a range it is in, its kind
   */
  setEdges(ends, edges) {
    const gl = this.gl, m = ends.length / 2;
    this.edgeCount = m;
    this.edgeEnds = ends;
    const packed = new Float32Array(m * 2 * 8);
    for (let e = 0; e < m; e++) {
      for (let end = 0; end < 2; end++) {
        packed.set([edges.color[e * 4], edges.color[e * 4 + 1], edges.color[e * 4 + 2], edges.color[e * 4 + 3],
                    end, edges.birth[e], edges.kind[e], edges.since[e]], (e * 2 + end) * 8);
      }
    }
    gl.bindVertexArray(this.edgeVao);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgePos);
    gl.bufferData(gl.ARRAY_BUFFER, m * 24, gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 3, gl.FLOAT, false, 0, 0);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgeStatic);
    gl.bufferData(gl.ARRAY_BUFFER, packed, gl.STATIC_DRAW);
    const stride = 32;
    [[1, 4, 0], [2, 1, 16], [3, 1, 20], [4, 1, 24], [6, 1, 28]].forEach(([loc, size, offset]) => {
      gl.enableVertexAttribArray(loc);
      gl.vertexAttribPointer(loc, size, gl.FLOAT, false, stride, offset);
    });
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgeHl);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(m * 2), gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(5);
    gl.vertexAttribPointer(5, 1, gl.FLOAT, false, 0, 0);
    gl.bindVertexArray(null);
  }

  /** @param {Float32Array} positions x, y, z per node */
  setPositions(positions) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodePos);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, positions.subarray(0, this.nodeCount * 3));
    const m = this.edgeCount, ends = this.edgeEnds, line = new Float32Array(m * 6);
    for (let e = 0; e < m; e++) {
      const s = ends[e * 2] * 3, t = ends[e * 2 + 1] * 3;
      line[e * 6] = positions[s]; line[e * 6 + 1] = positions[s + 1]; line[e * 6 + 2] = positions[s + 2];
      line[e * 6 + 3] = positions[t]; line[e * 6 + 4] = positions[t + 1]; line[e * 6 + 5] = positions[t + 2];
    }
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgePos);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, line);
  }

  /** @param {Float32Array} nodeHl one per node @param {Float32Array} edgeHl one per edge */
  setHighlight(nodeHl, edgeHl) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodeHl);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, nodeHl);
    const doubled = new Float32Array(edgeHl.length * 2);
    for (let e = 0; e < edgeHl.length; e++) { doubled[e * 2] = edgeHl[e]; doubled[e * 2 + 1] = edgeHl[e]; }
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgeHl);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, doubled);
  }

  /** @param {Frame} f */
  draw(f) {
    const gl = this.gl;
    const canvas = /** @type {HTMLCanvasElement} */ (gl.canvas);
    gl.viewport(0, 0, canvas.width, canvas.height);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    gl.enable(gl.BLEND);
    // Premultiplied colour laid over what is there: lines and lights cover, they do not add up.
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);

    const camera = f.camera;
    const k = [(2 * camera.focal) / f.res[0], (2 * camera.focal) / f.res[1]];
    const off = [(2 * camera.frame.cx) / f.res[0] - 1, 1 - (2 * camera.frame.cy) / f.res[1]];
    // What lies beyond the target dims with distance; a flat map lies at one depth and never dims.
    const fog = [camera.dist + f.radius * 0.1, camera.dist + f.radius * 1.6, 0.26];
    /** @param {{uniforms: Record<string, WebGLUniformLocation | null>}} program */
    const project = (program) => {
      gl.uniformMatrix4fv(program.uniforms.u_view, false, camera.view);
      gl.uniform2f(program.uniforms.u_k, k[0], k[1]);
      gl.uniform2f(program.uniforms.u_off, off[0], off[1]);
      gl.uniform3f(program.uniforms.u_fog, fog[0], fog[1], fog[2]);
    };

    const e = this.edgeProgram;
    gl.useProgram(e.p);
    project(e);
    gl.uniform1f(e.uniforms.u_play, f.play);
    gl.uniform1f(e.uniforms.u_from, f.from);
    gl.uniform1f(e.uniforms.u_focus, f.focus);
    gl.uniform1f(e.uniforms.u_similar, f.similar);
    gl.uniform1f(e.uniforms.u_time, f.time);
    gl.bindVertexArray(this.edgeVao);
    gl.drawArrays(gl.LINES, 0, this.edgeCount * 2);

    const n = this.nodeProgram;
    gl.useProgram(n.p);
    project(n);
    gl.uniform1f(n.uniforms.u_dpr, f.dpr);
    gl.uniform1f(n.uniforms.u_time, f.time);
    gl.uniform1f(n.uniforms.u_play, f.play);
    gl.uniform1f(n.uniforms.u_from, f.from);
    gl.uniform1f(n.uniforms.u_travel, f.travel);
    gl.uniform1f(n.uniforms.u_focus, f.focus);
    gl.uniform1f(n.uniforms.u_focal, camera.focal);
    gl.uniform1f(n.uniforms.u_fit, camera.focal / camera.fitDist);
    gl.uniform1f(n.uniforms.u_lights, f.lights);
    gl.uniform1f(n.uniforms.u_maxPoint, this.maxPoint);
    gl.bindVertexArray(this.nodeVao);
    gl.drawArrays(gl.POINTS, 0, this.nodeCount);
    gl.bindVertexArray(null);
  }
}
