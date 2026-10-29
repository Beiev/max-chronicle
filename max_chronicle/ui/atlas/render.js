// @ts-check
/**
 * WebGL2 renderer for the Atlas: edges as thin lines of light, memories as
 * point sprites, both blended additively over the dark sky.
 *
 * Time travel costs no buffer upload: every node and edge carries its birth
 * day, and the playhead is one uniform. Focus is one float per node and edge
 * (-1 hidden, 0 plain, 1 neighbour, 2 the focus) and one eased uniform.
 */

const NODE_VS = `#version 300 es
precision highp float;
layout(location=0) in vec2 a_pos;
layout(location=1) in vec3 a_color;
layout(location=2) in float a_size;
layout(location=3) in float a_shape;
layout(location=4) in float a_birth;
layout(location=5) in float a_last;
layout(location=6) in float a_seed;
layout(location=7) in float a_hl;
uniform vec2 u_res;
uniform vec3 u_view;
uniform float u_dpr, u_time, u_play, u_travel, u_focus, u_zoom, u_maxPoint;
out vec3 v_color;
out float v_alpha;
out float v_shape;
out float v_flash;
void main() {
  vec2 screen = a_pos * u_view.x + u_view.yz;
  vec2 clip = screen / u_res * 2.0 - 1.0;
  gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);
  float age = u_play - a_birth;
  float born = smoothstep(-0.5, 0.0, age);
  float flash = u_travel * step(0.0, age) * exp(-age * 1.1);
  float since = max(u_play - min(a_last, u_play), 0.0);
  float glow = mix(0.42, 1.0, exp(-since / 45.0));
  float twinkle = 0.86 + 0.14 * sin(u_time * (0.5 + fract(a_seed * 7.13)) + a_seed * 6.2831);
  float keep = a_hl > 0.5 ? 1.0 : mix(1.0, 0.06, u_focus);
  float shown = a_hl < -0.5 ? 0.0 : 1.0;
  v_alpha = born * glow * twinkle * keep * shown;
  v_color = a_color;
  v_shape = a_shape;
  v_flash = flash;
  float grow = clamp(pow(u_zoom, 0.55), 0.45, 7.0);
  float px = a_size * grow * (1.0 + 1.4 * flash) * (a_hl > 1.5 ? 1.45 : 1.0);
  gl_PointSize = min(px * 4.0 * u_dpr, u_maxPoint) * shown * step(0.001, born);
}`;

const NODE_FS = `#version 300 es
precision highp float;
in vec3 v_color;
in float v_alpha;
in float v_shape;
in float v_flash;
out vec4 o;
void main() {
  vec2 p = gl_PointCoord * 2.0 - 1.0;
  float d = length(p);
  if (d > 1.0) discard;
  float core;
  float halo = exp(-d * d * 7.0) * 0.5 + exp(-d * d * 30.0) * 0.25;
  if (v_shape < 0.5) {            // event: a star
    core = smoothstep(0.25, 0.06, d);
  } else if (v_shape < 1.5) {     // task: a ring around a small core
    core = smoothstep(0.10, 0.03, d) * 0.9 + smoothstep(0.045, 0.0, abs(d - 0.21)) * 0.85;
  } else if (v_shape < 2.5) {     // fact: a crystal
    core = smoothstep(0.30, 0.20, abs(p.x) * 0.8 + abs(p.y));
  } else if (v_shape < 3.5) {     // note: a soft page
    vec2 q = abs(p);
    core = smoothstep(0.21, 0.15, max(q.x, q.y)) * 0.8;
    halo *= 0.8;
  } else {                        // project: a sun with faint spikes
    core = smoothstep(0.28, 0.0, d);
    float spikes = exp(-abs(p.x) * 46.0) * exp(-abs(p.y) * 2.6) + exp(-abs(p.y) * 46.0) * exp(-abs(p.x) * 2.6);
    halo = halo * 1.6 + spikes * 0.45;
  }
  vec3 col = mix(v_color, vec3(1.0), clamp(core * 0.6 + v_flash * 0.5, 0.0, 1.0));
  float a = clamp(core + halo, 0.0, 1.0) * v_alpha;
  o = vec4(col * a, a);
}`;

const EDGE_VS = `#version 300 es
precision highp float;
layout(location=0) in vec2 a_pos;
layout(location=1) in vec4 a_color;
layout(location=2) in float a_along;
layout(location=3) in float a_birth;
layout(location=4) in float a_kind;
layout(location=5) in float a_hl;
uniform vec2 u_res;
uniform vec3 u_view;
uniform float u_play, u_focus, u_similar;
out vec4 v_color;
out float v_along;
out float v_hl;
void main() {
  vec2 screen = a_pos * u_view.x + u_view.yz;
  vec2 clip = screen / u_res * 2.0 - 1.0;
  gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);
  float born = smoothstep(-0.5, 0.0, u_play - a_birth);
  bool lit = a_hl > 0.5;
  float meaning = a_kind > 3.5 ? (lit ? 1.0 : u_similar) : 1.0;
  float keep = lit ? 1.0 : mix(1.0, 0.03, u_focus);
  float shown = a_hl < -0.5 ? 0.0 : 1.0;
  float boost = lit ? (a_kind > 3.5 ? 5.0 : 4.0) : 1.0;
  v_color = vec4(a_color.rgb, min(a_color.a * boost, 0.7) * born * meaning * keep * shown);
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
 * @typedef {{res: [number, number], view: [number, number, number], dpr: number, time: number,
 *   play: number, travel: number, focus: number, zoom: number, similar: number}} Frame
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
    gl.bufferData(gl.ARRAY_BUFFER, n * 8, gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0);
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
   * @param {{color: Float32Array, birth: Float32Array, kind: Float32Array}} edges rgba per edge, birth, kind
   */
  setEdges(ends, edges) {
    const gl = this.gl, m = ends.length / 2;
    this.edgeCount = m;
    this.edgeEnds = ends;
    const packed = new Float32Array(m * 2 * 7);
    for (let e = 0; e < m; e++) {
      for (let end = 0; end < 2; end++) {
        packed.set([edges.color[e * 4], edges.color[e * 4 + 1], edges.color[e * 4 + 2], edges.color[e * 4 + 3],
                    end, edges.birth[e], edges.kind[e]], (e * 2 + end) * 7);
      }
    }
    gl.bindVertexArray(this.edgeVao);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgePos);
    gl.bufferData(gl.ARRAY_BUFFER, m * 16, gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgeStatic);
    gl.bufferData(gl.ARRAY_BUFFER, packed, gl.STATIC_DRAW);
    const stride = 28;
    [[1, 4, 0], [2, 1, 16], [3, 1, 20], [4, 1, 24]].forEach(([loc, size, offset]) => {
      gl.enableVertexAttribArray(loc);
      gl.vertexAttribPointer(loc, size, gl.FLOAT, false, stride, offset);
    });
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.edgeHl);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(m * 2), gl.DYNAMIC_DRAW);
    gl.enableVertexAttribArray(5);
    gl.vertexAttribPointer(5, 1, gl.FLOAT, false, 0, 0);
    gl.bindVertexArray(null);
  }

  /** @param {Float32Array} positions x, y per node */
  setPositions(positions) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.nodePos);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, positions.subarray(0, this.nodeCount * 2));
    const m = this.edgeCount, ends = this.edgeEnds, line = new Float32Array(m * 4);
    for (let e = 0; e < m; e++) {
      const s = ends[e * 2] * 2, t = ends[e * 2 + 1] * 2;
      line[e * 4] = positions[s]; line[e * 4 + 1] = positions[s + 1];
      line[e * 4 + 2] = positions[t]; line[e * 4 + 3] = positions[t + 1];
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
    gl.blendFunc(gl.ONE, gl.ONE);

    const e = this.edgeProgram;
    gl.useProgram(e.p);
    gl.uniform2f(e.uniforms.u_res, f.res[0], f.res[1]);
    gl.uniform3f(e.uniforms.u_view, f.view[0], f.view[1], f.view[2]);
    gl.uniform1f(e.uniforms.u_play, f.play);
    gl.uniform1f(e.uniforms.u_focus, f.focus);
    gl.uniform1f(e.uniforms.u_similar, f.similar);
    gl.uniform1f(e.uniforms.u_time, f.time);
    gl.bindVertexArray(this.edgeVao);
    gl.drawArrays(gl.LINES, 0, this.edgeCount * 2);

    const n = this.nodeProgram;
    gl.useProgram(n.p);
    gl.uniform2f(n.uniforms.u_res, f.res[0], f.res[1]);
    gl.uniform3f(n.uniforms.u_view, f.view[0], f.view[1], f.view[2]);
    gl.uniform1f(n.uniforms.u_dpr, f.dpr);
    gl.uniform1f(n.uniforms.u_time, f.time);
    gl.uniform1f(n.uniforms.u_play, f.play);
    gl.uniform1f(n.uniforms.u_travel, f.travel);
    gl.uniform1f(n.uniforms.u_focus, f.focus);
    gl.uniform1f(n.uniforms.u_zoom, f.zoom);
    gl.uniform1f(n.uniforms.u_maxPoint, this.maxPoint);
    gl.bindVertexArray(this.nodeVao);
    gl.drawArrays(gl.POINTS, 0, this.nodeCount);
    gl.bindVertexArray(null);
  }
}
