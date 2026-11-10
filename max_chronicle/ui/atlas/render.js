// @ts-check
/**
 * WebGL2 renderer for the Atlas: the gas of the galaxies as soft clouds, edges
 * as thin lines of light, memories as small drawn lights (a star, a ringed
 * point, a diamond, a square, a sun).
 *
 * Positions are in three dimensions and the camera is a matrix and a focal
 * length (camera.js). What lies between the galaxies is drawn first, then the
 * lines, then the galaxies one by one from the furthest to the nearest, each
 * its gas and then its memories. So where two galaxies overlap on the screen
 * the nearer one lies over the further: its gas veils the other's lights, and
 * its own lights are on top. What is far is dimmer and smaller besides.
 *
 * A light of gas (gas.js) is a square that faces the reader wherever it hangs,
 * so a galaxy's glow has the depth its memories have. Each is a soft round
 * cloud torn by a slow noise of its own, never the same twice.
 *
 * Gas is soft and covers a great deal of the screen, so it is drawn at half the
 * window's sharpness into a sheet of its own and the sheet laid over the
 * picture: a quarter of the work, and no eye can tell. To keep near over far,
 * the galaxies are taken in a few layers of depth: the gas of a layer, then
 * the memories of that layer at full sharpness, then the next layer nearer.
 *
 * Time travel costs no buffer upload: every node and edge carries its birth
 * day, and the playhead and the start of the range are two uniforms. Focus is
 * one float per node (-1 hidden, 0 plain, 0.5 of the galaxy the focus is in,
 * 1 a neighbour, 2 the focus) and per edge (-1 hidden, 0 plain, 0.5 kept as it
 * is beside a focus, 1 lit, 2 drawn by the ink instead), one eased uniform,
 * and how dim the rest gets.
 */

import { mulberry32 } from "./model.js";

// The projection the programs share. With w = depth the GPU clips what is
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
const float RAYS = 2.4;
layout(location=0) in vec3 a_pos;
layout(location=1) in vec3 a_color;
layout(location=2) in float a_size;
layout(location=3) in float a_shape;
layout(location=4) in float a_birth;
layout(location=5) in float a_last;
layout(location=6) in float a_seed;
layout(location=7) in float a_hl;
uniform float u_dpr, u_time, u_play, u_from, u_travel, u_focus, u_dim, u_focal, u_fit, u_lights, u_maxPoint;
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
  // With something in focus the rest steps back: its own galaxy a little, the others further.
  float keep = a_hl > 0.75 ? 1.0 : mix(1.0, a_hl > 0.25 ? min(u_dim * 2.0, 0.8) : u_dim, u_focus);
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
  // A bright star (a decision, a milestone) is drawn in a wider square, for its rays to lie in.
  if (a_shape > 0.15 && a_shape < 0.5) px *= RAYS;
  v_px = min(px * 4.0 * u_dpr, u_maxPoint);
  gl_PointSize = v_px * shown * step(0.001, born * ranged);
}`;

const NODE_FS = `#version 300 es
precision highp float;
const float RAYS = 2.4;
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
  if (v_shape < 0.15) {           // event: a star, a point in a small glow
    core = inside(d, 0.26, aa);
  } else if (v_shape < 0.5) {     // a decision or a milestone: the same star, with four thin rays as a bright star has
    core = inside(d, 0.26 / RAYS, aa);
    float rays = max(exp(-abs(p.x) * 30.0 - abs(p.y) * 3.4), exp(-abs(p.y) * 30.0 - abs(p.x) * 3.4));
    halo = exp(-d * d * 5.5 * RAYS * RAYS) * 0.34 + rays * 0.5;
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

const GAS_VS = `#version 300 es
precision highp float;
layout(location=0) in vec2 a_corner;
layout(location=1) in vec3 a_at;
layout(location=2) in vec2 a_now;     // radius in world units, how much shows
layout(location=3) in vec4 a_look;    // red, green, blue, strength
layout(location=4) in vec3 a_form;    // its size as a share of its galaxy's reach, seed, the most pixels it may span
uniform float u_focal, u_time, u_window;
uniform vec2 u_px;                    // 2 / resolution
${PROJECT}
out vec2 v_uv;
out vec3 v_color;
out float v_alpha;
out float v_seed;
out float v_px;
void main() {
  vec4 clip;
  float depth = depthOf(a_at, clip);
  float breathe = 1.0 + 0.05 * sin(u_time * (0.11 + fract(a_form.y * 3.7) * 0.16) + a_form.y * 6.2831);
  float px = min(a_now.x * breathe * u_focal / max(depth, NEAR), a_form.z);
  // A galaxy the reader is inside of is a haze, not a wall: its gas thins as the galaxy outgrows the window.
  float thin = clamp(u_window * 0.85 * a_form.x / max(px, 0.5), 0.0, 1.0);
  thin = max(thin * thin, 0.16);
  v_alpha = a_look.a * a_now.y * fog(depth) * thin;
  v_color = a_look.rgb;
  v_seed = a_form.y;
  v_px = px;
  v_uv = a_corner;
  // What would span far more than the window is left out: it would be a wash over everything, at a great cost.
  gl_Position = (v_alpha < 0.006 || depth < NEAR || px > u_window * 1.2) ? vec4(2.0, 2.0, 2.0, 1.0)
    : vec4(clip.xy + a_corner * px * u_px * clip.w, clip.z, clip.w);
}`;

const GAS_FS = `#version 300 es
precision highp float;
in vec2 v_uv;
in vec3 v_color;
in float v_alpha;
in float v_seed;
in float v_px;
uniform float u_time;
uniform sampler2D u_wisps;
out vec4 o;
void main() {
  float d = length(v_uv);
  if (d >= 1.0) discard;
  // a soft round light that is nothing at its rim
  float light = (exp(-d * d * 3.0) - 0.0498) / 0.9502;
  vec3 col = v_color;
  if (v_seed >= 0.0 && v_px > 4.0) {
    // Gas is not a round fog: it is drawn out and curled. Each cloud reads its own patch of the painted wisps
    // (wisps() below), turned its own way, and drifts across it very slowly.
    float way = v_seed * 6.2831;
    vec2 along = vec2(cos(way), sin(way));
    vec2 q = mat2(along.x, along.y, -along.y, along.x) * v_uv * 0.27 + v_seed * vec2(37.0, 17.0) + u_time * 0.0014 * along;
    vec3 painted = texture(u_wisps, q).rgb;
    if (v_color.r + v_color.g + v_color.b < 0.3) {
      // dark dust lies in thin lanes, not in clouds
      float lane = 1.0 - abs(2.0 * painted.r - 1.0);
      light *= 1.7 * lane * lane * lane;
    } else {
      // clear filaments with gaps between them, and a colour that shifts a little from one to the next
      light *= 0.16 + 1.9 * smoothstep(0.3, 0.8, painted.r);
      col *= 0.86 + 0.3 * vec3(painted.g, painted.r, painted.b);
    }
  }
  light = min(light * v_alpha, 1.0);
  // laid over what is behind it, as every light here is
  o = vec4(min(col, vec3(1.0)) * light, light);
}`;

const WISPS_PX = 512;

/**
 * The wisps every cloud of gas is cut from, painted once: noise on noise, each layer finer than the last, read
 * through a slower noise that bends it, so the gas is drawn out and curled as gas is. It repeats at its edges, so a
 * cloud can read any patch of it. Red is the wisps, spread over the whole range from none to full; green and blue are
 * two slow noises of their own, for the shift of colour within a cloud.
 * @returns {Uint8Array} WISPS_PX by WISPS_PX, four bytes a pixel
 */
export function wisps() {
  const size = WISPS_PX, random = mulberry32(20261112);
  /** A square of random values that repeats: noise read from it repeats too. @param {number} period */
  const lattice = (period) => {
    const values = new Float32Array(period * period);
    for (let i = 0; i < values.length; i++) values[i] = random();
    /** @param {number} x @param {number} y in lattice cells */
    return (x, y) => {
      const x0 = Math.floor(x), y0 = Math.floor(y), fx = x - x0, fy = y - y0;
      const ux = fx * fx * fx * (fx * (fx * 6 - 15) + 10), uy = fy * fy * fy * (fy * (fy * 6 - 15) + 10);
      const a = ((x0 % period) + period) % period, b = ((y0 % period) + period) % period, c = (a + 1) % period, e = (b + 1) % period;
      const top = values[b * period + a] + (values[b * period + c] - values[b * period + a]) * ux;
      const low = values[e * period + a] + (values[e * period + c] - values[e * period + a]) * ux;
      return top + (low - top) * uy;
    };
  };
  const layers = [6, 13, 27, 55, 111].map((period, k) => ({ period, read: lattice(period), part: 0.5 * Math.pow(0.54, k) }));
  const bendX = lattice(5), bendY = lattice(5), second = lattice(7), third = lattice(9);
  const raw = new Float32Array(size * size), data = new Uint8Array(size * size * 4);
  let least = Infinity, most = -Infinity;
  for (let y = 0; y < size; y++) {
    for (let x = 0; x < size; x++) {
      const u = x / size, v = y / size;
      // the bend, itself repeating, so the bent wisps still repeat
      const bu = u + (bendX(u * 5, v * 5) - 0.5) * 0.11, bv = v + (bendY(u * 5, v * 5) - 0.5) * 0.11;
      let sum = 0, weight = 0;
      for (const layer of layers) { sum += layer.part * layer.read(bu * layer.period, bv * layer.period); weight += layer.part; }
      const value = sum / weight, at = y * size + x;
      raw[at] = value;
      if (value < least) least = value;
      if (value > most) most = value;
      data[at * 4 + 1] = Math.round(second(u * 7, v * 7) * 255);
      data[at * 4 + 2] = Math.round(third(u * 9, v * 9) * 255);
      data[at * 4 + 3] = 255;
    }
  }
  for (let at = 0; at < raw.length; at++) data[at * 4] = Math.round(((raw[at] - least) / (most - least || 1)) * 255);
  return data;
}

// The sheet of gas laid over the picture: one triangle that covers the window.
const SHEET_VS = `#version 300 es
out vec2 v_uv;
void main() {
  vec2 corner = vec2(gl_VertexID == 1 ? 3.0 : -1.0, gl_VertexID == 2 ? 3.0 : -1.0);
  v_uv = corner * 0.5 + 0.5;
  gl_Position = vec4(corner, 0.0, 1.0);
}`;

const SHEET_FS = `#version 300 es
precision highp float;
in vec2 v_uv;
uniform sampler2D u_sheet;
out vec4 o;
void main() {
  vec4 gas = texture(u_sheet, v_uv);
  // The sheet holds finer steps of light than the screen can show. Laid down as it is, a slow fade would fall into
  // bands; a grain of less than one step, different in every pixel, hides them.
  vec3 q = fract(vec3(gl_FragCoord.xyx) * 0.1031);
  q += dot(q, q.yzx + 33.33);
  float grain = (fract((q.x + q.y) * q.z) - 0.5) / 255.0;
  o = gas.a > 0.0005 ? clamp(gas + grain, 0.0, 1.0) : gas;
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
uniform float u_play, u_from, u_focus, u_dim, u_similar, u_focal, u_fit;
${PROJECT}
out vec4 v_color;
out float v_along;
out float v_hl;
void main() {
  float depth = depthOf(a_pos, gl_Position);
  float born = smoothstep(-0.5, 0.0, u_play - a_birth) * step(u_from - 0.01, a_since);
  bool lit = a_hl > 0.75;
  float meaning = a_kind > 3.5 ? (lit ? 1.0 : u_similar) : 1.0;
  float keep = a_hl > 0.25 ? 1.0 : mix(1.0, u_dim * 0.5, u_focus);
  // A line the ink draws in brass is not drawn here as well.
  float shown = (a_hl < -0.5 || a_hl > 1.5) ? 0.0 : 1.0;
  float boost = lit ? (a_kind > 3.5 ? 5.0 : 4.0) : 1.0;
  // From afar a galaxy is its lights and its gas; the lines inside it come as the reader comes closer. A line by
  // meaning the reader asked for, and any line that is lit, shows from any distance.
  float nearness = u_focal / max(depth, NEAR) / u_fit;
  float closeBy = (lit || a_kind > 3.5) ? 1.0 : smoothstep(1.5, 3.6, nearness);
  v_color = vec4(a_color.rgb, min(a_color.a * boost, 0.7) * born * meaning * keep * shown * closeBy * fog(depth));
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
  if (v_hl > 0.75) {
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

/** @typedef {{start: number, count: number}} Range a run of lights of gas */
/** The gas is drawn at this share of the window's sharpness: soft enough to cost little, fine enough to show its filaments. */
const SHEET_SHARE = 0.62;
/** The galaxies are taken in this many layers of depth, the gas of each under its own memories and over the layers behind. */
const LAYERS = 4;
/**
 * @typedef {{res: [number, number], camera: import("./camera.js").Camera, radius: number, dpr: number, time: number,
 *   play: number, from: number, travel: number, focus: number, dim: number, similar: number, lights: number,
 *   order: number[], field: Range}} Frame
 *   lights scales every light: a small window holds the same sky in less room; dim is how much light is left to
 *   what is not in focus; order is the groups from the furthest to the nearest, field the gas that is no galaxy's
 */

export class Renderer {
  /** @param {HTMLCanvasElement} canvas */
  constructor(canvas) {
    const gl = canvas.getContext("webgl2", { antialias: true, premultipliedAlpha: true, alpha: true });
    if (!gl) throw new Error("WebGL2 is not available in this browser");
    this.gl = gl;
    this.nodeProgram = program(gl, NODE_VS, NODE_FS);
    this.edgeProgram = program(gl, EDGE_VS, EDGE_FS);
    this.gasProgram = program(gl, GAS_VS, GAS_FS);
    this.sheetProgram = program(gl, SHEET_VS, SHEET_FS);
    /** The sheet the gas of one layer is drawn into, at a share of the window's sharpness. */
    this.sheet = { frame: gl.createFramebuffer(), image: gl.createTexture(), w: 0, h: 0 };
    /** The painted wisps every cloud of gas is cut from. */
    this.wisps = gl.createTexture();
    gl.bindTexture(gl.TEXTURE_2D, this.wisps);
    gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, WISPS_PX, WISPS_PX, 0, gl.RGBA, gl.UNSIGNED_BYTE, wisps());
    gl.generateMipmap(gl.TEXTURE_2D);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.REPEAT);
    gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.REPEAT);
    /** Whether the sheet can hold finer steps of light than a screen does: where it can, slow fades do not band. */
    this.fineSheet = Boolean(gl.getExtension("EXT_color_buffer_float") || gl.getExtension("EXT_color_buffer_half_float"));
    this.sheetVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.maxPoint = /** @type {Float32Array} */ (gl.getParameter(gl.ALIASED_POINT_SIZE_RANGE))[1] || 64;
    this.nodeVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.edgeVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.gasVao = /** @type {WebGLVertexArrayObject} */ (gl.createVertexArray());
    this.nodeCount = 0;
    this.edgeCount = 0;
    this.gasCount = 0;
    /** What is drawn together and in what order: a galaxy's gas, its dust of stars and its memories. @type {{offset: number, count: number, gas: Range | null, dust: Range | null}[]} */
    this.groups = [];
    /** @type {Int32Array} */ this.edgeEnds = new Int32Array(0);
    /** The two ends of every edge, written again whenever the nodes move. */
    this.line = new Float32Array(0);
    this.buffers = {
      nodePos: gl.createBuffer(), nodeStatic: gl.createBuffer(), nodeHl: gl.createBuffer(),
      edgePos: gl.createBuffer(), edgeStatic: gl.createBuffer(), edgeHl: gl.createBuffer(),
      gasCorner: gl.createBuffer(), gasLook: gl.createBuffer(), gasAt: gl.createBuffer(), nodeIndex: gl.createBuffer(),
    };
  }

  /**
   * What is drawn together: the nodes of each galaxy and the runs of gas and of star dust that are its own. Every
   * node is in one group.
   * @param {{nodes: number[], gas: Range | null, dust: Range | null}[]} groups
   */
  setGroups(groups) {
    const gl = this.gl, index = new Uint32Array(groups.reduce((sum, group) => sum + group.nodes.length, 0));
    let at = 0;
    this.groups = groups.map((group) => {
      index.set(group.nodes, at);
      const kept = { offset: at * 4, count: group.nodes.length, gas: group.gas, dust: group.dust };
      at += group.nodes.length;
      return kept;
    });
    gl.bindVertexArray(this.nodeVao);
    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.buffers.nodeIndex);
    gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, index, gl.STATIC_DRAW);
    gl.bindVertexArray(null);
  }

  /**
   * The lights of gas: what never changes of each (gas.js: red, green, blue, strength, share of the reach, seed, most
   * pixels). Where they are comes every frame through setGasAt.
   * @param {Float32Array} look seven floats a light @param {number} count
   */
  setGas(look, count) {
    const gl = this.gl;
    this.gasCount = count;
    gl.bindVertexArray(this.gasVao);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasCorner);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
    gl.enableVertexAttribArray(0);
    gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 0, 0);
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasAt);
    gl.bufferData(gl.ARRAY_BUFFER, count * 20, gl.DYNAMIC_DRAW);
    [[1, 3, 0], [2, 2, 12]].forEach(([loc, size, offset]) => {
      gl.enableVertexAttribArray(loc);
      gl.vertexAttribPointer(loc, size, gl.FLOAT, false, 20, offset);
      gl.vertexAttribDivisor(loc, 1);
    });
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasLook);
    gl.bufferData(gl.ARRAY_BUFFER, look, gl.STATIC_DRAW);
    [[3, 4, 0], [4, 3, 16]].forEach(([loc, size, offset]) => {
      gl.enableVertexAttribArray(loc);
      gl.vertexAttribPointer(loc, size, gl.FLOAT, false, 28, offset);
      gl.vertexAttribDivisor(loc, 1);
    });
    gl.bindVertexArray(null);
  }

  /** @param {Float32Array} at x, y, z, radius and how much shows, per light of gas */
  setGasAt(at) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasAt);
    gl.bufferSubData(gl.ARRAY_BUFFER, 0, at);
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
    const m = this.edgeCount, ends = this.edgeEnds;
    if (this.line.length !== m * 6) this.line = new Float32Array(m * 6);
    const line = this.line;
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

    const g = this.gasProgram;
    gl.useProgram(g.p);
    project(g);
    gl.uniform1f(g.uniforms.u_focal, camera.focal);
    gl.uniform1f(g.uniforms.u_time, f.time);
    gl.uniform1f(g.uniforms.u_window, Math.min(f.res[0], f.res[1]));
    gl.uniform2f(g.uniforms.u_px, 2 / f.res[0], 2 / f.res[1]);
    // The wisps are read from the second texture slot; the first is the sheet's, when it is laid down.
    gl.activeTexture(gl.TEXTURE1);
    gl.bindTexture(gl.TEXTURE_2D, this.wisps);
    gl.uniform1i(g.uniforms.u_wisps, 1);
    /** Draws one run of the lights of gas. @param {Range} run */
    const gas = (run) => {
      if (!run.count) return;
      gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasAt);
      gl.vertexAttribPointer(1, 3, gl.FLOAT, false, 20, run.start * 20);
      gl.vertexAttribPointer(2, 2, gl.FLOAT, false, 20, run.start * 20 + 12);
      gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers.gasLook);
      gl.vertexAttribPointer(3, 4, gl.FLOAT, false, 28, run.start * 28);
      gl.vertexAttribPointer(4, 3, gl.FLOAT, false, 28, run.start * 28 + 16);
      gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, run.count);
    };
    if (this.gasCount) {
      gl.bindVertexArray(this.gasVao);
      gas(f.field);
    }

    const e = this.edgeProgram;
    gl.useProgram(e.p);
    project(e);
    gl.uniform1f(e.uniforms.u_play, f.play);
    gl.uniform1f(e.uniforms.u_from, f.from);
    gl.uniform1f(e.uniforms.u_focus, f.focus);
    gl.uniform1f(e.uniforms.u_dim, f.dim);
    gl.uniform1f(e.uniforms.u_similar, f.similar);
    gl.uniform1f(e.uniforms.u_time, f.time);
    gl.uniform1f(e.uniforms.u_focal, camera.focal);
    gl.uniform1f(e.uniforms.u_fit, camera.focal / camera.fitDist);
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
    gl.uniform1f(n.uniforms.u_dim, f.dim);
    gl.uniform1f(n.uniforms.u_focal, camera.focal);
    gl.uniform1f(n.uniforms.u_fit, camera.focal / camera.fitDist);
    gl.uniform1f(n.uniforms.u_lights, f.lights);
    gl.uniform1f(n.uniforms.u_maxPoint, this.maxPoint);
    if (!this.groups.length) {
      gl.bindVertexArray(this.nodeVao);
      gl.drawArrays(gl.POINTS, 0, this.nodeCount);
    }
    // The sheet the gas is drawn into follows the size of the window.
    const sheet = this.sheet, sw = Math.max(1, Math.round(canvas.width * SHEET_SHARE)), sh = Math.max(1, Math.round(canvas.height * SHEET_SHARE));
    if (sheet.w !== sw || sheet.h !== sh) {
      sheet.w = sw; sheet.h = sh;
      gl.bindTexture(gl.TEXTURE_2D, sheet.image);
      if (this.fineSheet) gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA16F, sw, sh, 0, gl.RGBA, gl.HALF_FLOAT, null);
      else gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, sw, sh, 0, gl.RGBA, gl.UNSIGNED_BYTE, null);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
      gl.bindFramebuffer(gl.FRAMEBUFFER, sheet.frame);
      gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, sheet.image, 0);
      gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    }
    // From the furthest to the nearest, a few galaxies at a time: their gas, then their memories over it.
    const groups = f.order.map((which) => this.groups[which]).filter(Boolean);
    const gassy = groups.filter((group) => group.gas).length, perLayer = Math.max(1, Math.ceil(gassy / LAYERS));
    for (let from = 0; from < groups.length;) {
      let to = from, taken = 0;
      while (to < groups.length && (taken < perLayer || !groups[to].gas)) { if (groups[to].gas) taken++; to++; }
      const layer = groups.slice(from, to);
      from = to;
      if (taken && this.gasCount) {
        gl.bindFramebuffer(gl.FRAMEBUFFER, sheet.frame);
        gl.viewport(0, 0, sw, sh);
        gl.clear(gl.COLOR_BUFFER_BIT);
        gl.useProgram(g.p);
        gl.bindVertexArray(this.gasVao);
        for (const group of layer) if (group.gas) gas(group.gas);
        gl.bindFramebuffer(gl.FRAMEBUFFER, null);
        gl.viewport(0, 0, canvas.width, canvas.height);
        gl.useProgram(this.sheetProgram.p);
        gl.bindVertexArray(this.sheetVao);
        gl.activeTexture(gl.TEXTURE0);
        gl.bindTexture(gl.TEXTURE_2D, sheet.image);
        gl.uniform1i(this.sheetProgram.uniforms.u_sheet, 0);
        gl.drawArrays(gl.TRIANGLES, 0, 3);
      }
      // Galaxy by galaxy within the layer: its dust of stars, sharp, and its memories over that.
      for (const group of layer) {
        if (group.dust && this.gasCount) {
          gl.useProgram(g.p);
          gl.bindVertexArray(this.gasVao);
          gas(group.dust);
        }
        gl.useProgram(n.p);
        gl.bindVertexArray(this.nodeVao);
        gl.drawElements(gl.POINTS, group.count, gl.UNSIGNED_INT, group.offset);
      }
    }
    gl.bindVertexArray(null);
  }
}
