// A fake page for the Atlas: enough of a document, of WebGL and of the clock to run the page's own wiring (main.js)
// under node, a frame at a time. Nothing is drawn. What the page asks of WebGL is kept: the values of its uniforms,
// and for every draw which textures the program reads and whether one of them is the one being drawn into.
// Used by tests/test_atlas_page.py, which copies it beside the page's modules.
let counter = 0;

export async function openPage({ data, hash = "", calm = false, W = 1400, H = 900, similar = { status: "none" }, storage = {} }) {
  const page = { clock: 1000, frames: [], listeners: new Map(), els: new Map(), gl: null, glLog: [], inkLog: [], errors: [], workers: [], fetches: [], uniform: {}, };
  const listen = (map) => (type, fn) => { const list = map.get(type) || []; list.push(fn); map.set(type, list); };

  class FakeEl {
    constructor(id) {
      this.id = id; this.hidden = false; this.value = ""; this.textContent = ""; this.children = []; this.dataset = {}; this.title = "";
      this.style = {}; this.width = 0; this.height = 0; this._listeners = new Map(); this._classes = new Set(); this._attrs = {}; this._ctx = null;
      const self = this;
      this.classList = { add: (c) => self._classes.add(c), remove: (c) => self._classes.delete(c), contains: (c) => self._classes.has(c),
                         toggle: (c, on) => { const want = on === undefined ? !self._classes.has(c) : on; if (want) self._classes.add(c); else self._classes.delete(c); return want; } };
    }
    addEventListener(type, fn) { listen(this._listeners)(type, fn); }
    setAttribute(k, v) { this._attrs[k] = v; }
    getAttribute(k) { return this._attrs[k]; }
    append(...kids) { this.children.push(...kids); }
    replaceChildren(...kids) { this.children = kids; }
    querySelectorAll() { return []; }
    focus() {} showPicker() {} scrollIntoView() {} setPointerCapture() {}
    getBoundingClientRect() { return { left: 0, top: 0, width: W, height: 134 }; }
    getContext(kind) {
      if (kind === "webgl2") { page.gl = page.gl || makeGL(this, page); return page.gl; }
      return this._ctx || (this._ctx = makeCtx(this, page));
    }
    fire(type, event = {}) { for (const fn of this._listeners.get(type) || []) fn({ target: this, preventDefault() {}, ...event }); }
  }
  const lenient = (el) => new Proxy(el, { get(t, k, r) { if (k in t) return Reflect.get(t, k, t); return typeof k === "string" ? () => undefined : undefined; },
                                           set(t, k, v) { t[k] = v; return true; } });
  class HTMLInputElement extends FakeEl {}
  const get = (id) => { if (!page.els.has(id)) page.els.set(id, lenient(id === "search-input" || id === "time-date" ? new HTMLInputElement(id) : new FakeEl(id))); return page.els.get(id); };
  get("search").hidden = true; get("tip").hidden = true; get("known").hidden = true;

  const store = new Map(Object.entries(storage));
  const location = { hash, pathname: "/ui/atlas", search: "" };
  const winListeners = page.listeners;
  const g = globalThis;
  g.HTMLInputElement = HTMLInputElement;
  g.document = { getElementById: get, createElement: (tag) => lenient(new FakeEl(`<${tag}>`)), querySelectorAll: () => [], body: get("body"), createTextNode: (t) => t };
  g.window = { innerWidth: W, innerHeight: H, devicePixelRatio: 1, matchMedia: () => ({ matches: calm }), addEventListener: listen(winListeners),
               setTimeout: (fn, ms) => setTimeout(fn, ms), clearTimeout: (t) => clearTimeout(t) };
  g.localStorage = { getItem: (k) => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, v) };
  g.location = location;
  g.history = { pushState: (_s, _t, url) => { location.hash = url.startsWith("#") ? url : ""; }, replaceState: (_s, _t, url) => { location.hash = url.startsWith("#") ? url : ""; } };
  Object.defineProperty(g, "performance", { value: { now: () => page.clock }, configurable: true, writable: true });
  g.requestAnimationFrame = (fn) => { page.frames.push(fn); return page.frames.length; };
  g.Worker = class { constructor() { page.workers.push(this); this.posted = []; } postMessage(m) { this.posted.push(m); } terminate() { this.dead = true; } };
  g.fetch = async (url, options) => {
    page.fetches.push([url, options]);
    const answer = url === "/ui/api/atlas" ? data : url === "/ui/api/atlas/similar" ? similar : (page.answer ? page.answer(url, options) : {});
    return { ok: true, status: 200, json: async () => answer };
  };
  process.removeAllListeners("unhandledRejection");
  process.on("unhandledRejection", (error) => { page.errors.push(error); });

  await import(`./main.mjs?page=${++counter}`);
  // let main() get past its awaits
  for (let k = 0; k < 20; k++) await new Promise((resolve) => setImmediate(resolve));

  /** Runs one frame at the page's clock plus ms; returns the error thrown, if any. */
  page.frame = (ms = 16) => {
    page.clock += ms;
    const fn = page.frames.pop();
    page.frames.length = 0;
    if (!fn) return "no frame scheduled";
    try { fn(page.clock); return null; } catch (error) { return error; }
  };
  page.window = (type, event = {}) => { for (const fn of winListeners.get(type) || []) fn({ target: get("body"), key: "", preventDefault() {}, ...event }); };
  page.el = get;
  /** Every fake element under one, depth first. */
  page.under = (root) => { const out = []; const walk = (node) => { if (!node || typeof node !== "object") return; out.push(node); for (const kid of node.children || []) walk(kid); }; walk(root); return out; };
  page.settle = async () => { for (let k = 0; k < 20; k++) await new Promise((resolve) => setImmediate(resolve)); };
  page.location = location;
  /** Finishes the layout with the positions the page seeded the worker with. */
  page.finishLayout = () => {
    const worker = page.workers[page.workers.length - 1];
    worker.onmessage({ data: { positions: worker.posted[0].init.slice(), done: true, tick: 1 } });
  };
  return page;
}

function makeCtx(canvas, page) {
  const log = canvas.id === "ink" ? page.inkLog : null;
  const real = { canvas, measureText: (text) => ({ width: String(text).length * 6 }),
                 createLinearGradient: () => ({ addColorStop() {} }), createRadialGradient: () => ({ addColorStop() {} }) };
  return new Proxy(real, {
    get(t, k) { if (k in t) return t[k]; return (...args) => { if (log) log.push([k, ...args]); }; },
    set(t, k, v) { t[k] = v; return true; },
  });
}

function makeGL(canvas, page) {
  const consts = new Map();
  let next = 1;
  const state = { unit: 0, units: {}, fb: null, attach: new Map(), program: null, sources: new Map(), attached: new Map(), uniformValue: new Map(), names: new Map(), vao: null, ptr: {} };
  const uniformsOf = (program) => {
    const names = [];
    for (const s of state.attached.get(program) || []) {
      for (const m of (state.sources.get(s) || "").matchAll(/uniform\s+\w+\s+([^;]+);/g)) for (const name of m[1].split(",")) names.push(name.trim());
    }
    return [...new Set(names)];
  };
  let made = 0;
  const api = {
    canvas,
    getContextAttributes: () => ({}),
    createShader: () => ({ kind: "shader", n: ++made }), createProgram: () => ({ kind: "program", n: ++made }),
    createTexture: () => ({ kind: "texture", n: ++made }), createBuffer: () => ({ kind: "buffer", n: ++made }),
    createFramebuffer: () => ({ kind: "framebuffer", n: ++made }), createVertexArray: () => ({ kind: "vao", n: ++made }),
    shaderSource: (s, source) => state.sources.set(s, source),
    attachShader: (p, s) => state.attached.set(p, [...(state.attached.get(p) || []), s]),
    getShaderParameter: () => true,
    getProgramParameter: (p, what) => (what === proxy.ACTIVE_UNIFORMS ? uniformsOf(p).length : true),
    getActiveUniform: (p, i) => ({ name: uniformsOf(p)[i] }),
    getUniformLocation: (p, name) => { const loc = { p, name }; return loc; },
    getParameter: () => new Float32Array([1, 256]),
    getExtension: () => null,
    activeTexture: (unit) => { state.unit = unit - 33984; },
    bindTexture: (_target, texture) => { state.units[state.unit] = texture; },
    bindFramebuffer: (_target, fb) => { state.fb = fb; },
    framebufferTexture2D: (_t, _a, _tt, texture) => { state.attach.set(state.fb, texture); },
    useProgram: (p) => { state.program = p; },
    uniform1f: (loc, value) => { if (loc) page.uniform[loc.name] = value; },
    uniform3f: (loc, a, b, c) => { if (loc) page.uniform[loc.name] = [a, b, c]; },
    uniform1i: (loc, value) => { if (loc) state.uniformValue.set(`${loc.p.n}:${loc.name}`, value); },
    vertexAttribPointer: (loc, _size, _type, _norm, _stride, offset) => { state.ptr[loc] = offset; },
    bindVertexArray: (vao) => { state.vao = vao; },
  };
  const draw = (name) => (...args) => {
    const samplers = uniformsOf(state.program).filter((u) => /wisps|sheet/.test(u));
    const reads = samplers.map((u) => ({ u, unit: state.uniformValue.get(`${state.program.n}:${u}`) ?? 0 })).map(({ u, unit }) => ({ u, unit, texture: state.units[unit] }));
    const target = state.fb ? state.attach.get(state.fb) : null;
    page.glLog.push({ name, args, program: state.program, fb: state.fb, reads, feedback: Boolean(target && reads.some((r) => r.texture === target)), gasStart: state.ptr[1] / 20, vao: state.vao });
  };
  for (const name of ["drawArrays", "drawArraysInstanced", "drawElements"]) api[name] = draw(name);
  page.glState = state;
  const proxy = new Proxy(api, {
    get(t, k) {
      if (k in t) return t[k];
      if (typeof k === "string" && /^[A-Z0-9_]+$/.test(k)) { if (!consts.has(k)) consts.set(k, k === "TEXTURE0" ? 33984 : k === "TEXTURE1" ? 33985 : 100000 + next++); return consts.get(k); }
      return () => undefined;
    },
  });
  return proxy;
}

/** A memory to open the page on. projects: [{name, events, tasks?: number, quietDays?: number}] */
export function memory(projects, { days = 100, stale = [], extra = () => {} } = {}) {
  const DAY = 86400, T0 = 1_700_000_000, END = T0 + days * DAY;
  const nodes = [], edges = [], names = projects.map((p) => p.name);
  projects.forEach((project, p) => { nodes.push({ id: `p:${project.name}`, k: "project", l: project.name, p, t: null, u: END }); });
  projects.forEach((project, p) => {
    const count = project.events;
    for (let i = 0; i < count; i++) {
      const t = T0 + Math.round(((i + 0.5) / count) * (days - (project.quietDays || 0)) * DAY);
      nodes.push({ id: `e:${p}-${i}`, k: "event", l: `event ${p}-${i}`, p, t, u: null, c: "note" });
      edges.push([nodes.length - 1, p, 1]);
    }
    for (let i = 0; i < (project.tasks || 0); i++) {
      nodes.push({ id: `t:${project.name}/${i}`, k: "task", l: `task ${p}-${i}`, p, t: END - 3 * DAY, u: END - DAY, o: 1 });
      edges.push([nodes.length - 1, p, 1]);
    }
  });
  const data = { nodes, projects: names, edges, span: nodes.length ? [T0, END] : null, facts: [], actions: { enabled: true, token: "t" } };
  for (const id of stale) nodes.find((node) => node.id === id).st = 1;
  extra(data, { DAY, T0, END });
  return data;
}
