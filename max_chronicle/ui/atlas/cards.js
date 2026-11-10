// @ts-check
/**
 * What the side panel says: one memory, the line between two, or a stretch
 * of time.
 *
 * The line's card is the answer to "why are these two joined": what kind of
 * line it is, who drew it and when, and, for a resemblance, what the two
 * memories share that a reader can check: the words, the files, the task.
 *
 * Built with DOM nodes and textContent throughout (panel.js): memory text is
 * written by agents and is never parsed as HTML.
 */

import { DAY, EDGE, KIND_LABEL, SIMILAR_TINT, STARLIGHT, percent } from "./model.js";
import { el, markdown, relations, kindChip, statRow } from "./panel.js";
import { cut } from "./ink.js";

const TITLE_CHARS = 96;
const number = new Intl.NumberFormat("en");
const dayFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", year: "numeric" });
const timeFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit" });
const shortFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "short" });

/** What each kind of line is called, from the side of its first memory. */
const LINE_NAME = ["Part of a task", "Part of a project", "A link in a note", "A fact it set", "Close in meaning"];
/** @type {Record<string, string>} */
const SIMILAR_WAIT = {
  "computing…": "Resemblance by meaning is still being computed.",
  "no vectors": "Resemblance by meaning needs vectors: none are stored yet.",
  unavailable: "Resemblance by meaning is unavailable: the server log says why.",
};

/**
 * @typedef {{
 *   go: (i: number) => void, goLine: (index: number) => void,
 *   act: (action: string, target: string, origin?: string) => void,
 *   fetchItem: (id: string) => Promise<any>, fetchLink: (a: string, b: string) => Promise<any>,
 *   actions: {enabled: boolean}, similar: () => {ready: boolean, note: string},
 *   acting: () => boolean, outcome: () => {text: string, error: boolean} | null,
 *   dateAt: (day: number) => Date,
 * }} Host what the cards ask of the page; outcome is what the last action did, when it was taken from what the panel shows
 */

export class Cards {
  /** @param {HTMLElement} body @param {import("./model.js").Graph} graph @param {Host} host */
  constructor(body, graph, host) {
    this.body = body;
    this.graph = graph;
    this.host = host;
    // A record that arrives after the panel was drawn again is for a panel that is gone.
    this.drawn = 0;
  }

  /** @param {number} i */
  #when(i) {
    const t = this.graph.nodes[i].t;
    return t ? shortFormat.format(new Date(t * 1000)) : "";
  }

  /** @param {number} i @param {string} [note] @returns {import("./panel.js").Row} */
  #row(i, note) {
    return { index: i, label: this.graph.title(i), color: this.graph.colorOf(this.graph.nodes[i].p), when: this.#when(i), note };
  }

  /** A row that also opens the line joining it to the memory in the panel. @param {import("./model.js").Link} link @param {string} [note] */
  #joined(link, note) {
    return { ...this.#row(link.o, note), why: () => this.host.goLine(link.e) };
  }

  /** @param {string} target a note's name as a link writes it */
  #resolveNote(target) {
    const name = target.split("/").pop()?.replace(/\.md$/, "").trim().toLowerCase() || "";
    if (!this.noteByName) {
      /** @type {Map<string, number>} */ this.noteByName = new Map();
      this.graph.nodes.forEach((node, i) => { if (node.k === "note") this.noteByName?.set(node.l.trim().toLowerCase(), i); });
    }
    const i = this.noteByName.get(name);
    return i === undefined ? null : () => this.host.go(i);
  }

  // ---------- a memory ----------

  /** @param {number} i */
  node(i) {
    const drawn = ++this.drawn, graph = this.graph, host = this.host, body = this.body;
    const node = graph.nodes[i], color = graph.colorOf(node.p);
    body.replaceChildren();
    const head = el("div");
    head.append(kindChip(KIND_LABEL[node.k] + (node.c ? ` · ${node.c}` : ""), color));
    head.append(el("h2", "", cut(graph.title(i), TITLE_CHARS)));
    const sub = el("div", "sub");
    const to = (/** @type {number | undefined} */ target, /** @type {string} */ text) => {
      if (target === undefined) return sub.append(el("span", "", text));
      const a = el("a", "link", text);
      a.addEventListener("click", () => host.go(target));
      sub.append(a);
    };
    if (node.p !== null && node.k !== "project") to(graph.projectNode.get(node.p), graph.projects[node.p]);
    const task = graph.neighbours(i).find((link) => link.k === EDGE.task && link.out);
    if (task) to(task.o, graph.nodes[task.o].l);
    if (node.t) sub.append(el("span", "", (node.k === "event" ? timeFormat : dayFormat).format(new Date(node.t * 1000))));
    if (node.a) sub.append(el("b", "", node.a));
    head.append(sub);
    body.append(head);

    const id = graph.memoryId(i);
    const go = (/** @type {number} */ target) => host.go(target);
    if (id) {
      body.append(this.#curate(i, id));
      const text = el("div", "text", "…");
      text.style.marginTop = "14px";
      body.append(text);
      const plain = el("a", "open-plain", node.k === "event" ? "Open the plain record ↗" : "Open the plain note ↗");
      host.fetchItem(id).then((item) => {
        if (drawn !== this.drawn) return;
        if (!item) { text.textContent = node.l; return; }
        if (item.copies && item.copies.length) body.insertBefore(this.#copies(item.copies, id), plain);
        this.#markTitles(item);
        if (node.k === "note") text.replaceWith(markdown(item.text || "", (name) => this.#resolveNote(name), item.title || node.l));
        else text.textContent = item.text || node.l;
        if (item.why) body.insertBefore(el("div", "why", item.why), body.children[3] || null);
        if (item.checkpoint) body.insertBefore(this.#checkpoint(item.checkpoint), body.children[4] || null);
      }).catch(() => { if (drawn === this.drawn) text.textContent = node.l; });
      const near = graph.neighbours(i);
      if (node.k === "note") {
        body.append(relations("Links to", near.filter((l) => l.k === EDGE.link && l.out).map((l) => this.#joined(l)), go));
        body.append(relations("Linked from", near.filter((l) => l.k === EDGE.link && !l.out).map((l) => this.#joined(l)), go));
      } else {
        body.append(relations("Facts it set", near.filter((l) => l.k === EDGE.fact).map((l) => this.#joined(l)), go));
      }
      const similar = near.filter((l) => l.k === EDGE.similar).sort((a, b) => graph.edges[b.e].w - graph.edges[a.e].w)
        .map((l) => this.#joined(l, graph.edges[l.e].dup ? "same" : `${Math.round(graph.edges[l.e].w * 100)}%`));
      body.append(relations("Close in meaning", similar, go));
      const state = host.similar();
      if (!state.ready) body.append(el("p", "sub", SIMILAR_WAIT[state.note] || SIMILAR_WAIT["computing…"]));
      plain.setAttribute("href", `/ui/${node.k}/${encodeURIComponent(node.id.slice(2))}`);
      body.append(plain);
    } else if (node.k === "task") {
      const events = graph.neighbours(i).filter((l) => l.k === EDGE.task && !l.out).map((l) => l.o)
        .sort((a, b) => (graph.nodes[b].t || 0) - (graph.nodes[a].t || 0));
      body.append(statRow([["events", events.length], ["first", this.#when(i)],
                           ["last", node.u ? shortFormat.format(new Date(node.u * 1000)) : "—"]]));
      body.append(relations("Timeline", events.map((e) => this.#row(e)), go));
      if (node.p !== null) {
        const plain = el("a", "open-plain", "Open the task page ↗");
        plain.setAttribute("href", `/ui/task/${encodeURIComponent(graph.projects[node.p])}/${encodeURIComponent(node.l)}`);
        body.append(plain);
      }
    } else if (node.k === "project") {
      this.#project(i, go);
    } else if (node.k === "fact") {
      const history = graph.facts.filter((f) => f.node === i).sort((a, b) => b.from - a.from);
      const section = el("section");
      section.append(el("h3", "", "Values, newest first"));
      const list = el("ul", "rel facts-chain");
      for (const f of history) {
        const li = el("li");
        li.append(el("span", "v", f.value));
        li.append(f.to ? el("span", "when", `${shortFormat.format(new Date(f.from * 1000))} – ${shortFormat.format(new Date(f.to * 1000))}`)
                       : el("span", "now", `current since ${shortFormat.format(new Date(f.from * 1000))}`));
        list.append(li);
      }
      section.append(list);
      body.append(section);
      body.append(relations("Set by", graph.neighbours(i).filter((l) => l.k === EDGE.fact).map((l) => this.#joined(l))
        .sort((a, b) => (graph.nodes[b.index].t || 0) - (graph.nodes[a.index].t || 0)), go));
    }
  }

  /** @param {number} i @param {(target: number) => void} go */
  #project(i, go) {
    const graph = this.graph, node = graph.nodes[i], body = this.body;
    const tasks = graph.neighbours(i).filter((l) => !l.out && graph.nodes[l.o].k === "task").map((l) => l.o)
      .sort((a, b) => (graph.nodes[b].u || 0) - (graph.nodes[a].u || 0));
    /** @type {number[]} */ const written = [];
    /** @type {number[]} */ const routine = [];
    let notes = 0;
    for (let k = 0; k < graph.n; k++) {
      const other = graph.nodes[k];
      if (other.p !== node.p || graph.gone[k] || (other.k !== "event" && other.k !== "note")) continue;
      if (other.k === "note") notes++;
      (graph.routine(k) ? routine : written).push(k);
    }
    const latest = (/** @type {number} */ a, /** @type {number} */ b) => (graph.nodes[b].t || 0) - (graph.nodes[a].t || 0);
    body.append(statRow([["events", number.format(written.length + routine.length - notes)], ["tasks", tasks.length],
                         ["notes", number.format(notes)], ["since", this.#when(i)]]));
    body.append(relations("Tasks, latest first", tasks.map((t) => this.#row(t)), go));
    // What somebody decided or found comes first; what the machinery logged on schedule is one line further down.
    body.append(relations("Latest memories", written.sort(latest).slice(0, 60).map((k) => this.#row(k)), go));
    body.append(relations("Routine records", routine.sort(latest).slice(0, 36).map((k) => this.#row(k)), go));
    const plain = el("a", "open-plain", "Open the project page ↗");
    plain.setAttribute("href", `/ui/project/${encodeURIComponent(graph.projects[/** @type {number} */ (node.p)])}`);
    body.append(plain);
  }

  /** @param {any} checkpoint */
  #checkpoint(checkpoint) {
    const section = el("section");
    section.append(el("h3", "", "Checkpoint"));
    if (checkpoint.goal) section.append(el("div", "text", checkpoint.goal));
    for (const [key, title] of [["completed", "Completed"], ["next_steps", "Next"], ["open_questions", "Open questions"]]) {
      const entries = checkpoint[key];
      if (!entries || !entries.length) continue;
      const list = el("ul", "plain-list");
      for (const entry of entries) list.append(el("li", "text", entry));
      section.append(el("h3", "", title), list);
    }
    return section;
  }

  /** What the operator marked on a memory, and the buttons to change it. @param {number} i @param {string} id */
  #curate(i, id) {
    const graph = this.graph, host = this.host;
    const bar = el("section", "curate");
    const marks = el("div", "marks");
    if (graph.pinned[i]) marks.append(el("span", "badge pin", "Pinned to the brief"));
    if (graph.stale[i]) marks.append(el("span", "badge stale", "Marked stale"));
    if (marks.childElementCount) bar.append(marks);
    if (host.actions.enabled) {
      const buttons = el("div", "actions");
      const button = (/** @type {string} */ label, /** @type {string} */ title, /** @type {string} */ action) => {
        const b = /** @type {HTMLButtonElement} */ (el("button", "act", label));
        b.type = "button";
        b.title = title;
        b.disabled = host.acting();
        b.addEventListener("click", () => host.act(action, id));
        buttons.append(b);
      };
      if (graph.pinned[i]) button("Unpin", "Take it out of the brief", "unpin");
      else button("Pin to brief", "Open every brief of its project with it", "pin");
      if (graph.stale[i]) button("Mark current", "Rank it as any other memory again", "fresh");
      else button("Mark stale", "Agents still find it, marked stale and ranked lower; it leaves the brief", "stale");
      bar.append(buttons);
    }
    const outcome = host.outcome();
    if (outcome) bar.append(el("p", `act-status${outcome.error ? " error" : ""}`, outcome.text));
    return bar;
  }

  /** When and by whom the marks were set, once the full record has come. @param {any} item */
  #markTitles(item) {
    const since = (/** @type {any} */ mark) => (mark ? `since ${timeFormat.format(new Date(mark.since))}, by ${mark.by}` : "");
    const pin = this.body.querySelector(".badge.pin"), old = this.body.querySelector(".badge.stale");
    if (pin instanceof HTMLElement) pin.title = since(item.pinned);
    if (old instanceof HTMLElement) old.title = since(item.stale);
  }

  /** The copies hidden as repeating a memory, each with a way back. @param {any[]} copies @param {string} id */
  #copies(copies, id) {
    const section = el("section");
    section.append(el("h3", "", `Hidden copies · ${copies.length}`));
    const list = el("ul", "rel copies");
    for (const copy of copies) {
      const li = el("li");
      const label = el("span", "label", copy.label);
      label.title = `Hidden ${timeFormat.format(new Date(copy.since))} by ${copy.by}`;
      li.append(label);
      if (this.host.actions.enabled) {
        const back = el("button", "row-act", "Restore");
        back.setAttribute("type", "button");
        back.title = "Show it to agents and on the map again";
        back.addEventListener("click", () => this.host.act("restore", copy.id, id));
        li.append(back);
      }
      list.append(li);
    }
    section.append(list);
    return section;
  }

  // ---------- the line between two ----------

  /** @param {number} index of an edge */
  line(index) {
    const drawn = ++this.drawn, graph = this.graph, host = this.host, body = this.body;
    const edge = graph.edges[index], a = edge.s, b = edge.t, first = graph.nodes[a], second = graph.nodes[b];
    body.replaceChildren();
    const head = el("div");
    head.append(kindChip(`Line · ${LINE_NAME[edge.k]}`, edge.k === EDGE.similar ? SIMILAR_TINT : graph.colorOf(first.p)));
    head.append(el("h2", "", edge.k === EDGE.similar ? `${percent(edge.w)} alike` : LINE_NAME[edge.k]));
    body.append(head);
    body.append(relations("Between", [this.#row(a), this.#row(b)], (i) => host.go(i)));

    const how = el("section");
    how.append(el("h3", "", "How this line came to be"));
    how.append(el("p", "text", this.#origin(edge)));
    body.append(how);

    const ids = [graph.memoryId(a), graph.memoryId(b)];
    if (edge.k === EDGE.similar && host.actions.enabled && edge.dup && ids[0] && ids[1] && first.k === second.k) {
      const bar = el("section", "curate");
      const buttons = el("div", "actions");
      for (const [copy, keep, label] of /** @type {[string, string, string][]} */ ([[ids[1], ids[0], "Hide the second as a copy"], [ids[0], ids[1], "Hide the first as a copy"]])) {
        const button = /** @type {HTMLButtonElement} */ (el("button", "act", label));
        button.type = "button";
        button.title = "Hide that copy from agents and the map; the other memory stays";
        button.disabled = host.acting();
        button.addEventListener("click", () => host.act("duplicate", copy, keep));
        buttons.append(button);
      }
      bar.append(buttons);
      const outcome = host.outcome();
      if (outcome) bar.append(el("p", `act-status${outcome.error ? " error" : ""}`, outcome.text));
      body.append(bar);
    }
    if (!ids[0] || !ids[1]) return;  // a task, a project or a fact slot at one end: nothing more to compare
    const shared = el("section");
    shared.append(el("h3", "", "What they share"), el("p", "sub", "…"));
    body.append(shared);
    host.fetchLink(ids[0], ids[1]).then((link) => {
      if (drawn !== this.drawn) return;
      shared.replaceChildren(el("h3", "", "What they share"));
      if (!link) { shared.append(el("p", "sub", "One of the two is hidden or gone since the map was loaded.")); return; }
      this.#shared(shared, link, edge);
    }).catch(() => { if (drawn === this.drawn) shared.replaceChildren(el("h3", "", "What they share"), el("p", "sub", "The server did not answer.")); });
  }

  /** In a sentence or two: who drew this line, from what. @param {import("./model.js").Edge} edge */
  #origin(edge) {
    const graph = this.graph, first = graph.nodes[edge.s], second = graph.nodes[edge.t];
    const who = first.a ? `${first.a} ` : "Whoever recorded it ";
    const day = first.t ? ` on ${dayFormat.format(new Date(first.t * 1000))}` : "";
    switch (edge.k) {
      case EDGE.task:
        return `${who}wrote this ${first.k} with the task “${second.l}”${day}. The writer names the task; nothing is guessed.`;
      case EDGE.project:
        return first.k === "task"
          ? `The task “${first.l}” belongs to ${graph.title(edge.t)}: its records were written with that project.`
          : `This ${first.k} carries the project ${graph.title(edge.t)}: named by its writer, or taken from the folder it was written in.`;
      case EDGE.link:
        return `The note “${cut(first.l, 60)}” links to “${cut(second.l, 60)}” in its own text, as a [[wikilink]] or a markdown link. Somebody wrote that link.`;
      case EDGE.fact: {
        // This line runs from the slot to the event: the slot is the first end.
        const setter = second.a ? `${second.a} ` : "Whoever recorded it ";
        const when = second.t ? ` on ${dayFormat.format(new Date(second.t * 1000))}` : "";
        return `${setter}set the fact “${first.l}” with this event${when}. A fact is a slot with one current value; each event that sets it is joined to it, and the slot keeps its history.`;
      }
      default:
        return `Nobody drew this line. The two texts are close by meaning: each memory has a vector, and these two point almost the same way. `
          + `Every memory keeps the three nearest above 80%; this pair is at ${percent(edge.w)}`
          + (edge.dup ? ", close enough to be the same thing said twice." : ".");
    }
  }

  /** @param {HTMLElement} section @param {any} link the answer of /ui/api/atlas/link @param {import("./model.js").Edge} edge */
  #shared(section, link, edge) {
    const facts = el("ul", "plain-list");
    const say = (/** @type {string} */ text) => facts.append(el("li", "text", text));
    if (link.similarity !== null && link.similarity !== undefined) {
      const meter = el("div", "meter");
      const fill = el("i");
      fill.style.width = `${Math.round(Math.max(0, Math.min(1, link.similarity)) * 100)}%`;
      meter.append(fill);
      section.append(meter, el("p", "sub", `Resemblance ${percent(link.similarity)} by ${link.model}`
        + (edge.k === EDGE.similar ? "" : ": not among each other's three nearest, so no line of meaning is drawn.")));
    } else {
      section.append(el("p", "sub", "No vectors to compare these two yet."));
    }
    if (link.same_task) say("The same task.");
    else if (link.same_project) say("The same project, different tasks.");
    else say("Different projects.");
    if (link.days_apart !== null && link.days_apart !== undefined) {
      say(link.days_apart < 1 ? "Written within a day of each other."
        : `Written ${number.format(Math.round(link.days_apart))} days apart.`);
    }
    if (link.shared_sources.length) say(`Both rest on: ${link.shared_sources.join(", ")}.`);
    section.append(facts);
    if (link.shared_words.length) {
      section.append(el("h3", "", "Words both use"));
      const words = el("div", "words");
      for (const word of link.shared_words) words.append(el("span", "word", word));
      section.append(words);
    }
    for (const [side, lines] of /** @type {[string, string[]][]} */ ([["first", link.mentions.a], ["second", link.mentions.b]])) {
      if (!lines.length) continue;
      section.append(el("h3", "", `Where the ${side} links to the other`));
      for (const line of lines) section.append(el("blockquote", "quote", line));
    }
  }

  // ---------- a stretch of time ----------

  /** The memories written between two days of the axis. @param {number} from @param {number} to */
  days(from, to) {
    ++this.drawn;
    const graph = this.graph, host = this.host, body = this.body;
    body.replaceChildren();
    /** @type {number[]} */ const written = [];
    /** @type {number[]} */ const routine = [];
    /** @type {Map<number | null, number>} */ const perProject = new Map();
    for (let i = 0; i < graph.n; i++) {
      const node = graph.nodes[i];
      if ((node.k !== "event" && node.k !== "note") || graph.gone[i] || graph.birth[i] < from || graph.birth[i] > to) continue;
      (graph.routine(i) ? routine : written).push(i);
      perProject.set(node.p, (perProject.get(node.p) || 0) + 1);
    }
    const first = host.dateAt(from), last = host.dateAt(to);
    const oneDay = to - from <= 1.01;
    const head = el("div");
    head.append(kindChip(oneDay ? "A day" : "A stretch of time", STARLIGHT));
    head.append(el("h2", "", oneDay ? dayFormat.format(first) : `${shortFormat.format(first)} – ${dayFormat.format(last)}`));
    body.append(head);
    body.append(statRow([["memories", number.format(written.length + routine.length)], ["projects", perProject.size],
                         ["days", Math.max(1, Math.round(to - from))]]));
    const projects = [...perProject.entries()].sort((a, b) => b[1] - a[1]).slice(0, 12).flatMap(([p, count]) => {
      const sun = p === null ? undefined : graph.projectNode.get(p);
      return sun === undefined ? [] : [{ ...this.#row(sun), when: number.format(count) }];
    });
    body.append(relations("By project", projects, (i) => host.go(i)));
    const latest = (/** @type {number} */ a, /** @type {number} */ b) => (graph.nodes[b].t || 0) - (graph.nodes[a].t || 0);
    body.append(relations("Written then", written.sort(latest).map((i) => this.#row(i)), (i) => host.go(i)));
    body.append(relations("Routine records", routine.sort(latest).map((i) => this.#row(i)), (i) => host.go(i)));
    if (!written.length && !routine.length) body.append(el("p", "sub", "Nothing was written in this stretch."));
  }
}

export { DAY };
