// @ts-check
/**
 * The side panel: one memory in full, and the ways out of it.
 *
 * Everything here is built with DOM nodes and textContent. Memory text is
 * written by agents, so it is never parsed as HTML; notes get a small
 * markdown reading (headings, lists, code, bold, links) built the same way.
 */

const LIST_STEP = 12;

/**
 * @param {string} tag @param {string} [className] @param {string} [text]
 * @returns {HTMLElement}
 */
export function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

/**
 * Inline markdown: **bold**, `code`, [[note]] and [text](target).
 * @param {HTMLElement} parent @param {string} text @param {(name: string) => (() => void) | null} resolve
 */
function inline(parent, text, resolve) {
  // Bounded runs: an unclosed "[" or "**" must not make every later position rescan the line.
  const pattern = /(\*\*[^*\n]{1,400}\*\*|`[^`\n]{1,400}`|\[\[[^\]\n]{1,200}\]\]|\[[^\]\n]{1,400}\]\([^)\s]{1,400}\))/g;
  let last = 0;
  for (const match of text.matchAll(pattern)) {
    const index = match.index || 0;
    if (index > last) parent.append(text.slice(last, index));
    const token = match[0];
    if (token.startsWith("**")) parent.append(el("strong", "", token.slice(2, -2)));
    else if (token.startsWith("`")) parent.append(el("code", "", token.slice(1, -1)));
    else {
      const wiki = token.startsWith("[[");
      const label = wiki ? token.slice(2, -2).split("|").pop() || "" : token.slice(1, token.indexOf("]("));
      const target = wiki ? token.slice(2, -2).split("|")[0] : token.slice(token.indexOf("](") + 2, -1);
      const go = resolve(target);
      if (go) {
        const link = el("a", "link", label);
        link.setAttribute("role", "link");
        link.tabIndex = 0;
        link.addEventListener("click", go);
        parent.append(link);
      } else {
        parent.append(label);
      }
    }
    last = index + token.length;
  }
  if (last < text.length) parent.append(text.slice(last));
}

/**
 * A safe reading of a markdown note. Indexed notes carry their sections as
 * "## Title › Section": the title is already the panel's heading, so it goes.
 * @param {string} source @param {(name: string) => (() => void) | null} resolve @param {string} [title]
 */
export function markdown(source, resolve, title = "") {
  const prefix = title ? `${title} › ` : "";
  const root = el("div", "text md");
  const body = source.replace(/^---\n[\s\S]*?\n---\n?/, "");
  /** @type {HTMLElement | null} */ let list = null;
  /** @type {HTMLElement | null} */ let paragraph = null;
  /** @type {string[] | null} */ let fence = null;
  const close = () => { list = null; paragraph = null; };
  for (const line of body.split("\n")) {
    if (line.trimStart().startsWith("```")) {
      if (fence) { root.append(el("pre", "", fence.join("\n"))); fence = null; }
      else { close(); fence = []; }
      continue;
    }
    if (fence) { fence.push(line); continue; }
    const heading = line.match(/^#{1,6}\s+(.*)/);
    const item = line.match(/^\s*(?:[-*+]|\d+\.)\s+(.*)/);
    if (heading) {
      close();
      const text = prefix && heading[1].startsWith(prefix) ? heading[1].slice(prefix.length) : heading[1];
      if (text.trim() === title.trim()) continue;
      const h = el("h4");
      inline(h, text, resolve);
      root.append(h);
    } else if (item) {
      paragraph = null;
      if (!list) { list = el("ul"); root.append(list); }
      const li = el("li");
      inline(li, item[1], resolve);
      list.append(li);
    } else if (!line.trim()) {
      close();
    } else {
      list = null;
      if (!paragraph) { paragraph = el("p"); root.append(paragraph); }
      else paragraph.append(" ");
      inline(paragraph, line.trim(), resolve);
    }
  }
  if (fence) root.append(el("pre", "", fence.join("\n")));
  return root;
}

/**
 * @typedef {{index: number, label: string, color: string, when: string, note?: string}} Row
 */

/**
 * A titled list of nodes to go to, shown a dozen at a time.
 * @param {string} title @param {Row[]} rows @param {(index: number) => void} go
 */
export function relations(title, rows, go) {
  const section = el("section");
  if (!rows.length) return section;
  section.append(el("h3", "", `${title} · ${rows.length}`));
  const list = el("ul", "rel");
  section.append(list);
  let shown = 0;
  const more = el("button", "more");
  const step = () => {
    for (const row of rows.slice(shown, shown + LIST_STEP)) {
      const li = el("li");
      const button = el("button");
      button.setAttribute("type", "button");
      const dot = el("span", "dot");
      dot.style.background = row.color;
      dot.style.boxShadow = `0 0 8px ${row.color}`;
      button.append(dot, el("span", "label", row.label));
      if (row.note) button.append(el("span", "sim", row.note));
      button.append(el("span", "when", row.when));
      button.addEventListener("click", () => go(row.index));
      li.append(button);
      list.append(li);
    }
    shown += LIST_STEP;
    more.textContent = `Show ${Math.min(LIST_STEP, rows.length - shown)} more`;
    more.hidden = shown >= rows.length;
  };
  more.setAttribute("type", "button");
  more.addEventListener("click", step);
  step();
  section.append(more);
  return section;
}

/** @param {string} label @param {string} color */
export function kindChip(label, color) {
  const chip = el("span", "kind");
  const dot = el("i");
  dot.style.color = color;
  chip.append(dot, label);
  return chip;
}

/** @param {[string, string | number][]} stats */
export function statRow(stats) {
  const row = el("div", "stat-row");
  for (const [label, value] of stats) {
    const cell = el("div");
    cell.append(el("b", "", String(value)), el("span", "", label));
    row.append(cell);
  }
  return row;
}
