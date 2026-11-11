// @ts-check
/**
 * Where the reader is, in the address: the memory or the line in focus, the
 * day the time machine shows, the start of the range, one project alone, the
 * flat map. A reload, the back button and a link sent to oneself all land in
 * the same place.
 *
 *   #n=e:1f3a…                 a memory
 *   #l=e:1f3a…~n:77b0…&k=link  the line between two, from the first to the second, and its kind
 *   #t=2026-08-13&f=2026-07-01 the day shown, and where the range starts
 *   #p=max-chronicle           one project alone
 *   #m=2d                      the flat map
 *
 * Only what differs from the plain view is written. Dates are the reader's
 * own days, as every date on the page: the day picked in the calendar is the
 * day the label names and the day the address keeps.
 */

/**
 * @typedef {{node?: string, line?: [string, string], kind?: string, day?: string, from?: string, project?: string,
 *   flat?: boolean}} Place kind is the kind of the line: two memories can be joined by more than one
 */

const DATE = /^\d{4}-\d{2}-\d{2}$/;

/** @param {string} hash with or without its # @returns {Place} */
export function readPlace(hash) {
  /** @type {Map<string, string>} */ const raw = new Map();
  for (const part of hash.replace(/^#/, "").split("&")) {
    const at = part.indexOf("=");
    if (at > 0) raw.set(part.slice(0, at), part.slice(at + 1));
  }
  /** A value as it was written; one that does not decode is no value. @param {string | undefined} value */
  const text = (value) => { try { return decodeURIComponent(value || ""); } catch { return ""; } };
  /** @type {Place} */ const place = {};
  const node = text(raw.get("n")), day = text(raw.get("t")), from = text(raw.get("f")), project = text(raw.get("p"));
  // The two ends are split where the tilde was written: one inside an id is escaped.
  const ends = (raw.get("l") || "").split("~").map(text);
  if (ends.length === 2 && ends[0] && ends[1]) {
    place.line = [ends[0], ends[1]];
    if (text(raw.get("k"))) place.kind = text(raw.get("k"));
  } else if (node) place.node = node;
  if (DATE.test(day)) place.day = day;
  if (DATE.test(from)) place.from = from;
  if (project) place.project = project;
  if (raw.get("m") === "2d") place.flat = true;
  return place;
}

/** @param {Place} place @returns {string} the hash without its #; empty for the plain view */
export function writePlace(place) {
  const parts = [];
  // An id is written as it is but for what the address needs: a colon reads better than %3A,
  // and a tilde inside an id is escaped, since a bare one parts the two ends of a line.
  const plain = (/** @type {string} */ value) => encodeURIComponent(value).replace(/%3A/gi, ":").replace(/~/g, "%7E");
  if (place.line) parts.push(`l=${plain(place.line[0])}~${plain(place.line[1])}${place.kind ? `&k=${plain(place.kind)}` : ""}`);
  else if (place.node) parts.push(`n=${plain(place.node)}`);
  if (place.day) parts.push(`t=${place.day}`);
  if (place.from) parts.push(`f=${place.from}`);
  if (place.project) parts.push(`p=${plain(place.project)}`);
  if (place.flat) parts.push("m=2d");
  return parts.join("&");
}

/** @param {string} day @returns {[number, number, number] | null} year, month from zero, day of the month */
function parts(day) {
  if (!DATE.test(day)) return null;
  const [year, month, date] = day.split("-").map(Number);
  const made = new Date(year, month - 1, date);
  // The 31st of a month of thirty days rolls over: no day at all.
  return made.getFullYear() === year && made.getMonth() === month - 1 && made.getDate() === date ? [year, month - 1, date] : null;
}

/** The reader's day a moment falls on, as the address writes it. @param {number} seconds */
export function dayOf(seconds) {
  const at = new Date(seconds * 1000), two = (/** @type {number} */ value) => String(value).padStart(2, "0");
  return `${at.getFullYear()}-${two(at.getMonth() + 1)}-${two(at.getDate())}`;
}

/** The first second of a day written in the address; null when it is not a day. @param {string} day */
export function dayStart(day) {
  const found = parts(day);
  return found ? new Date(found[0], found[1], found[2]).getTime() / 1000 : null;
}

/** The last second of that day, however long the clocks made it; null when it is not a day. @param {string} day */
export function dayEnd(day) {
  const found = parts(day);
  return found ? new Date(found[0], found[1], found[2] + 1).getTime() / 1000 - 1 : null;
}
