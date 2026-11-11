"""A read-only web view of the memory, for people.

Agents read Chronicle through MCP; a person had the markdown projections and
the terminal. These pages are served by the same HTTP server under ``/ui``:
an overview (health, projects, open tasks, latest events), a project, a task
with its checkpoint history, the current facts with the chain of values each
slot had, a search that says why each hit matched, and single events and
notes with their evidence.

Every page is rendered on the server, with no script: the memory is written
by agents, so everything shown is escaped text, and a Content-Security-Policy
forbids scripts, frames and outside requests. Only this machine may ask (the
Host must be a loopback name, and an Origin, if sent, must be this server's).
Quarantined events never appear.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from html import escape
import json
import re
from typing import Any
from urllib.parse import quote, unquote, urlencode

from . import curation
from .agent_surface import get_items
from .brief import _read_only, _warnings
from .memory import _scope_filter, open_tasks
from .recall import query_memory
from .store import config_from_manifest

# Only this machine: DNS rebinding arrives with a foreign Host, another page's
# request with its own Origin or a cross-site Sec-Fetch-Site.
_LOOPBACK_HOST = re.compile(r"(127\.0\.0\.1|localhost|\[::1\])(:\d{1,5})?")
_SAME_SITE = {None, "same-origin", "none"}
PAGE_EVENTS = 30
OVERVIEW_EVENTS = 20
PROJECT_DAYS = 30
QUARANTINE = "e.visibility != 'raw_only'"
CSP = ("default-src 'none'; style-src 'unsafe-inline'; img-src 'self' data:; "
       "form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
SECURITY_HEADERS = {
    "Content-Security-Policy": CSP,
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "no-store",
}

STYLE = """
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--line:#e2e1dc;--card:#fff;--accent:#2f5f8a;--warn:#9a5b00;--ok:#2e7d4f}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--fg:#ecebe6;--muted:#9a9993;--line:#2c2c2a;--card:#1f1f1d;--accent:#8db8e0;--warn:#e0a34a;--ok:#6cc08b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header{border-bottom:1px solid var(--line);padding:12px 16px;display:flex;gap:16px;align-items:center;flex-wrap:wrap}
header b{font-size:16px}nav a{margin-right:14px}main{max-width:1080px;margin:0 auto;padding:16px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
h1{font-size:20px;margin:8px 0 12px}h2{font-size:16px;margin:24px 0 8px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px;margin:8px 0}
.muted{color:var(--muted)}.warn{color:var(--warn)}.ok{color:var(--ok)}
table{border-collapse:collapse;width:100%}td,th{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-weight:600;color:var(--muted);font-size:13px}code,pre{font:13px/1.45 ui-monospace,Menlo,monospace}
pre{white-space:pre-wrap;word-break:break-word;margin:0}.pill{display:inline-block;border:1px solid var(--line);border-radius:10px;padding:0 7px;font-size:12px;color:var(--muted)}
form input{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg);min-width:min(420px,100%)}
form button{font:inherit;padding:6px 12px;border-radius:6px;border:1px solid var(--line);background:var(--card);color:var(--fg)}
ul{margin:4px 0;padding-left:20px}
"""


def _e(value: Any) -> str:
    return escape("" if value is None else str(value))


def _when(value: str | None) -> str:
    return _e((value or "")[:16].replace("T", " "))


def _link(path: str, label: Any) -> str:
    return f'<a href="{_e(path)}">{_e(label)}</a>'


def _event_link(event_id: str, label: Any | None = None) -> str:
    return _link(f"/ui/event/{quote(event_id, safe='')}", label if label is not None else event_id[:8])


def _project_link(project: str | None) -> str:
    return _link(f"/ui/project/{quote(project, safe='')}", project) if project else '<span class="muted">—</span>'


def _task_link(project: str | None, task_id: str | None) -> str:
    if not (project and task_id):
        return '<span class="muted">—</span>'
    return _link(f"/ui/task/{quote(project, safe='')}/{quote(task_id, safe='')}", task_id)


def _page(title: str, body: str) -> str:
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        f"<title>{_e(title)} · Chronicle</title><style>{STYLE}</style></head><body>"
        "<header><b>Chronicle</b><nav>"
        + _link("/ui", "Overview") + _link("/ui/facts", "Facts") + _link("/ui/search", "Search")
        + _link("/ui/atlas", "Atlas")
        + "</nav></header>"
        f"<main><h1>{_e(title)}</h1>{body}</main></body></html>"
    )


def _list(items: list[str]) -> str:
    return "<ul>" + "".join(f"<li>{_e(item)}</li>" for item in items) + "</ul>" if items else ""


def _events_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return '<p class="muted">Nothing recorded.</p>'
    lines = ["<table><tr><th>When (UTC)</th><th>Who</th><th>Project</th><th>Task</th><th>What</th></tr>"]
    for row in rows:
        text = " ".join((row.get("text") or "").split())
        gist = text if len(text) <= 220 else text[:219] + "…"
        lines.append(
            f"<tr><td>{_when(row.get('recorded_at_utc'))}</td><td>{_e(row.get('actor'))}</td>"
            f"<td>{_project_link(row.get('project'))}</td><td>{_task_link(row.get('project'), row.get('task_id'))}</td>"
            f"<td><span class=\"pill\">{_e(row.get('category'))}</span> {_event_link(row['id'], gist)}</td></tr>")
    return "".join(lines) + "</table>"


def _recent_events(connection, identities, *, project: str | None = None, task_id: str | None = None,
                   limit: int = PAGE_EVENTS) -> list[dict[str, Any]]:
    where, params = _scope_filter(identities.scope(project=project, task_id=task_id))
    # With one MAX() in the select list, SQLite takes the other columns from that row.
    rows = connection.execute(
        f"""SELECT e.id, e.recorded_at_utc, e.actor, e.category, e.text, o.project, o.task_id, MAX(o.seq) AS last_seq
        FROM events e JOIN event_observations o ON o.event_id = e.id
        WHERE {where}
        GROUP BY e.id ORDER BY last_seq DESC LIMIT ?""",
        (*params, limit)).fetchall()
    return [dict(row) | {"project": identities.project(row["project"])} for row in rows]


def _checkpoint_html(checkpoint: dict[str, Any] | None) -> str:
    if not checkpoint:
        return '<p class="muted">No checkpoint.</p>'
    parts = [f"<p><b>Goal:</b> {_e(checkpoint.get('goal'))}</p>"]
    for key, label in (("completed", "Completed"), ("verification", "Verification"),
                       ("open_questions", "Open questions"), ("next_steps", "Next steps")):
        if checkpoint.get(key):
            parts.append(f"<p class=\"muted\">{label}</p>{_list(checkpoint[key])}")
    return "".join(parts)


def _health(runtime: dict[str, Any]) -> str:
    cells = []
    loop = runtime.get("loop")
    if loop:
        lag = loop.get("max_lag_ms_5m", 0)
        cells.append(f"<span class=\"{'warn' if lag >= 1000 else 'ok'}\">loop lag {lag} ms (5 min max)</span>")
    if runtime.get("last_write"):
        last = runtime["last_write"]
        cells.append(f"last write {_e(last.get('at'))} ({_e(last.get('ms'))} ms)")
    embedder = (runtime.get("embedder") or {}).get("last_report") or {}
    if embedder:
        state = "Ollama down" if embedder.get("backend_unavailable") else f"{embedder.get('total_without_embedding', 0)} unindexed"
        cells.append(f"vectors: {_e(state)}")
    for run in (runtime.get("scheduler") or {}).get("last_runs", [])[-3:]:
        cells.append(f"{_e(run.get('job'))} exit {_e(run.get('exit'))}")
    return " · ".join(cells)


def overview(manifest: dict[str, Any], runtime: dict[str, Any] | None = None) -> str:
    config = config_from_manifest(manifest)
    identities = config.identities
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=PROJECT_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _read_only(config) as connection:
        tasks = open_tasks(connection, identities, limit=20)
        events = _recent_events(connection, identities, limit=OVERVIEW_EVENTS)
        where, params = _scope_filter(identities.scope())
        projects = connection.execute(
            f"""SELECT o.project, COUNT(DISTINCT e.id) AS events, MAX(o.recorded_at_utc) AS last
            FROM events e JOIN event_observations o ON o.event_id = e.id
            WHERE {where} AND o.project IS NOT NULL AND o.recorded_at_utc >= ?
            GROUP BY o.project ORDER BY last DESC""", (*params, since)).fetchall()
        backup = connection.execute(
            "SELECT started_at_utc FROM backup_runs WHERE status = 'ok' ORDER BY started_at_utc DESC LIMIT 1").fetchone()
        warnings = _warnings(connection, now)
    merged: dict[str, dict[str, Any]] = {}
    for row in projects:
        name = identities.project(row["project"]) or row["project"]
        entry = merged.setdefault(name, {"events": 0, "last": ""})
        entry["events"] += row["events"]
        entry["last"] = max(entry["last"], row["last"] or "")
    body = [f"<div class=\"card\">{_health(runtime or {}) or '<span class=muted>Process details appear when served by the HTTP server.</span>'}"
            f"<br><span class=\"muted\">last good backup {_when(backup[0]) if backup else 'never'}</span></div>"]
    if warnings:
        body.append("<div class=\"card warn\">" + "<br>".join(_e(w) for w in warnings) + "</div>")
    body.append("<h2>Open tasks</h2>")
    body.append("".join(
        f"<div class=\"card\">{_project_link(t['project'])} / {_task_link(t['project'], t['task_id'])} "
        f"<span class=\"muted\">{_e(t['agent'])}, {_when(t['recorded_at_utc'])}</span><br>{_e(t['goal'])}"
        f"{_list(t['next_steps'])}</div>" for t in tasks) or '<p class="muted">No open task.</p>')
    body.append(f"<h2>Projects, last {PROJECT_DAYS} days</h2><table><tr><th>Project</th><th>Events</th><th>Last</th></tr>"
                + "".join(f"<tr><td>{_project_link(name)}</td><td>{entry['events']}</td><td>{_when(entry['last'])}</td></tr>"
                          for name, entry in sorted(merged.items(), key=lambda item: item[1]["last"], reverse=True))
                + "</table>")
    body.append("<h2>Latest events</h2>" + _events_table(events))
    return _page("Overview", "".join(body))


def _current_facts(connection, identities, *, project: str | None = None, task_id: str | None = None):
    where, params = identities.scope(project=project, task_id=task_id).where(
        domain="f.domain", project="f.project", task="f.task_id", task_or_unset=True)
    return connection.execute(
        f"""SELECT f.id, f.slot_key, f.value_key, f.recorded_at_utc, f.attributed_to,
                  f.project, f.task_id, f.kind, f.event_id
        FROM current_facts f JOIN events e ON e.id = f.event_id
        WHERE {QUARANTINE} AND {where} ORDER BY f.slot_key""", params).fetchall()


def _facts_table(rows) -> str:
    if not rows:
        return '<p class="muted">No current fact.</p>'
    return ("<table><tr><th>Slot</th><th>Value</th><th>Scope</th><th>By</th><th>Since</th></tr>"
            + "".join(
                f"<tr><td>{_link('/ui/facts?' + urlencode({'slot': row['slot_key']}), row['slot_key'])}</td>"
                f"<td>{_e(row['value_key'])}{' <span class=pill>assumption</span>' if row['kind'] == 'assumption' else ''}</td>"
                f"<td>{_project_link(row['project'])} {_e(row['task_id'] or '')}</td><td>{_e(row['attributed_to'])}</td>"
                f"<td>{_event_link(row['event_id'], _when(row['recorded_at_utc'])) if row['event_id'] else _when(row['recorded_at_utc'])}</td></tr>"
                for row in rows) + "</table>")


def project_page(manifest: dict[str, Any], project: str) -> str:
    config = config_from_manifest(manifest)
    identities = config.identities
    name = identities.project(project) or project
    with _read_only(config) as connection:
        tasks = open_tasks(connection, identities, project=name, limit=20)
        facts = _current_facts(connection, identities, project=name)
        events = _recent_events(connection, identities, project=name)
        hidden = curation.marks(connection).hidden_notes()  # copies hidden in the Atlas
        notes = sum(1 for (identifier,) in connection.execute(
            "SELECT id FROM documents WHERE deleted_at_utc IS NULL AND project = ?", (name,)) if identifier not in hidden)
    body = [f"<p class=\"muted\">{notes} indexed notes · "
            + _link("/ui/search?" + urlencode({"project": name}), "search this project") + "</p>", "<h2>Open tasks</h2>"]
    body.append("".join(
        f"<div class=\"card\">{_task_link(t['project'], t['task_id'])} <span class=\"muted\">{_e(t['agent'])}, "
        f"{_when(t['recorded_at_utc'])}</span><br>{_e(t['goal'])}{_list(t['next_steps'])}</div>" for t in tasks)
        or '<p class="muted">No open task.</p>')
    body.append("<h2>Current facts</h2>" + _facts_table(facts))
    body.append("<h2>Latest events</h2>" + _events_table(events))
    return _page(f"Project {name}", "".join(body))


def task_page(manifest: dict[str, Any], project: str, task_id: str) -> str:
    config = config_from_manifest(manifest)
    identities = config.identities
    name = identities.project(project) or project
    with _read_only(config) as connection:
        where, params = _scope_filter(identities.scope(project=name, task_id=task_id))
        history = connection.execute(
            f"""SELECT o.event_id, o.actor, o.recorded_at_utc, json_extract(o.payload_json,'$.checkpoint') AS checkpoint
            FROM event_observations o JOIN events e ON e.id = o.event_id
            WHERE {where} AND json_type(o.payload_json,'$.checkpoint') = 'object'
            ORDER BY o.seq DESC LIMIT 20""", params).fetchall()
        facts = _current_facts(connection, identities, project=name, task_id=task_id)
        events = _recent_events(connection, identities, project=name, task_id=task_id, limit=50)
    checkpoints = [
        f"<div class=\"card\"><span class=\"muted\">{_e(row['actor'])}, "
        f"{_event_link(row['event_id'], _when(row['recorded_at_utc']))}</span>"
        f"{_checkpoint_html(json.loads(row['checkpoint']))}</div>" for row in history]
    body = [f"<p>{_project_link(name)}</p><h2>Latest checkpoint</h2>",
            checkpoints[0] if checkpoints else '<p class="muted">No checkpoint.</p>',
            "<h2>Current facts</h2>" + _facts_table(facts)]
    if len(checkpoints) > 1:
        body.append("<h2>Earlier checkpoints</h2>" + "".join(checkpoints[1:]))
    body.append("<h2>Events of this task</h2>" + _events_table(events))
    return _page(f"Task {name} / {task_id}", "".join(body))


def facts_page(manifest: dict[str, Any], slot: str | None = None) -> str:
    config = config_from_manifest(manifest)
    identities = config.identities
    with _read_only(config) as connection:
        if not slot:
            return _page("Current facts", _facts_table(_current_facts(connection, identities)))
        rows = connection.execute(
            f"""SELECT f.id, f.value_key, f.status, f.recorded_at_utc, f.expired_at_utc, f.attributed_to,
                      f.project, f.event_id,
                      (SELECT s.new_fact_id FROM fact_supersessions s WHERE s.old_fact_id = f.id LIMIT 1) AS replaced_by
            FROM facts f JOIN events e ON e.id = f.event_id
            WHERE {QUARANTINE} AND f.slot_key = ? ORDER BY f.recorded_at_utc DESC""", (slot,)).fetchall()
    body = ("<p class=\"muted\">Every value this slot had, newest first.</p>"
            "<table><tr><th>Value</th><th>Project</th><th>By</th><th>Recorded</th><th>State</th></tr>"
            + "".join(
                f"<tr><td>{_e(row['value_key'])}</td><td>{_project_link(row['project'])}</td><td>{_e(row['attributed_to'])}</td>"
                f"<td>{_event_link(row['event_id'], _when(row['recorded_at_utc'])) if row['event_id'] else _when(row['recorded_at_utc'])}</td>"
                f"<td>{'<span class=ok>current</span>' if row['status'] == 'active' and not row['expired_at_utc'] else 'replaced ' + _when(row['expired_at_utc'])}</td></tr>"
                for row in rows) + "</table>")
    return _page(f"Fact {slot}", body)


def search_page(manifest: dict[str, Any], query: str | None, project: str | None) -> str:
    form = ("<form method=\"get\" action=\"/ui/search\"><input name=\"q\" placeholder=\"What to recall\" "
            f"value=\"{_e(query)}\"> <input name=\"project\" placeholder=\"project (optional)\" value=\"{_e(project)}\" "
            "style=\"min-width:160px\"> <button>Search</button></form>")
    if not query:
        return _page("Search", form)
    response = query_memory(manifest, query=query, project=project or None, limit=20)
    flags = []
    if response.get("no_confident_match"):
        flags.append("<span class=\"warn\">no confident match: treat these as leads</span>")
    if response.get("relaxed"):
        flags.append("relaxed word-stem match")
    if response.get("degraded"):
        flags.append("<span class=\"warn\">degraded: " + _e(", ".join(response.get("channel_errors") or {})) + "</span>")
    rows = []
    for hit in response.get("results", []):
        channels = hit.get("channels") or {}
        why = []
        if channels.get("fts_rank") is not None:
            why.append(f"words (rank {channels['fts_rank'] + 1})")
        if channels.get("vector_similarity") is not None:
            why.append(f"meaning ({channels['vector_similarity']:.2f})")
        rows.append(f"<tr><td>{_when(hit.get('occurred_at_utc'))}</td><td>{_project_link(hit.get('project'))}</td>"
                    f"<td>{_event_link(hit['event_id'], ' '.join((hit.get('text') or '').split())[:200])}</td>"
                    f"<td class=\"muted\">{_e(', '.join(why))}</td></tr>")
    notes = []
    for note in response.get("notes", []):
        channels = note.get("channels") or {}
        why = [part for part in (
            "words" if channels.get("fts_rank") is not None else None,
            f"meaning ({channels['vector_similarity']:.2f})" if channels.get("vector_similarity") is not None else None) if part]
        notes.append(f"<tr><td>{_link('/ui/note/' + quote(note['document_id'], safe=''), note.get('title') or note['document_id'])}</td>"
                     f"<td>{_e(note.get('heading'))}</td><td class=\"muted\">{_e(', '.join(why))}</td></tr>")
    body = [form, "<p>" + " · ".join(flags) + "</p>" if flags else ""]
    body.append("<h2>Events</h2>" + ("<table><tr><th>When</th><th>Project</th><th>Text</th><th>Matched by</th></tr>"
                                     + "".join(rows) + "</table>" if rows else '<p class="muted">No event.</p>'))
    body.append("<h2>Notes</h2>" + ("<table><tr><th>Note</th><th>Section</th><th>Matched by</th></tr>"
                                    + "".join(notes) + "</table>" if notes else '<p class="muted">No note.</p>'))
    return _page(f"Search: {query}", "".join(body))


def _marks_html(item: dict[str, Any]) -> str:
    """What the operator marked on a record in the Atlas, when anything."""
    marks = [f"<span class=\"pill\">{label}</span> since {_when(item[key].get('since'))} by {_e(item[key].get('by'))}"
             for key, label in (("pinned", "pinned to the brief"), ("stale", "stale")) if item.get(key)]
    return f"<p class=\"muted\">{' · '.join(marks)}</p>" if marks else ""


def item_page(manifest: dict[str, Any], identifier: str) -> str | None:
    found = get_items(manifest, [identifier])["items"]
    if not found:
        return None
    item = found[0]
    if item["kind"] == "note":
        body = (f"<p class=\"muted\">{_e(item.get('path'))} · modified {_when(item.get('modified_at_utc'))}</p>"
                + _marks_html(item) + f"<div class=\"card\"><pre>{_e(item.get('text'))}</pre></div>")
        return _page(item.get("title") or "Note", body)
    parts = [f"<p class=\"muted\">{_e(item.get('agent'))} · {_when(item.get('recorded_at'))} · "
             f"{_project_link(item.get('project'))} {_task_link(item.get('project'), item.get('task_id'))} · "
             f"<span class=\"pill\">{_e(item.get('category'))}</span></p>",
             _marks_html(item),
             f"<div class=\"card\"><pre>{_e(item.get('text'))}</pre></div>"]
    if item.get("why"):
        parts.append(f"<h2>Why</h2><div class=\"card\"><pre>{_e(item['why'])}</pre></div>")
    if item.get("checkpoint"):
        parts.append("<h2>Checkpoint</h2><div class=\"card\">" + _checkpoint_html(item["checkpoint"]) + "</div>")
    if item.get("fact"):
        fact = item["fact"]
        parts.append(f"<h2>Fact</h2><div class=\"card\">{_link('/ui/facts?' + urlencode({'slot': fact.get('slot', '')}), fact.get('slot'))}"
                     f" = {_e(fact.get('value'))} <span class=\"pill\">{_e(fact.get('kind'))}</span></div>")
    if item.get("source_files"):
        parts.append("<h2>Evidence files</h2>" + _list([str(path) for path in item["source_files"]]))
    observations = (item.get("provenance") or {}).get("observations") or []
    if observations:
        parts.append("<h2>Recorded by</h2><table><tr><th>Agent</th><th>When</th><th>Session</th></tr>" + "".join(
            f"<tr><td>{_e(o.get('agent'))}</td><td>{_when(o.get('recorded_at_utc'))}</td><td class=\"muted\">{_e(o.get('session_id'))}</td></tr>"
            for o in observations) + "</table>")
    return _page("Event", "".join(parts))


def allowed(headers: Mapping[str, str]) -> bool:
    """Whether a request comes from this machine's own browser tab or tool, not from another site."""
    host = headers.get("host", "")
    if not _LOOPBACK_HOST.fullmatch(host):
        return False
    origin = headers.get("origin")
    if origin is not None and origin != f"http://{host}":
        return False
    return headers.get("sec-fetch-site") in _SAME_SITE


def render(manifest: dict[str, Any], segments: list[str], query: Mapping[str, str],
           runtime: Callable[[], dict[str, Any]] | None = None) -> tuple[int, str]:
    """The status and page for the path *segments* after ``/ui``."""
    match segments:
        case [] | [""]:
            return 200, overview(manifest, runtime() if runtime else None)
        case ["project", project]:
            return 200, project_page(manifest, project)
        case ["task", project, task_id]:
            return 200, task_page(manifest, project, task_id)
        case ["facts"]:
            return 200, facts_page(manifest, query.get("slot") or None)
        case ["search"]:
            return 200, search_page(manifest, (query.get("q") or "").strip() or None,
                                    (query.get("project") or "").strip() or None)
        case ["event" | "note" as kind, identifier]:
            page = item_page(manifest, f"{kind}:{identifier}")
            if page is not None:
                return 200, page
    return 404, _page("Not found", '<p class="muted">Nothing here. ' + _link("/ui", "Back to the overview") + "</p>")


def segments_of(raw_path: bytes | str) -> list[str]:
    """The decoded path segments after ``/ui``; an encoded slash stays inside its segment."""
    path = raw_path.decode("latin-1") if isinstance(raw_path, bytes) else raw_path
    parts = path.split("?", 1)[0].split("/")[2:]
    return [unquote(part) for part in parts] if parts != [""] else []


def error_page(message: str) -> str:
    return _page("Error", f'<p class="warn">{_e(message)}</p>')
