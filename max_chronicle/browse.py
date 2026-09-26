"""Human-facing terminal browse renderer for Chronicle.

All functions return formatted strings or print directly to stdout.
Rich is used when available (detected at import time); otherwise plain-text
fallback is used. No heavy dependencies are added — rich is optional.
"""
from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import textwrap
from typing import Any

try:
    import rich  # noqa: F401 — presence check only
    from rich.console import Console
    from rich.table import Table
    from rich import box as rich_box
    _RICH = True
except ImportError:
    _RICH = False

# Maximum text width for event body in list views.
_TEXT_LIMIT = 100
# Maximum "why" width.
_WHY_LIMIT = 80
# Date prefix length for display (YYYY-MM-DD = 10 chars).
_DATE_WIDTH = 10


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _trunc(value: str | None, limit: int = _TEXT_LIMIT) -> str:
    if not value:
        return ""
    s = value.replace("\n", " ").strip()
    if len(s) <= limit:
        return s
    return s[: limit - 1] + "…"


def _date_prefix(timestamp: str | None) -> str:
    """Return YYYY-MM-DD from an ISO timestamp, or '??????????'."""
    if not timestamp:
        return "??????????"
    return timestamp[:10]


def _fmt_channels(channels: dict[str, Any]) -> str:
    """Build a compact channel annotation like 'fts/vec/rec'."""
    parts = []
    if channels.get("fts_rank") is not None:
        parts.append("fts")
    if channels.get("vector_similarity") is not None:
        parts.append("vec")
    if channels.get("recency_rank") is not None:
        parts.append("rec")
    return "/".join(parts) or "—"


# ---------------------------------------------------------------------------
# browse search
# ---------------------------------------------------------------------------


def render_search_results(payload: dict[str, Any]) -> None:
    """Print hybrid-recall results from query_memory in a ranked, scannable list."""
    query = payload.get("query") or ""
    results = payload.get("results") or []
    notes = payload.get("notes") or []
    degraded = bool(payload.get("degraded"))
    channels = ", ".join(payload.get("channels_used") or [])

    none = "no matching events." if notes else "no matches."
    if _RICH:
        _render_search_rich(query, results, degraded, channels, none)
    else:
        _render_search_plain(query, results, degraded, channels, none)
    for line in _note_lines(notes):
        print(line)


def _note_lines(notes: list[dict[str, Any]]) -> Iterator[str]:
    """Notes (FR-10) after the events: the best section of each, with the file it came from."""
    if notes:
        yield ""
        yield "Notes:"
    for rank, note in enumerate(notes, 1):
        name = (note.get("path") or "").rsplit("/", 1)[-1]
        yield f"n{rank:<3} {note.get('heading') or note.get('title') or '—'}  ({name})"
        yield f"     {_trunc(note.get('text'), _TEXT_LIMIT)} | id: {note.get('document_id') or '—'}"


def _render_search_plain(
    query: str,
    results: list[dict[str, Any]],
    degraded: bool,
    channels: str,
    none: str = "no matches.",
) -> None:
    print(f"Search: {query!r}  channels: {channels or 'none'}")
    if degraded:
        print("  [degraded: vector channel skipped — Ollama may be down]")
    print()
    if not results:
        print(f"  {none}")
        return
    for rank, hit in enumerate(results, 1):
        date = _date_prefix(hit.get("occurred_at_local") or hit.get("occurred_at_utc"))
        cat = hit.get("category") or "—"
        text = _trunc(hit.get("text"), _TEXT_LIMIT)
        rrf = hit.get("rrf_score")
        ch_label = _fmt_channels(hit.get("channels") or {})
        eid = hit.get("event_id") or "—"
        print(f"#{rank:<3} {date}  [{cat}]  {text}")
        rrf_str = f"{rrf:.4f}" if rrf is not None else "n/a"
        print(f"     why: {_trunc(hit.get('why') or '', _WHY_LIMIT) or '—'} | id: {eid} | score: {rrf_str} | ch: {ch_label}")


def _render_search_rich(
    query: str,
    results: list[dict[str, Any]],
    degraded: bool,
    channels: str,
    none: str = "no matches.",
) -> None:
    console = Console()
    console.print(f"[bold]Search:[/bold] {query!r}  [dim]channels: {channels or 'none'}[/dim]")
    if degraded:
        console.print("[yellow]  degraded: vector channel skipped — Ollama may be down[/yellow]")
    console.print()
    if not results:
        console.print(f"[dim]  {none}[/dim]")
        return
    table = Table(
        box=rich_box.SIMPLE,
        show_header=True,
        header_style="bold cyan",
        pad_edge=False,
        expand=False,
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Date", style="cyan", width=10)
    table.add_column("Category", style="green", width=22)
    table.add_column("Text", max_width=_TEXT_LIMIT)
    table.add_column("Score", style="dim", width=7)
    table.add_column("Ch.", style="dim", width=9)
    for rank, hit in enumerate(results, 1):
        date = _date_prefix(hit.get("occurred_at_local") or hit.get("occurred_at_utc"))
        cat = hit.get("category") or "—"
        text = _trunc(hit.get("text"), _TEXT_LIMIT)
        rrf = hit.get("rrf_score")
        rrf_str = f"{rrf:.4f}" if rrf is not None else "n/a"
        ch_label = _fmt_channels(hit.get("channels") or {})
        table.add_row(f"#{rank}", date, cat, text, rrf_str, ch_label)
        # dim line for why + id
        why = _trunc(hit.get("why") or "", _WHY_LIMIT)
        eid = hit.get("event_id") or "—"
        detail = f"why: {why or '—'} | id: {eid}"
        table.add_row("", "", "[dim]" + detail + "[/dim]", "", "", "")
    console.print(table)


# ---------------------------------------------------------------------------
# browse recent
# ---------------------------------------------------------------------------


def render_recent_events(events: list[dict[str, Any]], *, limit: int | None = None) -> None:
    """Print events as a reverse-chron timeline."""
    shown = events[:limit] if limit else events
    if _RICH:
        _render_recent_rich(shown)
    else:
        _render_recent_plain(shown)


def _render_recent_plain(events: list[dict[str, Any]]) -> None:
    if not events:
        print("  no recent events.")
        return
    for ev in events:
        ts = (ev.get("recorded_at") or ev.get("occurred_at_utc") or "")[:19]
        cat = ev.get("category") or "—"
        proj = ev.get("project") or "—"
        text = _trunc(ev.get("text"), _TEXT_LIMIT)
        print(f"{ts}  [{cat}]  {proj}")
        print(f"  {text}")
        if ev.get("why"):
            print(f"  why: {_trunc(ev['why'], _WHY_LIMIT)}")


def _render_recent_rich(events: list[dict[str, Any]]) -> None:
    console = Console()
    if not events:
        console.print("[dim]  no recent events.[/dim]")
        return
    table = Table(
        box=rich_box.SIMPLE,
        show_header=True,
        header_style="bold cyan",
        pad_edge=False,
        expand=False,
    )
    table.add_column("Timestamp", style="cyan", width=19)
    table.add_column("Category", style="green", width=22)
    table.add_column("Project", style="yellow", width=20)
    table.add_column("Text", max_width=_TEXT_LIMIT)
    for ev in events:
        ts = (ev.get("recorded_at") or ev.get("occurred_at_utc") or "")[:19]
        cat = ev.get("category") or "—"
        proj = ev.get("project") or "—"
        text = _trunc(ev.get("text"), _TEXT_LIMIT)
        table.add_row(ts, cat, proj, text)
        if ev.get("why"):
            table.add_row("", "", "[dim]why:[/dim]", "[dim]" + _trunc(ev["why"], _WHY_LIMIT) + "[/dim]")
    console.print(table)


# ---------------------------------------------------------------------------
# browse entity
# ---------------------------------------------------------------------------


def render_entity_timeline(
    name: str,
    events: list[dict[str, Any]],
    relations: list[dict[str, Any]],
) -> None:
    """Print events and relations touching an entity, chronologically."""
    if _RICH:
        _render_entity_rich(name, events, relations)
    else:
        _render_entity_plain(name, events, relations)


def _render_entity_plain(
    name: str,
    events: list[dict[str, Any]],
    relations: list[dict[str, Any]],
) -> None:
    print(f"Entity: {name}")
    print()
    print("Events:")
    if not events:
        print("  none.")
    else:
        for ev in reversed(events):  # chronological order (events come desc)
            ts = (ev.get("recorded_at") or ev.get("occurred_at_utc") or "")[:10]
            cat = ev.get("category") or "—"
            text = _trunc(ev.get("text"), _TEXT_LIMIT)
            print(f"  {ts}  [{cat}]  {text}")
    print()
    print("Relations:")
    if not relations:
        print("  none.")
    else:
        for rel in relations:
            print(f"  {rel.get('from_name') or rel.get('from_entity_id')}  --[{rel.get('relation_type')}]-->  {rel.get('to_name') or rel.get('to_entity_id')}")
            if rel.get("rationale"):
                print(f"    rationale: {_trunc(rel['rationale'], _WHY_LIMIT)}")


def _render_entity_rich(
    name: str,
    events: list[dict[str, Any]],
    relations: list[dict[str, Any]],
) -> None:
    from rich.panel import Panel

    console = Console()
    console.print(Panel(f"[bold]{name}[/bold]", expand=False))

    # Events table
    console.print("[bold cyan]Events[/bold cyan]")
    if not events:
        console.print("[dim]  none.[/dim]")
    else:
        table = Table(box=rich_box.SIMPLE, show_header=True, header_style="bold", pad_edge=False)
        table.add_column("Date", style="cyan", width=10)
        table.add_column("Category", style="green", width=22)
        table.add_column("Text", max_width=_TEXT_LIMIT)
        for ev in reversed(events):
            ts = (ev.get("recorded_at") or ev.get("occurred_at_utc") or "")[:10]
            cat = ev.get("category") or "—"
            text = _trunc(ev.get("text"), _TEXT_LIMIT)
            table.add_row(ts, cat, text)
        console.print(table)

    console.print()
    console.print("[bold cyan]Relations[/bold cyan]")
    if not relations:
        console.print("[dim]  none.[/dim]")
    else:
        for rel in relations:
            frm = rel.get("from_name") or rel.get("from_entity_id") or "?"
            rtype = rel.get("relation_type") or "?"
            to = rel.get("to_name") or rel.get("to_entity_id") or "?"
            console.print(f"  [yellow]{frm}[/yellow] --[{rtype}]--> [yellow]{to}[/yellow]")
            if rel.get("rationale"):
                console.print(f"    [dim]{_trunc(rel['rationale'], _WHY_LIMIT)}[/dim]")


# ---------------------------------------------------------------------------
# browse daybook
# ---------------------------------------------------------------------------


def list_daybook_dates(daybook_dir: Path, count: int = 10) -> list[Path]:
    """Return the last `count` daybook markdown files, most-recent first."""
    files: list[Path] = []
    for year_dir in sorted(daybook_dir.iterdir(), reverse=True):
        if not year_dir.is_dir():
            continue
        for f in sorted(year_dir.glob("*.md"), reverse=True):
            files.append(f)
            if len(files) >= count:
                return files
    return files


def render_daybook(
    daybook_dir: Path,
    date_str: str | None = None,
    *,
    list_count: int = 10,
) -> int:
    """Print a daybook file for `date_str`, or list recent dates and exit.

    Returns 0 on success, 1 if the file is not found.
    """
    if date_str is None:
        recent = list_daybook_dates(daybook_dir, list_count)
        if not recent:
            _print_line("No daybook files found.")
            return 0
        _print_line("Recent daybooks:")
        for f in recent:
            _print_line(f"  {f.stem}  {f}")
        _print_line()
        _print_line("Use --date YYYY-MM-DD to view a specific day.")
        return 0

    # Search by date stem across year subdirectories
    year = date_str[:4]
    candidate = daybook_dir / year / f"{date_str}.md"
    if not candidate.exists():
        # Fall back: search all year dirs
        found = None
        for year_dir in daybook_dir.iterdir():
            p = year_dir / f"{date_str}.md"
            if p.exists():
                found = p
                break
        if found is None:
            _print_line(f"No daybook found for {date_str}.")
            return 1
        candidate = found

    content = candidate.read_text(encoding="utf-8")
    if _RICH:
        from rich.markdown import Markdown
        Console().print(Markdown(content))
    else:
        print(content)
    return 0


def _print_line(text: str = "") -> None:
    """Print a line, using rich Console when available for consistent output."""
    if _RICH:
        Console().print(text)
    else:
        print(text)


# ---------------------------------------------------------------------------
# browse help (no subcommand)
# ---------------------------------------------------------------------------


def render_browse_help() -> None:
    lines = [
        "chronicle browse — human-facing archive navigation",
        "",
        "  search \"<query>\" [--limit N] [--domain D]",
        "       Hybrid recall (FTS + vector + recency). Ranked, readable list.",
        "",
        "  recent [--limit N] [--domain D]",
        "       Recent events as a reverse-chron timeline.",
        "",
        "  entity \"<name>\"",
        "       Events + relations touching a named entity over time.",
        "",
        "  daybook [--date YYYY-MM-DD]",
        "       Print a daybook markdown. Omit --date to list recent dates.",
    ]
    if _RICH:
        Console().print("\n".join(lines))
    else:
        print("\n".join(lines))
