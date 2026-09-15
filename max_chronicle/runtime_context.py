from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import threading
import tomllib
from typing import Any
import uuid
from zoneinfo import ZoneInfo

from .config import ACTIVATION_CONTRACT_NAME, ACTIVATION_CONTRACT_VERSION
from .db import utc_now  # re-exported: one timestamp format for the whole package


def read_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def expand_path(path_str: str) -> Path:
    return Path(path_str).expanduser()


def normalize_heading(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


def parse_markdown_sections(text: str) -> list[dict[str, Any]]:
    lines = text.splitlines()
    headings: list[dict[str, Any]] = []
    for idx, line in enumerate(lines):
        match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if not match:
            continue
        headings.append(
            {
                "level": len(match.group(1)),
                "title": match.group(2).strip(),
                "line": idx,
            }
        )

    sections: list[dict[str, Any]] = []
    for index, heading in enumerate(headings):
        end_line = len(lines)
        for candidate in headings[index + 1 :]:
            if candidate["level"] <= heading["level"]:
                end_line = candidate["line"]
                break
        body = "\n".join(lines[heading["line"] : end_line]).strip()
        sections.append(
            {
                "title": heading["title"],
                "normalized": normalize_heading(heading["title"]),
                "body": body,
            }
        )
    return sections


def extract_markdown_content(path: Path, source: dict[str, Any]) -> str:
    text = path.read_text(encoding="utf-8", errors="replace").strip()
    headings = source.get("headings") or []
    if not headings:
        return text

    sections = parse_markdown_sections(text)
    wanted = {normalize_heading(item) for item in headings}
    picked = [section["body"] for section in sections if section["normalized"] in wanted]
    if picked:
        return "\n\n".join(picked).strip()
    return text


def shorten(text: str, max_chars: int = 3000) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def load_manifest(path: Path) -> dict[str, Any]:
    raw = read_toml(path)
    raw["source_map"] = {source["id"]: source for source in raw.get("sources", [])}
    raw["domain_map"] = {domain["id"]: domain for domain in raw.get("domains", [])}
    return raw


_manifest_cache: dict[Path, tuple[tuple[int, int], dict[str, Any]]] = {}
_manifest_cache_lock = threading.Lock()


def load_manifest_cached(path: Path) -> dict[str, Any]:
    """load_manifest with an mtime+size guard.

    The long-lived MCP daemon re-parsed SSOT_MANIFEST.toml on every tool call,
    resource read and prompt render. Callers treat the result as read-only
    except for the copy-then-assign idiom on top-level keys (see cli.py), so
    each caller gets its own shallow copy while the parsed body is shared.
    """
    resolved = Path(path)
    try:
        stat = resolved.stat()
        stamp = (stat.st_mtime_ns, stat.st_size)
    except OSError:
        return load_manifest(resolved)

    with _manifest_cache_lock:
        cached = _manifest_cache.get(resolved)
        if cached is not None and cached[0] == stamp:
            return dict(cached[1])

    loaded = load_manifest(resolved)
    with _manifest_cache_lock:
        _manifest_cache[resolved] = (stamp, loaded)
    return dict(loaded)


def file_meta(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def tokenize(query: str) -> list[str]:
    return [token for token in re.findall(r"[A-Za-zА-Яа-я0-9$+._-]+", query.casefold()) if len(token) >= 2]


def score_text(haystack: str, tokens: list[str], raw_query: str) -> float:
    lowered = haystack.casefold()
    if not lowered:
        return 0.0
    token_hits = sum(1 for token in tokens if token in lowered)
    exact_bonus = 1.5 if raw_query.casefold() in lowered else 0.0
    return float(token_hits) + exact_bonus


def _normalize_mem0_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def _normalize_mem0_metadata(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, str):
            normalized[key] = re.sub(r"\s+", " ", item.strip())
            continue
        normalized[key] = item
    return normalized


def _mem0_result_key(item: dict[str, Any]) -> str:
    text = _normalize_mem0_text(str(item.get("memory", "")))
    metadata = _normalize_mem0_metadata(item.get("metadata", {}) or {})
    if text:
        return json.dumps({"memory": text, "metadata": metadata}, ensure_ascii=False, sort_keys=True)
    return json.dumps({"id": item.get("id")}, ensure_ascii=False, sort_keys=True)


def search_source_blocks(path: Path, query: str, limit: int) -> list[dict[str, Any]]:
    tokens = tokenize(query)
    hits: list[dict[str, Any]] = []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    block_lines: list[str] = []
    block_start = 1

    def flush_block() -> None:
        if not block_lines:
            return
        block_text = "\n".join(block_lines).strip()
        score = score_text(block_text, tokens, query)
        if score > 0:
            hits.append(
                {
                    "path": str(path),
                    "line": block_start,
                    "score": score,
                    "snippet": shorten(block_text.replace("\n", " | "), 320),
                }
            )

    for index, line in enumerate(lines, start=1):
        if line.strip():
            if not block_lines:
                block_start = index
            block_lines.append(line.strip())
            continue
        flush_block()
        block_lines = []

    flush_block()
    hits.sort(key=lambda item: (-item["score"], item["path"], item["line"]))
    return hits[:limit]


def search_mem0_dump(path: Path, query: str, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    dump = json.loads(path.read_text(encoding="utf-8"))
    memories = dump.get("memories", [])
    tokens = tokenize(query)
    scored: dict[str, dict[str, Any]] = {}
    for item in memories:
        text = item.get("memory", "")
        metadata = item.get("metadata", {}) or {}
        haystack = " ".join([text, json.dumps(metadata, ensure_ascii=False)])
        score = score_text(haystack, tokens, query)
        if score <= 0:
            continue
        key = _mem0_result_key(item)
        existing = scored.get(key)
        source_collection = item.get("source_collection")
        source_collection_name = item.get("source_collection_name")
        if existing is None:
            deduped = {
                "id": item.get("id"),
                "score": score,
                "memory": text,
                "metadata": metadata,
                "source_collections": [source_collection] if source_collection else [],
                "source_collection_names": [source_collection_name] if source_collection_name else [],
                "duplicate_ids": [item.get("id")] if item.get("id") else [],
                "duplicate_count": 1,
            }
            if source_collection:
                deduped["source_collection"] = source_collection
            if source_collection_name:
                deduped["source_collection_name"] = source_collection_name
            scored[key] = deduped
            continue

        existing["score"] = max(float(existing["score"]), score)
        existing["duplicate_count"] = int(existing.get("duplicate_count", 1)) + 1
        if item.get("id") and item.get("id") not in existing["duplicate_ids"]:
            existing["duplicate_ids"].append(item.get("id"))
        if source_collection and source_collection not in existing["source_collections"]:
            existing["source_collections"].append(source_collection)
        if source_collection_name and source_collection_name not in existing["source_collection_names"]:
            existing["source_collection_names"].append(source_collection_name)

    ranked = list(scored.values())
    ranked.sort(
        key=lambda item: (
            -item["score"],
            -int(item.get("duplicate_count", 1)),
            item.get("id") or "",
        )
    )
    return ranked[:limit]


def read_source_content(source: dict[str, Any]) -> dict[str, Any]:
    path = expand_path(source["path"])
    if not path.exists():
        return {
            "id": source["id"],
            "label": source["label"],
            "path": str(path),
            "exists": False,
            "content": "",
        }

    if source.get("kind") == "markdown":
        content = extract_markdown_content(path, source)
    else:
        content = path.read_text(encoding="utf-8", errors="replace").strip()

    item = {
        "id": source["id"],
        "label": source["label"],
        "path": str(path),
        "exists": True,
        "content": content,
    }
    item.update(file_meta(path))
    return item


def tz_name(manifest: dict[str, Any]) -> str:
    return manifest.get("settings", {}).get("timezone", "Europe/Warsaw")


def now_local(manifest: dict[str, Any]) -> datetime:
    return datetime.now(ZoneInfo(tz_name(manifest)))


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def parse_when(raw_value: str, manifest: dict[str, Any]) -> datetime:
    value = raw_value.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        dt = datetime.fromisoformat(f"{value}T00:00:00")
        return dt.replace(tzinfo=ZoneInfo(tz_name(manifest)))

    candidate = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz_name(manifest)))
    return dt


def git_repo_summary(repo_path: Path) -> dict[str, Any]:
    import subprocess

    if not repo_path.exists():
        return {"path": str(repo_path), "exists": False}

    branch_proc = subprocess.run(
        ["git", "-C", str(repo_path), "branch", "--show-current"],
        capture_output=True,
        text=True,
        check=False,
    )
    status_proc = subprocess.run(
        ["git", "-C", str(repo_path), "status", "--short"],
        capture_output=True,
        text=True,
        check=False,
    )

    lines = [line for line in status_proc.stdout.splitlines() if line.strip()]
    counts = Counter()
    for line in lines:
        if line.startswith("??"):
            counts["untracked"] += 1
            continue
        if len(line) >= 2:
            if line[0] == "M" or line[1] == "M":
                counts["modified"] += 1
            if line[0] == "A" or line[1] == "A":
                counts["added"] += 1
            if line[0] == "D" or line[1] == "D":
                counts["deleted"] += 1

    return {
        "path": str(repo_path),
        "exists": True,
        "branch": branch_proc.stdout.strip() or None,
        "dirty": bool(lines),
        "changes": dict(counts),
        "change_count": len(lines),
    }


def summarize_asset_manifest(path: Path) -> dict[str, Any]:
    text = read_text(path)
    if not text:
        return {"path": str(path), "exists": False}

    current_project: str | None = None
    missing_assets: list[dict[str, Any]] = []
    project_counter = 0
    for line in text.splitlines():
        project_match = re.match(r'\s*"([^"]+)":\s*{', line)
        if project_match:
            current_project = project_match.group(1)
            project_counter += 1
            continue

        if current_project is None:
            continue

        asset_match = re.match(r"\s*([A-Za-z0-9_]+):\s*(null|\[\]\s+as\s+string\[\])", line)
        if asset_match:
            comment_match = re.search(r"//\s*STATUS:\s*(.*)$", line)
            missing_assets.append(
                {
                    "project": current_project,
                    "asset": asset_match.group(1),
                    "status": comment_match.group(1).strip() if comment_match else None,
                }
            )

    og_needed = len(re.findall(r"/public/og/", text))
    return {
        "path": str(path),
        "exists": True,
        "projects_total": project_counter,
        "missing_assets_total": len(missing_assets),
        "og_images_needed": og_needed,
        "missing_assets_sample": missing_assets[:12],
    }




def source_freshness(manifest: dict[str, Any], source_ids: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_id in source_ids:
        source = manifest["source_map"][source_id]
        path = expand_path(source["path"])
        row = {
            "id": source_id,
            "label": source["label"],
            "path": str(path),
            "exists": path.exists(),
        }
        if path.exists():
            row.update(file_meta(path))
        rows.append(row)
    return rows


def mem0_meta(manifest: dict[str, Any]) -> dict[str, Any]:
    path = expand_path(manifest["paths"]["mem0_dump"])
    if not path.exists():
        return {"path": str(path), "exists": False}
    payload = read_json(path) or {}
    meta = file_meta(path)
    meta["total_memories"] = payload.get("total_memories")
    return meta


def build_source_excerpts(manifest: dict[str, Any], domain_id: str) -> list[dict[str, Any]]:
    excerpts: list[dict[str, Any]] = []
    for source_id in manifest["domain_map"][domain_id]["source_ids"]:
        source = read_source_content(manifest["source_map"][source_id])
        excerpts.append(
            {
                "id": source["id"],
                "label": source["label"],
                "path": source["path"],
                "excerpt": shorten(source.get("content", ""), 1200),
            }
        )
    return excerpts


def build_runtime_snapshot(
    manifest: dict[str, Any],
    *,
    domain_id: str,
    agent: str,
    title: str | None,
    focus: str | None,
    recent_events: list[dict[str, Any]],
) -> dict[str, Any]:
    local_dt = now_local(manifest)
    domain = manifest["domain_map"][domain_id]
    paths = manifest["paths"]
    mem0_hits: dict[str, list[dict[str, Any]]] = {}
    mem0_dump_path = expand_path(paths["mem0_dump"])
    for query in domain.get("mem0_queries", []):
        mem0_hits[query] = search_mem0_dump(mem0_dump_path, query, 2)

    # Repo paths are optional: retiring a project means deleting its entry from
    # the manifest, and a snapshot must not hard-fail on a key that is simply
    # gone (dropping render_repo/media_sorter_repo broke daily-capture with a
    # bare KeyError until this became a lookup over whatever is declared).
    repos = [
        git_repo_summary(expand_path(paths[key]))
        for key in ("workspace_root", "portfolio_repo", "render_repo", "media_sorter_repo")
        if paths.get(key)
    ]

    return {
        "id": str(uuid.uuid4()),
        "captured_at_utc": utc_now(),
        "captured_at_local": local_dt.isoformat(timespec="seconds"),
        "timezone": tz_name(manifest),
        # Per-installation identity for agent-facing prompts; generic by default.
        "operator": manifest.get("settings", {}).get("operator") or "the operator",
        "agent": agent,
        "domain": domain_id,
        "label": domain["label"],
        "title": title or f"{domain['label']} snapshot",
        "focus": focus,
        "source_freshness": source_freshness(manifest, domain["source_ids"]),
        "source_excerpts": build_source_excerpts(manifest, domain_id),
        "recent_ledger": recent_events[:8],
        "mem0_dump": mem0_meta(manifest),
        "mem0_snapshot_hits": mem0_hits,
        "portfolio_assets": summarize_asset_manifest(expand_path(paths["portfolio_asset_manifest"])) if paths.get("portfolio_asset_manifest") else {},
        "repos": repos,
    }


def render_activation_prompt(snapshot: dict[str, Any]) -> str:
    portfolio = snapshot["portfolio_assets"]
    recent_ledger = snapshot["recent_ledger"][:5]

    ledger_lines: list[str] = []
    for entry in recent_ledger:
        timestamp = entry.get("recorded_at", "unknown-time")
        project = entry.get("project") or "n/a"
        category = entry.get("category") or "n/a"
        text = entry.get("text") or ""
        ledger_lines.append(f"- {timestamp} | {category} | {project} | {text}")

    missing_assets: list[str] = []
    for item in portfolio.get("missing_assets_sample", [])[:8]:
        asset_status = f" ({item['status']})" if item.get("status") else ""
        missing_assets.append(f"- {item['project']}.{item['asset']}{asset_status}")
    if not missing_assets:
        missing_assets.append("- No missing asset sample captured.")

    lines = [
        f"You are connecting to Max Chronicle, the durable operating memory for {snapshot.get('operator') or 'the operator'}.",
        "",
        f"Activation contract: {ACTIVATION_CONTRACT_NAME} {ACTIVATION_CONTRACT_VERSION}",
        f"Current local time: {snapshot['captured_at_local']} ({snapshot['timezone']})",
        f"Snapshot ID: {snapshot['id']}",
        f"Domain: {snapshot['domain']} - {snapshot['label']}",
        "",
        "Operating contract:",
        "- Treat `chronicle.db` as canonical truth.",
        "- Treat `status/` as the authoritative readable structure and operator-facing view.",
        "- Treat `Mem0` and `mem0-dump.json` as derived semantic recall layers, not as truth.",
        "- Treat `ssot-ledger.jsonl` and `chronicle-snapshots.jsonl` as compatibility logs; write through tools, not by hand.",
        "- Communicate in Russian. Write code and commits in English.",
        "- Prefer incremental shipping over rebuilding systems.",
        "- Do not rewrite history; record a new event or snapshot when reality changes.",
        "",
        "Your role in this session:",
        "- You are both the execution agent and the chronicler of the system.",
        "- At session start, load the activation context before making assumptions.",
        "- During work, record durable decisions, blockers, state changes, and rationale with `why`.",
        "- Before context switches, handoff, or thread end, capture a timestamped snapshot.",
        "- If live Mem0 is blocked by sandbox or infra, keep Chronicle current and replay later.",
        "",
        "Current task:",
        f"- Focus: {snapshot.get('focus') or 'Read current project context before choosing work.'}",
        "- Read current priorities from the configured sources; historical events may describe superseded decisions.",
        "",
        "Portfolio asset state:",
        f"- Projects tracked: {portfolio.get('projects_total', 'unknown')}",
        f"- Missing asset slots: {portfolio.get('missing_assets_total', 'unknown')}",
        f"- OG images still needed: {portfolio.get('og_images_needed', 'unknown')}",
        *missing_assets,
        "",
        "Recent durable events:",
        *(ledger_lines or ["- No durable events captured yet."]),
        "",
        "Canonical source files for this activation:",
        *[f"- {item['path']}" for item in snapshot["source_excerpts"]],
    ]
    return "\n".join(lines).strip()
