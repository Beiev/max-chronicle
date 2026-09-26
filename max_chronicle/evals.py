"""Retrieval evaluation over a golden question set.

A golden case asks one question and names the evidence a good answer rests on.
An empty ``expected`` list marks a question memory cannot answer, where the
right response is to find nothing. Answerable cases are scored by hit@k,
recall@5 and MRR@10, unanswerable ones by abstention, and every query is
timed. Categories follow LongMemEval.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
import functools
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any

from . import __version__

EVAL_SCHEMA = "chronicle-eval/1"
CATEGORIES = ("fact", "rationale", "knowledge_update", "temporal", "handoff", "abstention")
SCOPE_KEYS = ("domain", "project", "task_id")
# Evidence kinds recall can return today. Documents join with the file index.
REF_KINDS = ("event",)
CASE_FIELDS = {"id", "query", "category", "lang", "expected", "stale", "scope", "notes"}
HIT_KS = (1, 5, 10)
RECALL_AT = 5
MRR_DEPTH = 10
RECALL_LIMIT = 10  # results requested per query: the deepest cutoff scored

RecallFn = Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class GoldenCase:
    """One question and the evidence a correct answer rests on."""

    id: str
    query: str
    category: str
    expected: tuple[str, ...]
    stale: tuple[str, ...] = ()
    lang: str = "und"
    scope: Mapping[str, str] = field(default_factory=dict)


def load_golden(path: Path) -> list[GoldenCase]:
    """Parse a JSONL golden set, one case per line.

    Raises:
        ValueError: on the first invalid line, naming the file and line.
    """
    cases: list[GoldenCase] = []
    seen: set[str] = set()
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            case = _parse_case(json.loads(line))
        except (json.JSONDecodeError, ValueError) as exc:
            raise ValueError(f"{path}:{line_no}: {exc}") from exc
        if case.id in seen:
            raise ValueError(f"{path}:{line_no}: duplicate case id {case.id!r}")
        seen.add(case.id)
        cases.append(case)
    if not cases:
        raise ValueError(f"{path}: no cases")
    return cases


def _parse_case(raw: Any) -> GoldenCase:
    if not isinstance(raw, dict):
        raise ValueError("a case must be a JSON object")
    unknown = set(raw) - CASE_FIELDS
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(sorted(unknown))}")
    case_id, query, category = raw.get("id"), raw.get("query"), raw.get("category")
    if not isinstance(case_id, str) or not case_id.strip():
        raise ValueError("id must be a non-empty string")
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    if category not in CATEGORIES:
        raise ValueError(f"category must be one of: {', '.join(CATEGORIES)}")
    expected = _refs(raw.get("expected"), "expected")
    stale = _refs(raw.get("stale", []), "stale")
    if len(set(expected)) != len(expected) or len(set(stale)) != len(stale):
        raise ValueError("a reference is listed twice")
    if (category == "abstention") != (not expected):
        raise ValueError("abstention cases, and only they, have no expected evidence")
    if set(stale) & set(expected):
        raise ValueError("a reference cannot be both expected and stale")
    scope = raw.get("scope", {})
    if (
        not isinstance(scope, dict)
        or set(scope) - set(SCOPE_KEYS)
        or not all(isinstance(value, str) and value.strip() for value in scope.values())
    ):
        raise ValueError(f"scope maps {', '.join(SCOPE_KEYS)} to non-empty strings")
    if "task_id" in scope and "project" not in scope:
        raise ValueError("a task_id scope needs its project")
    lang = raw.get("lang", "und")
    if not isinstance(lang, str) or not lang.strip():
        raise ValueError("lang must be a non-empty string")
    return GoldenCase(
        id=case_id, query=query, category=category, expected=expected, stale=stale, lang=lang, scope=scope
    )


def _refs(value: Any, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(
        isinstance(ref, str) and ref.partition(":")[0] in REF_KINDS and ref.partition(":")[2].strip()
        for ref in value
    ):
        kinds = ", ".join(f"{kind}:<id>" for kind in REF_KINDS)
        raise ValueError(f"{name} must be a list of references ({kinds})")
    return tuple(value)


def run_eval(
    manifest: Mapping[str, Any],
    cases: Iterable[GoldenCase],
    *,
    recall: RecallFn | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> list[dict[str, Any]]:
    """Ask every case through recall and score the ranking it returns."""
    if recall is None:
        from .recall import query_memory

        # The golden set scores event recall; indexed notes are ranked apart.
        recall = functools.partial(query_memory, include_notes=False)
    details = []
    for case in cases:
        started = clock()
        try:
            response = recall(manifest, query=case.query, limit=RECALL_LIMIT, **case.scope)
            error = None
        except Exception as exc:  # a failing query is a measured miss, not an aborted run
            response, error = {}, f"{type(exc).__name__}: {exc}"
        latency_ms = (clock() - started) * 1000
        details.append(_score_case(case, response, latency_ms=latency_ms, error=error))
    return details


def _hit_ref(hit: Mapping[str, Any]) -> str:
    return hit.get("ref") or f"event:{hit['event_id']}"


def _score_case(
    case: GoldenCase, response: Mapping[str, Any], *, latency_ms: float, error: str | None
) -> dict[str, Any]:
    ranked = [_hit_ref(hit) for hit in response.get("results") or []]
    expected = set(case.expected)
    ranks = [rank for rank, ref in enumerate(ranked, start=1) if ref in expected]
    detail: dict[str, Any] = {
        "id": case.id,
        "category": case.category,
        "lang": case.lang,
        "query": case.query,
        "expected": list(case.expected),
        "ranked": ranked,
        "first_hit_rank": ranks[0] if ranks else None,
        "found_at_5": len(expected & set(ranked[:RECALL_AT])),
        # An explicit "nothing confident" signal counts even with candidates;
        # a failed query abstained from nothing.
        "abstained": error is None and (bool(response.get("no_confident_match")) or not ranked),
        "degraded": bool(response.get("degraded")),
        "channel_errors": sorted((response.get("channel_errors") or {}).keys()),
        "latency_ms": round(latency_ms, 2),
    }
    if case.stale:
        stale_ranks = [rank for rank, ref in enumerate(ranked, start=1) if ref in set(case.stale)]
        first = detail["first_hit_rank"]
        detail["update_correct"] = first is not None and all(first < rank for rank in stale_ranks)
    if error:
        detail["error"] = error
    return detail


def summarize(details: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate scored cases into one metrics block."""
    answerable = [d for d in details if d["expected"]]
    unanswerable = [d for d in details if not d["expected"]]
    metrics: dict[str, Any] = {
        "cases": len(details),
        "answerable": len(answerable),
        "abstention_cases": len(unanswerable),
    }
    if answerable:
        for k in HIT_KS:
            metrics[f"hit@{k}"] = _mean(_ranked_within(d, k) for d in answerable)
        metrics[f"recall@{RECALL_AT}"] = _mean(d["found_at_5"] / len(d["expected"]) for d in answerable)
        metrics[f"mrr@{MRR_DEPTH}"] = _mean(
            1 / d["first_hit_rank"] if _ranked_within(d, MRR_DEPTH) else 0.0 for d in answerable
        )
        metrics["empty_on_answerable"] = _mean(not d["ranked"] for d in answerable)
        # The cost of abstaining: answerable questions called unconfident or empty.
        metrics["abstained_on_answerable"] = _mean(d["abstained"] for d in answerable)
    if unanswerable:
        metrics["abstention_accuracy"] = _mean(d["abstained"] for d in unanswerable)
    updates = [d for d in details if "update_correct" in d]
    if updates:
        metrics["update_accuracy"] = _mean(d["update_correct"] for d in updates)
    metrics["degraded_rate"] = _mean(d["degraded"] for d in details)
    metrics["channel_errors"] = dict(Counter(channel for d in details for channel in d["channel_errors"]))
    metrics["errors"] = sum(1 for d in details if d.get("error"))
    latencies = sorted(d["latency_ms"] for d in details)
    metrics["latency_ms"] = {
        "p50": _percentile(latencies, 50),
        "p95": _percentile(latencies, 95),
        "max": latencies[-1] if latencies else None,
    }
    return metrics


def _ranked_within(detail: Mapping[str, Any], depth: int) -> bool:
    rank = detail["first_hit_rank"]
    return rank is not None and rank <= depth


def _mean(values: Iterable[float | bool]) -> float | None:
    items = [float(value) for value in values]
    return round(sum(items) / len(items), 4) if items else None


def _percentile(ordered: list[float], percent: int) -> float | None:
    """Nearest-rank percentile of an ascending list."""
    if not ordered:
        return None
    return ordered[max(0, math.ceil(percent / 100 * len(ordered)) - 1)]


def build_report(details: list[dict[str, Any]], *, golden_path: Path | None = None) -> dict[str, Any]:
    """Wrap scored cases in a self-describing report with group breakdowns."""
    from .embeddings import active_profile

    golden: dict[str, Any] = {"cases": len(details)}
    if golden_path is not None:
        golden |= {
            "path": str(golden_path),
            "sha256": hashlib.sha256(golden_path.read_bytes()).hexdigest(),
        }
    return {
        "schema": EVAL_SCHEMA,
        "version": __version__,
        "run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "golden": golden,
        "settings": {
            "limit": RECALL_LIMIT,
            "embed_model": active_profile().key,
            "vector_min_similarity": float(
                os.environ.get("CHRONICLE_VECTOR_MIN_SIMILARITY") or active_profile().min_similarity
            ),
            "vector_confident_similarity": (
                float(os.environ["CHRONICLE_VECTOR_CONFIDENT_SIMILARITY"])
                if os.environ.get("CHRONICLE_VECTOR_CONFIDENT_SIMILARITY", "").strip()
                else active_profile().confident_similarity
            ),
        },
        "overall": summarize(details),
        "by_category": _grouped(details, "category"),
        "by_lang": _grouped(details, "lang"),
        "cases": details,
    }


def _grouped(details: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for detail in details:
        groups.setdefault(detail[key], []).append(detail)
    return {name: summarize(group) for name, group in sorted(groups.items())}


def check_thresholds(report: Mapping[str, Any], thresholds: Mapping[str, float]) -> list[str]:
    """Return one message per overall metric below its floor."""
    failures = []
    if report["overall"].get("errors"):
        failures.append(f"errors: {report['overall']['errors']} queries failed")
    for name, floor in thresholds.items():
        value = report["overall"].get(name)
        if not isinstance(value, (int, float)):
            failures.append(f"{name}: not measured by this golden set")
        elif value < floor:
            failures.append(f"{name}: {value:.4f} < {floor:.4f}")
    return failures


def format_report(report: Mapping[str, Any]) -> str:
    """Render the report as a compact plain-text table plus the misses."""
    columns = ("hit@1", "hit@5", "mrr@10", "abstention_accuracy", "update_accuracy")
    headers = ("group", "n", "hit@1", "hit@5", "mrr@10", "abstain", "update", "p95 ms")
    rows = [("overall", report["overall"])]
    rows += [(f"cat:{name}", metrics) for name, metrics in report["by_category"].items()]
    rows += [(f"lang:{name}", metrics) for name, metrics in report["by_lang"].items()]
    table = [headers]
    for name, metrics in rows:
        cells = [_cell(metrics.get(column)) for column in columns]
        table.append((name, str(metrics["cases"]), *cells, _cell(metrics["latency_ms"]["p95"])))
    widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
    lines = [
        f"Chronicle eval {report['version']}: {report['golden']['cases']} cases, "
        f"embed model {report['settings']['embed_model']}, "
        f"degraded {_cell(report['overall']['degraded_rate'])}, "
        f"abstained on answerable {_cell(report['overall'].get('abstained_on_answerable'))}",
        "",
    ]
    lines += ["  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip() for row in table]
    misses = [
        detail
        for detail in report["cases"]
        if (detail["expected"] and not _ranked_within(detail, RECALL_AT))
        or (not detail["expected"] and not detail["abstained"])
    ]
    if misses:
        lines += ["", "Misses:"]
        for detail in misses:
            outcome = "answered" if not detail["expected"] else f"rank {detail['first_hit_rank'] or '-'}"
            lines.append(f"  {detail['id']} [{detail['category']}/{detail['lang']}] {outcome}: {detail['query']}")
    return "\n".join(lines)


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}" if isinstance(value, float) else str(value)
