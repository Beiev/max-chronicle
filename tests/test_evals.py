"""Retrieval eval: golden-set parsing, metric arithmetic, and the synthetic baseline."""

from __future__ import annotations

from dataclasses import asdict, replace
import json
from pathlib import Path
import re
from typing import Any

import pytest

from max_chronicle import embeddings, service
from max_chronicle.cli import build_parser
from max_chronicle.evals import (
    GoldenCase,
    build_report,
    check_thresholds,
    format_report,
    load_golden,
    run_eval,
    summarize,
)

FIXTURES = Path(__file__).parent / "fixtures" / "eval"
# Synthetic cases the lexical channel alone gets right today. Search changes
# must keep every one of them; when a change makes more pass, it adds their
# ids here, so each search PR shows its effect case by case.
LEXICAL_BASELINE = frozenset(
    {
        "fact-en-keywords",
        "fact-ru-keywords",
        "rationale-ru-keywords",
        "update-en-deploy-fact",
        "handoff-en-keywords",
        "abstain-en-unrelated",
        "abstain-ru-unrelated",
        "abstain-ru-near-miss",
        "abstain-scope",
        # Function words dropped, ё folded, Unicode tokens, relaxed stems:
        "fact-ru-question",
        "fact-ru-yo",
        "fact-ru-yo-as-e",
        "fact-ru-yo-other-word",
        "fact-uk-keywords",
        "fact-en-backups-question",
        "rationale-ru-question",
        # Negations kept as terms ("чому не публікуємо демо"):
        "rationale-uk-question",
        # A relaxed match alone is not an answer: no_confident_match (FR-2).
        "abstain-en-near-miss",
        # Traded away: recency only breaks ties (FR-1), so on full text alone the
        # shorter July decision outranks its August replacement by BM25. The
        # same update recorded as a fact with `supersedes` stays current
        # (update-en-deploy-fact); on 64 real questions the change lifts hybrid
        # hit@1 by 11 points with no loss on knowledge updates.
    }
)


def _seed_corpus(manifest: dict) -> dict[str, str]:
    """Record the synthetic corpus; map each corpus key to its event id."""
    ids: dict[str, str] = {}
    fact_ids: dict[str, str] = {}
    for line in (FIXTURES / "corpus.jsonl").read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        key = item.pop("key")
        fact = item.pop("fact", None)
        entry = {"agent": "agent-a", "domain": "global", **item}
        if fact is not None:
            superseded = fact.pop("supersedes_key", None)
            entry["fact"] = fact | ({"supersedes": fact_ids[superseded]} if superseded else {})
        result = service.record_event(manifest, entry)
        ids[key] = result["id"]
        if fact is not None:
            fact_ids[key] = result["fact_id"]
    return ids


def _synthetic_cases(ids: dict[str, str]) -> list[GoldenCase]:
    def resolve(refs: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(f"event:{ids[ref.partition(':')[2]]}" for ref in refs)

    return [
        replace(case, expected=resolve(case.expected), stale=resolve(case.stale))
        for case in load_golden(FIXTURES / "golden.jsonl")
    ]


def _passing(report: dict[str, Any]) -> set[str]:
    return {
        detail["id"]
        for detail in report["cases"]
        if (
            detail["first_hit_rank"] is not None
            and detail["first_hit_rank"] <= 5
            and detail.get("update_correct", True)
            if detail["expected"]
            else detail["abstained"]
        )
    }


@pytest.fixture()
def lexical_only(monkeypatch):
    """No embedding backend: the baseline measures the lexical channel alone."""
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    monkeypatch.setattr(embeddings, "embed_text", lambda *args, **kwargs: None)


def _case(case_id: str, expected: tuple[str, ...] = (), stale: tuple[str, ...] = ()) -> GoldenCase:
    return GoldenCase(
        id=case_id,
        query=case_id,
        category="knowledge_update" if stale else ("fact" if expected else "abstention"),
        expected=expected,
        stale=stale,
    )


def _ticking(latencies_ms: list[float]):
    moments: list[float] = []
    for index, latency in enumerate(latencies_ms):
        moments += [float(index), index + latency / 1000]
    return iter(moments).__next__


def test_metrics_follow_the_ranks_recall_returns() -> None:
    rankings = {
        "first": ["event:a", "event:x"],
        "third": ["event:x", "event:y", "event:b", "event:c"],
        "miss": ["event:x"],
        "update": ["event:old", "event:new"],
        "quiet": [],
        "noisy": ["event:x"],
    }
    cases = [
        _case("first", ("event:a",)),
        _case("third", ("event:b", "event:c")),
        _case("miss", ("event:d",)),
        _case("update", ("event:new",), stale=("event:old",)),
        _case("quiet"),
        _case("noisy"),
    ]

    def recall(manifest, *, query, limit, **scope):
        return {"results": [{"ref": ref} for ref in rankings[query]]}

    metrics = summarize(run_eval({}, cases, recall=recall, clock=_ticking([10, 20, 30, 40, 50, 60])))

    assert metrics["hit@1"] == 0.25
    assert metrics["hit@5"] == metrics["hit@10"] == 0.75
    assert metrics["recall@5"] == 0.75
    assert metrics["mrr@10"] == round((1 + 1 / 3 + 0 + 1 / 2) / 4, 4)
    assert metrics["abstention_accuracy"] == 0.5
    assert metrics["update_accuracy"] == 0.0  # the stale value outranks the current one
    assert metrics["latency_ms"] == {"p50": 30.0, "p95": 60.0, "max": 60.0}


def test_an_explicit_no_match_signal_counts_as_abstaining() -> None:
    def recall(manifest, *, query, limit, **scope):
        return {"results": [{"event_id": "x"}], "no_confident_match": True}

    [detail] = run_eval({}, [_case("unknowable")], recall=recall)

    assert detail["abstained"] is True
    assert detail["ranked"] == ["event:x"]


def test_a_failing_query_is_a_scored_miss_and_the_run_goes_on() -> None:
    def recall(manifest, *, query, limit, **scope):
        if query == "broken":
            raise ValueError("task_id requires project")
        return {"results": [{"event_id": "a"}]}

    details = run_eval({}, [_case("broken", ("event:a",)), _case("fine", ("event:a",))], recall=recall)

    assert details[0]["error"] == "ValueError: task_id requires project"
    assert details[0]["first_hit_rank"] is None
    assert details[1]["first_hit_rank"] == 1
    assert summarize(details)["errors"] == 1


def test_a_failing_query_never_counts_as_abstaining() -> None:
    def recall(manifest, *, query, limit, **scope):
        raise RuntimeError("database is locked")

    details = run_eval({}, [_case("unknowable")], recall=recall)
    report = {"overall": summarize(details)}

    assert details[0]["abstained"] is False
    assert report["overall"]["abstention_accuracy"] == 0.0
    assert check_thresholds(report, {}) == ["errors: 1 queries failed"]


def test_thresholds_name_every_metric_below_its_floor() -> None:
    report = {"overall": {"hit@5": 0.5, "abstention_accuracy": 1.0}}

    failures = check_thresholds(report, {"hit@5": 0.6, "abstention_accuracy": 0.9, "update_accuracy": 0.5})

    assert failures == ["hit@5: 0.5000 < 0.6000", "update_accuracy: not measured by this golden set"]


VALID = {"id": "a", "query": "q", "category": "fact", "expected": ["event:1"]}


@pytest.mark.parametrize(
    ("line", "message"),
    [
        ("[]", "must be a JSON object"),
        ("{not json", "Expecting property name"),
        (json.dumps(VALID | {"extra": 1}), "unknown fields: extra"),
        (json.dumps(VALID | {"query": " "}), "query must be a non-empty string"),
        (json.dumps(VALID | {"category": "trivia"}), "category must be one of"),
        (json.dumps(VALID | {"category": "abstention"}), "abstention cases, and only they"),
        (json.dumps(VALID | {"expected": []}), "abstention cases, and only they"),
        (json.dumps(VALID | {"expected": ["doc:readme"]}), "expected must be a list of references (event:<id>, note:<id>)"),
        (json.dumps(VALID | {"expected": ["event:1"], "category": "knowledge_update", "stale": ["note:~/a.md"]}),
         "a case expects events or notes, not both"),
        (json.dumps(VALID | {"stale": ["event:1"]}), "both expected and stale"),
        (json.dumps(VALID | {"scope": {"agent": "codex"}}), "scope maps domain, project, task_id"),
        (json.dumps(VALID | {"lang": ""}), "lang must be a non-empty string"),
        (json.dumps(VALID | {"expected": ["event:1", "event:1"]}), "a reference is listed twice"),
        (json.dumps(VALID | {"scope": {"task_id": "ship"}}), "a task_id scope needs its project"),
    ],
)
def test_an_invalid_case_names_its_file_and_line(tmp_path, line: str, message: str) -> None:
    golden = tmp_path / "golden.jsonl"
    golden.write_text(json.dumps(VALID | {"id": "ok"}) + "\n\n" + line + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match=f"golden.jsonl:3: .*{re.escape(message)}"):
        load_golden(golden)


def test_duplicate_ids_and_an_empty_set_are_rejected(tmp_path) -> None:
    duplicated = tmp_path / "duplicated.jsonl"
    duplicated.write_text(json.dumps(VALID) + "\n" + json.dumps(VALID) + "\n", encoding="utf-8")
    empty = tmp_path / "empty.jsonl"
    empty.write_text("\n", encoding="utf-8")

    with pytest.raises(ValueError, match="duplicated.jsonl:2: duplicate case id 'a'"):
        load_golden(duplicated)
    with pytest.raises(ValueError, match="no cases"):
        load_golden(empty)


def test_the_lexical_baseline_holds_case_by_case(loaded_manifest, lexical_only) -> None:
    cases = _synthetic_cases(_seed_corpus(loaded_manifest))

    report = build_report(run_eval(loaded_manifest, cases))

    passing = _passing(report)
    assert not LEXICAL_BASELINE - passing, f"regressed: {sorted(LEXICAL_BASELINE - passing)}"
    assert not passing - LEXICAL_BASELINE, f"now passing, add to LEXICAL_BASELINE: {sorted(passing - LEXICAL_BASELINE)}"
    assert report["overall"]["errors"] == 0
    assert report["overall"]["channel_errors"] == {"vector": len(cases)}
    assert set(report["by_category"]) == {"fact", "rationale", "knowledge_update", "temporal", "handoff", "abstention"}
    assert "Misses:" in format_report(report)


def test_the_eval_command_writes_the_report_and_enforces_floors(
    tmp_path, loaded_manifest, chronicle_sandbox, lexical_only, capsys
) -> None:
    golden = tmp_path / "golden.jsonl"
    golden.write_text(
        "".join(
            json.dumps(asdict(case), ensure_ascii=False) + "\n"
            for case in _synthetic_cases(_seed_corpus(loaded_manifest))
        ),
        encoding="utf-8",
    )
    report_path = tmp_path / "results" / "baseline.json"

    def run(*extra: str, top: tuple[str, ...] = ()) -> int:
        args = build_parser().parse_args(
            ["--manifest", str(chronicle_sandbox.manifest_path), *top, "eval", "--golden", str(golden), *extra]
        )
        return args.handler(args)

    assert run("--out", str(report_path), "--fail-under", "hit@5=0.2") == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["schema"] == "chronicle-eval/1"
    assert report["golden"]["cases"] == 30 and len(report["golden"]["sha256"]) == 64
    assert report["thresholds"] == {"floors": {"hit@5": 0.2}, "failures": []}

    capsys.readouterr()
    assert run("--fail-under", "hit@5=0.9") == 1
    assert "BELOW FLOOR hit@5" in capsys.readouterr().out

    elsewhere = tmp_path / "elsewhere.db"
    assert run("--json", top=("--db", str(elsewhere))) == 0
    assert json.loads(capsys.readouterr().out)["overall"]["hit@5"] == 0.0  # an empty store, not the manifest's
    assert elsewhere.exists()

    with pytest.raises(SystemExit) as refused:
        run("--fail-under", "hit@5=nan")
    assert refused.value.code == 2

    golden.write_text("{}\n", encoding="utf-8")
    assert run() == 2




def test_a_note_case_is_scored_on_the_notes_and_asks_for_them(monkeypatch, tmp_path) -> None:
    from max_chronicle import recall as recall_module
    from max_chronicle.evals import _parse_case, build_report, format_report

    monkeypatch.setenv("HOME", str(tmp_path))
    note = tmp_path / "memory" / "backups.md"
    asked: list[bool] = []

    def query_memory(manifest, *, query, limit, include_notes, **scope):
        asked.append(include_notes)
        return {"results": [{"event_id": "e1"}], "notes": [{"path": "/elsewhere.md"}, {"path": str(note)}]}

    monkeypatch.setattr(recall_module, "query_memory", query_memory)
    note_case = _parse_case({"id": "note", "query": "backups", "category": "fact", "expected": ["note:~/memory/backups.md"]})

    details = run_eval({}, [_case("event", ("event:e1",)), note_case])
    report = build_report(details)

    assert asked == [False, True]  # event cases score as before notes were indexed
    assert note_case.expected == (f"note:{note}",)
    assert [(d["surface"], d["first_hit_rank"]) for d in details] == [("events", 1), ("notes", 2)]
    assert report["by_surface"]["notes"]["hit@1"] == 0.0 and report["by_surface"]["events"]["hit@1"] == 1.0
    assert "surface:notes" in format_report(report)
