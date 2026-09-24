"""The identity registry (FR-14): one canonical name per agent, project and domain."""

from __future__ import annotations

import sqlite3

import pytest

from max_chronicle.identity import UNKNOWN_AGENT, Registry, fold, fold_sql


@pytest.mark.parametrize(
    "name",
    ["max-chronicle", "Max_Chronicle", " status ", "garden planner app", "Launch_plan-video",
     "ПАМЯТЬ проекта", "Экономика_digest", "a__b  c", "MiXeD-Case_Name"],
)
def test_fold_is_the_same_in_python_and_sqlite(name: str) -> None:
    with sqlite3.connect(":memory:") as connection:
        in_sqlite = connection.execute(f"SELECT {fold_sql('?')}", (name,)).fetchone()[0]
    assert fold(name) == in_sqlite


def test_projects_resolve_every_declared_spelling_and_keep_undeclared_names() -> None:
    registry = Registry.from_manifest({"projects": [
        {"id": "max-chronicle", "roots": ["~/x"], "aliases": ["status", "Chronicle"]},
    ]})

    assert registry.project("STATUS") == "max-chronicle"
    assert registry.project("chronicle") == "max-chronicle"
    assert registry.project("Max_Chronicle") == "max-chronicle"
    assert registry.project(" Other_Thing ") == "Other_Thing"
    assert registry.project(None) is None
    assert registry.projects.forms("status") == ("max-chronicle", "status", "chronicle")
    assert registry.projects.forms("Other_Thing") == ("other-thing",)


def test_a_spelling_claimed_by_two_projects_is_a_configuration_error() -> None:
    with pytest.raises(ValueError, match="belongs to both"):
        Registry.from_manifest({"projects": [
            {"id": "alpha", "aliases": ["shared"]},
            {"id": "beta", "aliases": ["Shared"]},
        ]})
    with pytest.raises(ValueError, match="list of names"):
        Registry.from_manifest({"projects": [{"id": "alpha", "aliases": "one"}]})


@pytest.mark.parametrize(
    ("given", "agent"),
    [
        ("claude-mac", "claude"), ("claude-opus-harp-nuss-p0-build", "claude"), ("operator", "claude"),
        ("Codex", "codex"), ("codex-mcp-client", "codex"), ("claude-code", "claude"),
        ("opencode-glm5.2", "glm"), ("glm-5.2-opencode", "glm"), ("zcode", "glm"),
        ("opencode-deepseek", "deepseek"), ("opencode", "opencode"),
        ("transcript-analyst-handoff", "transcript-analyst"),
        ("RZMRN_digest pipeline", "rzmrn-digest-pipeline"), (None, UNKNOWN_AGENT), ("  ", UNKNOWN_AGENT),
    ],
)
def test_default_agents_collapse_historical_spellings(given, agent) -> None:
    assert Registry.from_manifest(None).agent(given) == agent


def test_the_manifest_adds_agents_and_replaces_a_default() -> None:
    registry = Registry.from_manifest({"agents": [
        {"id": "claude", "aliases": ["napaarnik"]},  # replaces the default: no prefix rule left
        {"id": "antigravity", "prefixes": ["antigravity"]},
    ]})

    assert registry.agent("Napaarnik") == "claude"
    assert registry.agent("Antigravity-IDE") == "antigravity"
    assert registry.agent("claude-mac") == "claude-mac"
    assert registry.agent("Codex") == "codex"


def test_project_sql_names_one_project_for_every_spelling() -> None:
    registry = Registry.from_manifest({"projects": [{"id": "max-chronicle", "aliases": ["status", "chronicle"]}]})
    sql, params = registry.project_sql("t.name")

    with sqlite3.connect(":memory:") as connection:
        def canonical(value):
            return connection.execute(f"WITH t(name) AS (VALUES (?)) SELECT {sql} FROM t", (value, *params)).fetchone()[0]

        assert {canonical(value) for value in ("status", "Chronicle", "max_chronicle", "MAX-CHRONICLE")} == {"max-chronicle"}
        assert canonical("Other_Thing") == "other-thing"
        assert canonical(None) is None


def test_a_scope_key_is_the_same_for_every_spelling() -> None:
    registry = Registry.from_manifest({"projects": [{"id": "max-chronicle", "aliases": ["status"]}]})

    assert registry.scope(domain="global", project="status", task_id="Launch_plan").key == \
        registry.scope(domain="global", project="max-chronicle", task_id="launch plan").key == \
        ["global", "max-chronicle", "launch-plan"]


def test_a_write_gets_canonical_names_and_keeps_the_given_ones() -> None:
    registry = Registry.from_manifest({
        "projects": [{"id": "max-chronicle", "aliases": ["status"]}],
        "domains": [{"id": "memory", "aliases": ["mem"]}],
    })
    entry = {"agent": "Codex", "project": "status", "domain": "mem", "task_id": "Launch_plan"}
    registry.canonical_entry(entry)

    assert entry == {
        "agent": "codex", "actor_raw": "Codex",
        "project": "max-chronicle", "project_raw": "status",
        "domain": "memory", "domain_raw": "mem",
        "task_id": "Launch_plan",
    }
    canonical = {"agent": "codex", "project": "max-chronicle", "domain": "memory"}
    registry.canonical_entry(canonical)
    assert canonical == {"agent": "codex", "project": "max-chronicle", "domain": "memory"}
