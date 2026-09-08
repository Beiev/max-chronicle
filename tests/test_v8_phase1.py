"""Chronicle v8 Phase 1 — entity aliases, merge, Mem0 live, resolution, dedup.

All tests run against the isolated `chronicle_sandbox` fixture from conftest.py,
so no live DB is touched. The sandbox applies migrations 0001..0008 on
first connection.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

from max_chronicle import service, store
from max_chronicle.runtime_context import load_manifest


# --------------------------------------------------------------------------
# Pure-function helpers — no DB needed.
# --------------------------------------------------------------------------


class TestNormalizeAliasText:
    def test_casefold_and_whitespace_collapse(self) -> None:
        assert service.normalize_alias_text("Иван") == "иван"
        assert service.normalize_alias_text("ИВАН") == "иван"
        assert service.normalize_alias_text("  иван  ") == "иван"
        assert service.normalize_alias_text("Ivan\tIvanov") == "ivan ivanov"
        assert service.normalize_alias_text("Double  Space") == "double space"

    def test_nfkc_folds_fullwidth_digits(self) -> None:
        # Fullwidth '1' (U+FF11) collapses to ASCII '1' under NFKC.
        assert service.normalize_alias_text("Х\uff11") == "х1"

    def test_empty_and_none(self) -> None:
        assert service.normalize_alias_text("") == ""
        assert service.normalize_alias_text(None) == ""
        assert service.normalize_alias_text("   ") == ""

    def test_cyrillic_and_latin_stay_separate(self) -> None:
        # NFKC does not transliterate across scripts. That's Phase 2 work.
        assert service.normalize_alias_text("Иван") != service.normalize_alias_text("Ivan")


class TestComputeEventContentHash:
    def test_deterministic(self) -> None:
        h1 = service.compute_event_content_hash(
            text="hello", category="note", actor="a", entity_id="e1"
        )
        h2 = service.compute_event_content_hash(
            text="hello", category="note", actor="a", entity_id="e1"
        )
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_distinguishes_payload_changes(self) -> None:
        h_base = service.compute_event_content_hash(text="x", category="note")
        assert h_base != service.compute_event_content_hash(text="x", category="decision")
        assert h_base != service.compute_event_content_hash(text="y", category="note")
        assert h_base != service.compute_event_content_hash(
            text="x", category="note", actor="a"
        )

    def test_optional_fields_default_to_empty(self) -> None:
        # Passing None and empty string should hash identically.
        h_a = service.compute_event_content_hash(text="x")
        h_b = service.compute_event_content_hash(text="x", category=None, actor=None)
        assert h_a == h_b

    def test_domain_project_why_included(self) -> None:
        """P0.1 regression: hash must split by domain/project/why.

        Two digest_run events across different projects used to collide
        because the old delimiter-joined string ignored domain/project.
        """
        base = service.compute_event_content_hash(text="digest run", category="digest_run")
        with_domain = service.compute_event_content_hash(
            text="digest run", category="digest_run", domain="work"
        )
        with_project = service.compute_event_content_hash(
            text="digest run", category="digest_run", project="news-digest"
        )
        with_why = service.compute_event_content_hash(
            text="digest run", category="digest_run", why="nightly"
        )
        assert len({base, with_domain, with_project, with_why}) == 4

    def test_immune_to_delimiter_collision(self) -> None:
        """P0.1 regression: ('a|b','c') and ('a','b|c') must differ.

        The old implementation joined fields with '|' and produced the
        same hash for these inputs.
        """
        a = service.compute_event_content_hash(text="a|b", category="c")
        b = service.compute_event_content_hash(text="a", category="b|c")
        assert a != b

    def test_unicode_payload_is_stable(self) -> None:
        # sort_keys + ensure_ascii=False must still produce identical bytes
        # for the same logical payload regardless of dict literal order.
        h1 = service.compute_event_content_hash(
            text="Иван", category="note", domain="work", project="acme"
        )
        h2 = service.compute_event_content_hash(
            project="acme", domain="work", category="note", text="Иван"
        )
        assert h1 == h2


# --------------------------------------------------------------------------
# DB-backed tests — use the chronicle_sandbox / loaded_manifest fixtures.
# --------------------------------------------------------------------------


@pytest.fixture()
def _pair_of_normalized_entities(loaded_manifest: dict) -> tuple[str, str]:
    """Seed two normalized entities; return (target_id, other_id)."""
    store.upsert_normalized_entity(
        service._config(loaded_manifest),
        entity_type="company",
        canonical_key="alpha",
        canonical_name="Alpha",
        aliases=["Alpha"],
        source_refs=[],
    )
    store.upsert_normalized_entity(
        service._config(loaded_manifest),
        entity_type="company",
        canonical_key="bravo",
        canonical_name="Bravo",
        aliases=["Bravo"],
        source_refs=[],
    )
    return "company:alpha", "company:bravo"


class TestAddEntityAlias:
    def test_inserts_new_alias(self, loaded_manifest, _pair_of_normalized_entities) -> None:
        target_id, _ = _pair_of_normalized_entities
        out = service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Иван",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        assert out["action"] == "inserted"
        assert out["alias"]["alias_key"] == "иван"
        assert out["alias"]["canonical_entity_id"] == target_id
        assert out["conflict"] is None

    def test_dry_run_does_not_write(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        out = service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Иван",
            canonical_entity_id=target_id,
            entity_type="company",
            dry_run=True,
        )
        assert out["action"] == "dry_run"
        assert service.entity_resolution_report_service(loaded_manifest)[
            "aliases_total"
        ] == 0

    def test_same_canonical_duplicate_reports_exists(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Иван",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        out = service.add_entity_alias_service(
            loaded_manifest,
            alias_text="ИВАН",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        assert out["action"] == "exists"
        assert out["conflict"] is None  # same canonical → not a conflict

    def test_cross_canonical_duplicate_reports_conflict(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, other_id = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Иван",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        out = service.add_entity_alias_service(
            loaded_manifest,
            alias_text="иван",
            canonical_entity_id=other_id,
            entity_type="company",
        )
        assert out["action"] == "exists"
        assert out["conflict"] is not None
        assert out["conflict"]["canonical_entity_id"] == target_id

    def test_unknown_canonical_raises(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        with pytest.raises(ValueError, match="not found"):
            service.add_entity_alias_service(
                loaded_manifest,
                alias_text="x",
                canonical_entity_id="nope:missing",
                entity_type="company",
            )

    def test_blank_alias_text_raises(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        with pytest.raises(ValueError, match="blank"):
            service.add_entity_alias_service(
                loaded_manifest,
                alias_text="   ",
                canonical_entity_id=target_id,
                entity_type="company",
            )

    def test_cross_type_alias_rejected(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        """P1.1 regression: alias entity_type must match canonical entity."""
        target_id, _ = _pair_of_normalized_entities  # a 'company' entity
        with pytest.raises(ValueError, match="entity_type mismatch"):
            service.add_entity_alias_service(
                loaded_manifest,
                alias_text="wrong-type",
                canonical_entity_id=target_id,
                entity_type="person",  # target is company
            )

    def test_inactive_canonical_rejected(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        """P1.1 regression: cannot anchor alias to an inactive entity."""
        config = service._config(loaded_manifest)
        # Mark target inactive
        with store.open_connection(config) as conn:
            conn.execute(
                "UPDATE normalized_entities SET status='inactive' WHERE id='company:alpha'"
            )
            conn.commit()
        with pytest.raises(ValueError, match="not active"):
            service.add_entity_alias_service(
                loaded_manifest,
                alias_text="x",
                canonical_entity_id="company:alpha",
                entity_type="company",
            )

    def test_partial_unique_index_blocks_bypass(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        """P1.3 regression: even if the service-layer pre-check is
        bypassed (simulating a concurrent write that landed between our
        SELECT and INSERT), the partial-unique index on
        (domain, entity_type, alias_key) WHERE status='active' MUST
        reject the second active duplicate at the DB layer.

        We drive this by calling the low-level raw INSERT directly — the
        IntegrityError surfacing path is what ``add_entity_alias`` wraps
        in its except-and-rehydrate handler. We assert the DB-layer
        invariant here and rely on code review for the handler shape.
        """
        import sqlite3

        target_id, _ = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Racer",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        config = service._config(loaded_manifest)
        with store.open_connection(config) as conn:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    """INSERT INTO entity_aliases(
                        canonical_entity_id, entity_type, domain, alias_text,
                        alias_key, confidence, status, source, source_refs_json,
                        created_at_utc, updated_at_utc
                    )
                    VALUES (?, 'company', 'global', 'Racer', 'racer', 1.0,
                            'active', 'manual', '[]',
                            '2026-04-19T15:00:00Z', '2026-04-19T15:00:00Z')
                    """,
                    (target_id,),
                )

    def test_feature_flag_off_returns_disabled(
        self, loaded_manifest, _pair_of_normalized_entities, monkeypatch
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        monkeypatch.setenv("CHRONICLE_ENABLE_ENTITY_ALIASES", "0")
        out = service.add_entity_alias_service(
            loaded_manifest,
            alias_text="x",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        assert out["status"] == "disabled"


class TestMergeEntities:
    def test_dry_run_reports_counts_without_writing(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, other_id = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="on-other",
            canonical_entity_id=other_id,
            entity_type="company",
        )
        summary = service.merge_entities_service(
            loaded_manifest,
            source_entity_id=other_id,
            target_entity_id=target_id,
            reason="test",
            dry_run=True,
        )
        assert summary["status"] == "dry_run"
        assert summary["aliases_repointed"] >= 1
        config = service._config(loaded_manifest)
        with store.open_connection(config) as c:
            src_status = c.execute(
                "SELECT status FROM normalized_entities WHERE id = ?", (other_id,)
            ).fetchone()["status"]
        assert src_status == "active"  # dry run did not flip source

    def test_merge_repoints_aliases_and_marks_source_inactive(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, other_id = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="on-other-a",
            canonical_entity_id=other_id,
            entity_type="company",
        )
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="on-other-b",
            canonical_entity_id=other_id,
            entity_type="company",
        )
        summary = service.merge_entities_service(
            loaded_manifest,
            source_entity_id=other_id,
            target_entity_id=target_id,
            reason="consolidate",
            actor="test-agent",
        )
        assert summary["status"] == "ok"
        assert summary["aliases_repointed"] == 2

        config = service._config(loaded_manifest)
        with store.open_connection(config) as c:
            src_row = c.execute(
                "SELECT status, metadata_json FROM normalized_entities WHERE id = ?",
                (other_id,),
            ).fetchone()
            src_aliases = c.execute(
                "SELECT COUNT(*) FROM entity_aliases WHERE canonical_entity_id = ?",
                (other_id,),
            ).fetchone()[0]
            tgt_aliases = c.execute(
                "SELECT COUNT(*) FROM entity_aliases WHERE canonical_entity_id = ?",
                (target_id,),
            ).fetchone()[0]
        assert src_row["status"] == "inactive"
        assert "merged_into" in (src_row["metadata_json"] or "")
        assert src_aliases == 0
        assert tgt_aliases == 2

    def test_merge_same_id_rejected(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        with pytest.raises(ValueError, match="differ"):
            service.merge_entities_service(
                loaded_manifest,
                source_entity_id=target_id,
                target_entity_id=target_id,
                reason="noop",
            )

    def test_merge_unknown_source_raises(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        with pytest.raises(ValueError, match="not found"):
            service.merge_entities_service(
                loaded_manifest,
                source_entity_id="company:doesnotexist",
                target_entity_id=target_id,
                reason="test",
            )

    def test_merge_requires_reason(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, other_id = _pair_of_normalized_entities
        with pytest.raises(ValueError, match="reason"):
            service.merge_entities_service(
                loaded_manifest,
                source_entity_id=other_id,
                target_entity_id=target_id,
                reason="",
            )

    def test_cross_type_merge_rejected(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        """P1.1 regression: cannot merge a company into a person."""
        config = service._config(loaded_manifest)
        store.upsert_normalized_entity(
            config, entity_type="person", canonical_key="ivan",
            canonical_name="Ivan", aliases=["Ivan"], source_refs=[],
        )
        target_id, _ = _pair_of_normalized_entities  # company
        with pytest.raises(ValueError, match="across entity types"):
            service.merge_entities_service(
                loaded_manifest,
                source_entity_id="person:ivan",
                target_entity_id=target_id,
                reason="bug attempt",
            )

    def test_merge_inactive_source_rejected(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        """P1.1 regression: inactive source cannot be merged again."""
        target_id, other_id = _pair_of_normalized_entities
        config = service._config(loaded_manifest)
        with store.open_connection(config) as conn:
            conn.execute(
                "UPDATE normalized_entities SET status='inactive' WHERE id = ?",
                (other_id,),
            )
            conn.commit()
        with pytest.raises(ValueError, match="not active"):
            service.merge_entities_service(
                loaded_manifest,
                source_entity_id=other_id,
                target_entity_id=target_id,
                reason="retry",
            )


class TestEntityResolutionReport:
    def test_empty_baseline(self, loaded_manifest) -> None:
        rep = service.entity_resolution_report_service(loaded_manifest)
        assert rep["status"] == "ok"
        assert rep["aliases_total"] == 0
        assert rep["aliases_active"] == 0
        assert rep["fragmentation_candidates"] == []

    def test_counts_after_seed(
        self, loaded_manifest, _pair_of_normalized_entities
    ) -> None:
        target_id, _ = _pair_of_normalized_entities
        service.add_entity_alias_service(
            loaded_manifest,
            alias_text="Alpha-Alt",
            canonical_entity_id=target_id,
            entity_type="company",
        )
        rep = service.entity_resolution_report_service(loaded_manifest)
        assert rep["aliases_total"] == 1
        assert rep["aliases_active"] == 1
        assert rep["normalized_entities_active"] >= 2

    def test_fragmentation_candidate_surfaces(
        self, loaded_manifest
    ) -> None:
        config = service._config(loaded_manifest)
        # Two normalized entities with the same lower(canonical_name) should
        # be flagged as a fragmentation candidate.
        store.upsert_normalized_entity(
            config, entity_type="person", canonical_key="ivan",
            canonical_name="Ivan", aliases=["Ivan"], source_refs=[],
        )
        store.upsert_normalized_entity(
            config, entity_type="person", canonical_key="ivan-alt",
            canonical_name="IVAN", aliases=["IVAN"], source_refs=[],
        )
        rep = service.entity_resolution_report_service(loaded_manifest)
        buckets = [c["bucket"] for c in rep["fragmentation_candidates"]]
        assert "ivan" in buckets


class TestEventContentHashDedup:
    def test_records_content_hash_on_insert(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        e = service.record_event(
            loaded_manifest,
            {"text": "dedup sample 1", "category": "note", "agent": "t"},
            source_kind="agent_command",
        )
        assert e["chronicle_status"] == "stored"
        config = service._config(loaded_manifest)
        with store.open_connection(config) as c:
            row = c.execute(
                "SELECT content_hash FROM events WHERE id = ?", (e["id"],)
            ).fetchone()
        assert row["content_hash"] is not None
        assert len(row["content_hash"]) == 64

    def test_duplicate_returns_existing_id(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        e1 = service.record_event(
            loaded_manifest,
            {"text": "dedup sample 2", "category": "note", "agent": "t"},
            source_kind="agent_command",
        )
        e2 = service.record_event(
            loaded_manifest,
            {"text": "dedup sample 2", "category": "note", "agent": "t"},
            source_kind="agent_command",
        )
        assert e2["id"] == e1["id"]
        assert e2["chronicle_status"] == "existing"
        assert e2["dedupe_status"] == "content_hash_match"

        config = service._config(loaded_manifest)
        with store.open_connection(config) as c:
            n = c.execute(
                "SELECT COUNT(*) FROM events WHERE text = ?", ("dedup sample 2",)
            ).fetchone()[0]
        assert n == 1

    def test_different_text_creates_new_row(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        e1 = service.record_event(
            loaded_manifest,
            {"text": "alpha", "category": "note"},
            source_kind="agent_command",
        )
        e2 = service.record_event(
            loaded_manifest,
            {"text": "beta", "category": "note"},
            source_kind="agent_command",
        )
        assert e1["id"] != e2["id"]

    def test_feature_flag_off_skips_hash_dedup(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "0")
        e1 = service.record_event(
            loaded_manifest,
            {"text": "no-dedup", "category": "note"},
            source_kind="agent_command",
        )
        e2 = service.record_event(
            loaded_manifest,
            {"text": "no-dedup", "category": "note"},
            source_kind="agent_command",
        )
        # Without the flag, two rows are persisted (legacy semantics).
        assert e1["id"] != e2["id"]

    def test_legacy_dedupe_path_still_works(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "0")
        e1 = service.record_event(
            loaded_manifest,
            {"text": "legacy dedup", "category": "note", "agent": "t"},
            source_kind="agent_command",
            dedupe=True,
            dedupe_window_hours=24,
        )
        e2 = service.record_event(
            loaded_manifest,
            {"text": "legacy dedup", "category": "note", "agent": "t"},
            source_kind="agent_command",
            dedupe=True,
            dedupe_window_hours=24,
        )
        assert e2["id"] == e1["id"]
        assert e2.get("dedupe_status") == "exact_duplicate"

    def test_dedup_duplicate_returns_full_event_shape(
        self, loaded_manifest, monkeypatch
    ) -> None:
        """P0.2 regression: dedup path must return 'recorded_at',
        'project', 'agent', etc. — not the raw DB row. Downstream
        native_automation reads stored["recorded_at"] and would crash
        on the old shape."""
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        service.record_event(
            loaded_manifest,
            {"text": "shape test", "category": "note", "agent": "shape-t"},
            source_kind="agent_command",
        )
        dup = service.record_event(
            loaded_manifest,
            {"text": "shape test", "category": "note", "agent": "shape-t"},
            source_kind="agent_command",
        )
        # Must have canonical event fields, not raw DB column names.
        assert "recorded_at" in dup
        assert "agent" in dup
        assert "project" in dup
        assert "category" in dup
        # And the dedup markers.
        assert dup["chronicle_status"] == "existing"
        assert dup["dedupe_status"] == "content_hash_match"
        assert dup["artifacts_written"] == 0

    def test_dedup_splits_by_domain_and_project(
        self, loaded_manifest, monkeypatch
    ) -> None:
        """P0.1 regression: same text/category but different domain/project
        must NOT be treated as duplicates — otherwise digest_run events
        from distinct projects collide.
        """
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        e_work = service.record_event(
            loaded_manifest,
            {
                "text": "nightly digest",
                "category": "digest_run",
                "agent": "auto",
                "domain": "work",
                "project": "news-digest",
            },
            source_kind="agent_command",
        )
        e_personal = service.record_event(
            loaded_manifest,
            {
                "text": "nightly digest",
                "category": "digest_run",
                "agent": "auto",
                "domain": "personal",
                "project": "demo-portfolio",
            },
            source_kind="agent_command",
        )
        # Different logical events → must create distinct rows.
        assert e_work["id"] != e_personal["id"]

    def test_toctou_transaction_wrap_prevents_race(
        self, loaded_manifest, monkeypatch
    ) -> None:
        """P1.2 regression: find_event_by_content_hash_recent + store_event
        run inside the same BEGIN IMMEDIATE transaction. We can't cheaply
        simulate a true race here, so we assert the observable contract:
        identical sequential writes collapse to one row and the second
        call returns the existing id.
        """
        monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
        payload = {
            "text": "toctou payload",
            "category": "note",
            "agent": "t",
        }
        e1 = service.record_event(loaded_manifest, payload, source_kind="agent_command")
        e2 = service.record_event(loaded_manifest, dict(payload), source_kind="agent_command")
        e3 = service.record_event(loaded_manifest, dict(payload), source_kind="agent_command")
        assert e1["id"] == e2["id"] == e3["id"]
        config = service._config(loaded_manifest)
        with store.open_connection(config) as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM events WHERE text = 'toctou payload'"
            ).fetchone()[0]
        assert n == 1


class TestSearchMem0Live:
    def test_disabled_flag_returns_empty(
        self, loaded_manifest, monkeypatch
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_MEM0_LIVE_SEARCH", "0")
        out = service.search_mem0_live_service(
            loaded_manifest, query="anything"
        )
        assert out["status"] == "disabled"
        assert out["results"] == []

    def test_blank_query_raises(self, loaded_manifest) -> None:
        with pytest.raises(ValueError, match="blank"):
            service.search_mem0_live_service(loaded_manifest, query="")

    def test_bridge_missing_is_degraded(
        self, loaded_manifest, monkeypatch, tmp_path
    ) -> None:
        """Point the mem0_bridge resolver at a nonexistent path."""
        monkeypatch.setenv("CHRONICLE_ENABLE_MEM0_LIVE_SEARCH", "1")
        missing = tmp_path / "missing.py"
        monkeypatch.setattr(
            service, "_resolve_mem0_bridge_path", lambda _m: missing
        )
        out = service.search_mem0_live_service(
            loaded_manifest, query="x", timeout_s=5
        )
        assert out["status"] == "degraded"
        assert "not found" in out["reason"]
        assert out["results"] == []

    def test_bridge_nonzero_is_degraded(
        self, loaded_manifest, monkeypatch, tmp_path
    ) -> None:
        """Stub subprocess.run to simulate a bridge that exits nonzero."""
        monkeypatch.setenv("CHRONICLE_ENABLE_MEM0_LIVE_SEARCH", "1")
        fake_bridge = tmp_path / "bridge.py"
        fake_bridge.write_text("# stub")
        monkeypatch.setattr(
            service, "_resolve_mem0_bridge_path", lambda _m: fake_bridge
        )

        class _FakeProc:
            returncode = 1
            stdout = ""
            stderr = "boom\n"

        monkeypatch.setattr(service.subprocess, "run", lambda *a, **k: _FakeProc())
        out = service.search_mem0_live_service(
            loaded_manifest, query="x", timeout_s=5
        )
        assert out["status"] == "degraded"
        assert "exit=1" in out["reason"]
        assert out["results"] == []

    def test_bridge_timeout_is_degraded(
        self, loaded_manifest, monkeypatch, tmp_path
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_MEM0_LIVE_SEARCH", "1")
        fake_bridge = tmp_path / "bridge.py"
        fake_bridge.write_text("# stub")
        monkeypatch.setattr(
            service, "_resolve_mem0_bridge_path", lambda _m: fake_bridge
        )

        def _raise_timeout(*_a, **_k):
            raise subprocess.TimeoutExpired(cmd="x", timeout=0.1)

        monkeypatch.setattr(service.subprocess, "run", _raise_timeout)
        out = service.search_mem0_live_service(
            loaded_manifest, query="x", timeout_s=0.1
        )
        assert out["status"] == "degraded"
        assert "timed out" in out["reason"]

    def test_bridge_parses_json_payload(
        self, loaded_manifest, monkeypatch, tmp_path
    ) -> None:
        monkeypatch.setenv("CHRONICLE_ENABLE_MEM0_LIVE_SEARCH", "1")
        fake_bridge = tmp_path / "bridge.py"
        fake_bridge.write_text("# stub")
        monkeypatch.setattr(
            service, "_resolve_mem0_bridge_path", lambda _m: fake_bridge
        )

        class _FakeProc:
            returncode = 0
            stdout = (
                'some stderr-like log line\n'
                '{"query":"x","collection":"personal","results":'
                '[{"id":"m-1","memory":"hi","score":0.42,"metadata":{}}],'
                '"count":1}\n'
            )
            stderr = ""

        monkeypatch.setattr(service.subprocess, "run", lambda *a, **k: _FakeProc())
        out = service.search_mem0_live_service(
            loaded_manifest, query="x", timeout_s=5
        )
        assert out["status"] == "ok"
        assert out["count"] == 1
        assert out["results"][0]["id"] == "m-1"

    def test_invalid_collection_raises(self, loaded_manifest) -> None:
        with pytest.raises(ValueError, match="collection"):
            service.search_mem0_live_service(
                loaded_manifest, query="x", collection="bogus"
            )


# --------------------------------------------------------------------------
# MCP server registration — make sure the 4 v8 tools are exposed.
# --------------------------------------------------------------------------


class TestMCPServerRegistration:
    def test_chronicler_profile_exposes_v8_tools(self, chronicle_sandbox) -> None:
        import asyncio
        from max_chronicle.mcp_server import build_server, CHRONICLER_PROFILE

        server = build_server(
            manifest_path=chronicle_sandbox.manifest_path,
            profile=CHRONICLER_PROFILE,
        )
        tools = asyncio.run(server.list_tools())
        names = {t.name for t in tools}
        # v8 quartet consolidated into entity_admin (15->10 tool-surface simplification)
        assert {"entity_admin", "search_mem0_live"}.issubset(names)
        assert "add_entity_alias" not in names
        assert "merge_entities" not in names
        assert "entity_resolution_report" not in names

    def test_readonly_profile_excludes_v8_write_tools(self, chronicle_sandbox) -> None:
        import asyncio
        from max_chronicle.mcp_server import build_server, READ_ONLY_PROFILE

        server = build_server(
            manifest_path=chronicle_sandbox.manifest_path,
            profile=READ_ONLY_PROFILE,
        )
        tools = asyncio.run(server.list_tools())
        names = {t.name for t in tools}
        # Entity admin + mem0 live search are chronicler-only.
        assert "entity_admin" not in names
        assert "search_mem0_live" not in names
