"""Secret redaction before storage, indexing, and archiving.

Every credential below is synthetic and assembled at run time, so the source
holds no string a secret scanner would take for a real key.
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import random
import string
import time

import pytest

from max_chronicle import service
from max_chronicle.redaction import redact, redact_value
from max_chronicle.store import config_from_manifest, open_connection

_RANDOM = random.Random(20260923)
ALNUM = string.ascii_letters + string.digits


def _random(length: int, alphabet: str = ALNUM) -> str:
    return "".join(_RANDOM.choice(alphabet) for _ in range(length))


PREFIXED = [
    ("openai_key", "sk-" + "proj-" + _random(48)),
    ("anthropic_key", "sk-" + "ant-api03-" + _random(48)),
    ("openrouter_key", "sk-" + "or-v1-" + _random(64, "0123456789abcdef")),
    ("github_token", "gh" + "p_" + _random(36)),
    ("huggingface_token", "hf" + "_" + _random(34)),
    ("xai_key", "xa" + "i-" + _random(48)),
    ("runpod_key", "rp" + "a_" + _random(40)),
    ("replicate_token", "r8" + "_" + _random(37)),
    ("aws_access_key", "AK" + "IA" + _random(16, string.ascii_uppercase + string.digits)),
    ("google_api_key", "AI" + "za" + _random(35)),
    ("slack_token", "xo" + "xb-" + _random(12, string.digits) + "-" + _random(24)),
    ("stripe_key", "sk" + "_live_" + _random(24)),
    ("jwt", "ey" + "J" + _random(24) + ".ey" + "J" + _random(24) + "." + _random(24)),
    ("telegram_bot_token", _random(9, string.digits) + ":A" + "A" + _random(33)),
    (
        "private_key",
        "-----BEGIN OPENSSH " + "PRIVATE KEY-----\n" + _random(64) + "\n-----END OPENSSH " + "PRIVATE KEY-----",
    ),
]


@pytest.mark.parametrize(("kind", "secret"), PREFIXED, ids=[kind for kind, _ in PREFIXED])
def test_known_key_formats_are_redacted(kind: str, secret: str) -> None:
    result = redact(f"Rotated the credential {secret} after the leak.")

    assert secret not in result.text
    assert f"[REDACTED:{kind}]" in result.text
    assert result.counts == {kind: 1}


@pytest.mark.parametrize(
    "text",
    [
        "OPENAI_API_KEY=" + _random(40),
        '{"api_key": "' + _random(32) + '"}',
        "password: " + _random(14),
        "client_secret = '" + _random(30) + "'",
    ],
)
def test_values_assigned_to_secret_names_are_redacted(text: str) -> None:
    result = redact(text)

    assert result.counts == {"assigned_secret": 1}
    assert "[REDACTED:assigned_secret]" in result.text


@pytest.mark.parametrize(
    "text",
    [
        "max_tokens=4000",
        "token_count: 12345678",
        "api_key=${OPENAI_API_KEY}",
        "ANTHROPIC_AUTH_TOKEN=ZAI_API_KEY",
        "password: <redacted>",
        "token = None",
        "the token expired yesterday",
        "PWD=/Users/someone/project",
    ],
)
def test_references_and_ordinary_words_stay(text: str) -> None:
    assert redact(text).text == text


def test_bearer_tokens_and_url_passwords_are_redacted() -> None:
    token = _random(40)

    result = redact(f"Authorization: Bearer {token}\npostgres://app:{_random(12)}@db.local/main")

    assert token not in result.text
    assert "postgres://app:[REDACTED:url_password]@db.local/main" in result.text
    assert result.counts == {"bearer_token": 1, "url_password": 1}


def test_a_random_token_is_redacted_only_where_the_line_speaks_of_secrets() -> None:
    key, folder, fresh_key, author_id = _random(48), _random(33), _random(40), _random(44)

    result = redact(
        f"WaveSpeed key: {key}\n"
        f"Uploaded to the Drive folder {folder}.\n"
        f"Ключ WaveSpeed заменили на {fresh_key}\n"
        f"Ключевые уроки записаны; папка {folder}\n"
        f"Key takeaway: the importer works; Drive folder {folder}\n"
        f"Author: agent {author_id}\n"
    )

    assert key not in result.text and fresh_key not in result.text
    assert result.text.count(folder) == 3 and author_id in result.text
    assert result.counts == {"labelled_key": 1, "high_entropy": 1}


@pytest.mark.parametrize(
    "value",
    [
        "0f3c1d9e8b7a6f5e4d3c2b1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e",  # sha256
        "d7f9babe215f3fb3601daaa210b903a47118579e",  # commit
        "123e4567-e89b-12d3-a456-426614174000",  # UUID
        "SHA256:" + _random(43),  # SSH fingerprint
        "agent-review-2026-09-checkpoint-0223",  # slug
        "Byzantine_stone_tablet_final_20260915.png",  # file name
    ],
)
def test_digests_ids_slugs_and_file_names_survive_on_a_token_line(value: str) -> None:
    text = f"The token record: {value}"

    assert redact(text).text == text


def test_redaction_is_idempotent() -> None:
    once = redact(
        "export GITHUB_TOKEN=" + "gh" + "p_" + _random(36) + "\npostgres://app:" + _random(12) + "@db.local/main"
    )

    again = redact(once.text)

    assert again.text == once.text
    assert again.count == 0


def test_a_query_string_keeps_everything_but_the_token() -> None:
    token = _random(24)

    result = redact(f"https://api.example.com/items?token={token}&page=2&sort=desc")

    assert result.text == "https://api.example.com/items?token=[REDACTED:assigned_secret]&page=2&sort=desc"


@pytest.mark.parametrize(
    "text",
    [
        "a_" * 500_000,
        "a." * 500_000,
        "a+" * 500_000,
        "-----BEGIN " + "PRIVATE KEY-----\n" * 40_000,
        "api_key=" * 130_000,
        "https://a:" * 100_000,
        "ey" + "J" + "a" * 1_000_000,
        "Bearer " * 150_000,
    ],
    ids=["underscores", "dots", "pluses", "open-key-blocks", "assignments", "url-schemes", "jwt-head", "bearers"],
)
def test_redaction_time_stays_linear_on_hostile_input(text: str) -> None:
    started = time.perf_counter()

    redact(text)

    assert time.perf_counter() - started < 3.0  # was quadratic: hours for 1 MB


def test_a_value_under_a_secret_named_key_is_redacted_whole() -> None:
    password = _random(20)

    redacted, counts = redact_value({"db": {"password": password, "host": "db.local"}})

    assert redacted == {"db": {"password": "[REDACTED:assigned_secret]", "host": "db.local"}}
    assert counts == {"assigned_secret": 1}


def test_nested_values_are_redacted_except_under_skipped_keys() -> None:
    secret = "hf" + "_" + _random(34)
    entry = {"request_id": secret, "checkpoint": {"next_steps": [f"Use {secret}"]}, "fact": {"slot": "x", "value": secret}}

    redacted, counts = redact_value(entry, skip_keys=frozenset({"request_id", "slot"}))

    assert redacted["request_id"] == secret
    assert redacted["checkpoint"]["next_steps"] == ["Use [REDACTED:huggingface_token]"]
    assert redacted["fact"]["value"] == "[REDACTED:huggingface_token]"
    assert counts == {"huggingface_token": 2}


def _stored_text(manifest: dict) -> str:
    """Every text column and payload of every event and observation, as one string."""
    config = config_from_manifest(manifest)
    with open_connection(config) as connection:
        rows = connection.execute("SELECT text, why, payload_json FROM events").fetchall()
        observations = connection.execute("SELECT payload_json FROM event_observations").fetchall()
    return json.dumps([list(row) for row in rows] + [list(row) for row in observations], ensure_ascii=False)


def test_record_event_never_stores_the_secret(loaded_manifest, chronicle_sandbox, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    secret = "sk-" + "proj-" + _random(48)
    entry = {
        "agent": "agent-a",
        "project": "demo",
        "task_id": "ship",
        "request_id": "rotate-1",
        "text": f"Deployed with key {secret}",
        "why": f"The old key {secret} leaked.",
        "checkpoint": {"goal": "Rotate keys", "next_steps": [f"Revoke {secret}"]},
    }

    receipt = service.record_event(loaded_manifest, dict(entry))
    retry = service.record_event(loaded_manifest, dict(entry))

    assert receipt["redactions"] == {"openai_key": 3}
    assert retry["chronicle_status"] == "existing" and retry["id"] == receipt["id"]
    assert "[REDACTED:openai_key]" in receipt["text"]
    assert secret not in _stored_text(loaded_manifest)
    ledger = (chronicle_sandbox.status_root / "ssot-ledger.jsonl").read_text(encoding="utf-8")
    assert secret not in ledger and "[REDACTED:openai_key]" in ledger


def test_evidence_files_are_archived_redacted(loaded_manifest, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    secret = "rp" + "a_" + _random(40)
    note = tmp_path / "pod-notes.md"
    note.write_text(f"# Pod access\nRunPod API key: {secret}\n", encoding="utf-8")

    receipt = service.record_event(
        loaded_manifest,
        {"agent": "agent-a", "text": "Documented pod access", "source_files": [str(note)]},
    )

    [evidence] = receipt["evidence"]
    assert evidence["redactions"] == {"runpod_key": 1}
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        storage_path, metadata = connection.execute(
            "SELECT storage_path, metadata_json FROM artifacts WHERE sha256 = ?", (evidence["sha256"],)
        ).fetchone()
    archived = Path(storage_path) if Path(storage_path).is_absolute() else config.artifact_dir / storage_path
    content = archived.read_text(encoding="utf-8")
    assert secret not in content and "[REDACTED:runpod_key]" in content
    assert json.loads(metadata)["redactions"] == {"runpod_key": 1}
    assert note.read_text(encoding="utf-8").count(secret) == 1  # the source file is never touched


def test_encoded_blobs_are_archived_unchanged(loaded_manifest, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    image = base64.b64encode(random.Random(5).randbytes(300_000)).decode("ascii")
    response = tmp_path / "image-response.json"
    response.write_text(json.dumps({"b64_json": image, "note": "keyframe auth token pwd"}), encoding="utf-8")

    receipt = service.record_event(
        loaded_manifest,
        {"agent": "agent-a", "text": "Saved the image response", "source_files": [str(response)]},
    )

    [evidence] = receipt["evidence"]
    assert "redactions" not in evidence
    assert evidence["sha256"] == hashlib.sha256(response.read_bytes()).hexdigest()
