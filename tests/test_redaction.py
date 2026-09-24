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
    ("gitlab_token", "gl" + "pat-" + _random(20)),
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
        "I have a basic understanding of the importer",
        "tokenizer=bert-base-uncased",
        "passwordless request " + _random(40),
        "keyboardShortcuts" + _random(40),
        "https://example.com/?token=abc&project=chronicle",
        '{"max_tokens": 100, "id": "chatcmpl-' + _random(29) + '"}',
        '{"usage": {"prompt_tokens": 12}, "id": "' + _random(40) + '"}',
        "token_count: 1532, request " + _random(40),
        "publicKey " + _random(40),
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
        "Harbor_run-video-batch-07-r2-final-cut",  # slug
        "wan22_i2v_720p_turbo_10steps_v3_final",  # slug
        "lighthouse-batch8-production-lessons",  # slug of long words
        "harbor-nightlyRenderQueue-2026-08-31",  # slug with a camel-case part
        "release-PyQt6-Qt6-OpenGL-ES3-migration",  # branch with acronyms
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


@pytest.mark.parametrize("name", ["client_secret", "X-Amz-Security-Token", "access_token"])
def test_a_secret_parameter_ends_at_the_next_parameter(name: str) -> None:
    result = redact(f"https://app.example.com/callback#state=1&{name}={_random(24)}&expires_in=3600")

    assert result.text == f"https://app.example.com/callback#state=1&{name}=[REDACTED:assigned_secret]&expires_in=3600"


@pytest.mark.parametrize(
    "template",
    ['password="{}"', '{{"password": "{}"}}', "DB_PASSWORD={}", "password={}"],
)
def test_an_ampersand_outside_a_query_string_is_part_of_the_value(template: str) -> None:
    value = _random(6) + "&" + _random(9)

    result = redact(template.format(value))

    assert result.text == template.format("[REDACTED:assigned_secret]")


@pytest.mark.parametrize("credential", ["admin:" + _random(20), "u:" + _random(2)])
def test_basic_credentials_are_redacted(credential: str) -> None:
    encoded = base64.b64encode(credential.encode()).decode()

    result = redact(f"Authorization: Basic {encoded}")

    assert result.text == "Authorization: Basic [REDACTED:basic_credential]"


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        ("ACCESS_TOKEN " + _random(40), "high_entropy"),
        ("Production key " + _random(40), "labelled_key"),
        ("authToken: " + _random(40), "assigned_secret"),
        ('{"api_key":' + " " * 16 + '"' + _random(32) + '"}', "assigned_secret"),
        ("AWS_SESSION_TOKEN=" + _random(792, ALNUM + "+/"), "assigned_secret"),
        ("ey" + "J" + _random(20) + ".ey" + "J" + _random(10_000) + "." + _random(43), "jwt"),
        ("postgres://app:" + _random(1) + "@db.local/main", "url_password"),
        ("WAVESPEED_TOKEN " + _random(40), "high_entropy"),
        ("key: " + base64.urlsafe_b64encode(random.Random(8).randbytes(900)).decode(), "labelled_key"),
        ("key: " + base64.b32encode(random.Random(9).randbytes(320)).decode(), "labelled_key"),
        ("key: " + "-".join(_random(5, string.ascii_uppercase + string.digits) for _ in range(5)), "labelled_key"),
        ("key: ABCDEFG2-HIJKLMN3-OPQRSTU4-VWXYZ567", "labelled_key"),
    ],
    ids=[
        "name-joined-cue",
        "labelled",
        "camel-case",
        "wide-json",
        "long-session-token",
        "large-jwt",
        "short-password",
        "provider-token-name",
        "long-labelled-key",
        "base32-key",
        "license-key",
        "capital-groups",
    ],
)
def test_secrets_in_less_common_shapes_are_redacted(text: str, kind: str) -> None:
    result = redact(text)

    assert result.counts == {kind: 1}


@pytest.mark.parametrize(
    "alphabet",
    [
        string.ascii_lowercase + string.digits,
        string.ascii_uppercase + string.digits,
        string.ascii_uppercase + "234567",
        ALNUM,
        ALNUM + "-_",
    ],
    ids=["lower-digits", "upper-digits", "base32", "alnum", "url-safe"],
)
def test_random_keys_of_any_alphabet_are_redacted_near_a_cue(alphabet: str) -> None:
    rng = random.Random(len(alphabet))
    keys = [
        "".join(rng.choice(alphabet) for _ in range(length))
        for length in (32, 40, 48, 64, 128, 256)
        for _ in range(100)
    ]

    missed = [key for key in keys if key in redact(f"token {key}").text]

    assert len(missed) <= len(keys) // 100  # the share-of-maximum threshold missed up to 44%


def test_on_a_long_line_only_tokens_near_a_cue_count() -> None:
    near, far, filler = _random(40), _random(40), "x " * 1200

    result = redact(f"{filler}rotated token {near} {filler}build {far}")

    assert near not in result.text and far in result.text
    assert result.counts == {"high_entropy": 1}


def test_an_encoded_image_beside_a_token_field_is_kept_whole() -> None:
    token = _random(40)
    image = base64.b64encode(random.Random(3).randbytes(3000)).decode("ascii")

    result = redact(json.dumps({"access_token": token, "image": image, "note": "auth token"}))

    assert token not in result.text and image in result.text
    assert result.counts == {"assigned_secret": 1}


@pytest.mark.parametrize("key", ["AWS_SECRET_ACCESS_KEY", "accessToken", "clientSecret"])
def test_a_value_under_any_secret_name_is_redacted_whole(key: str) -> None:
    redacted, counts = redact_value({key: _random(24), "keyboard": "qwerty-" + _random(12)})

    assert redacted[key] == "[REDACTED:assigned_secret]"
    assert redacted["keyboard"].startswith("qwerty-")
    assert counts == {"assigned_secret": 1}


@pytest.mark.parametrize(("encrypted", "prefix"), [(True, ""), (False, "> "), (False, "")],
                         ids=["pem-headers", "quoted", "plain"])
def test_a_private_key_cut_before_its_end_line_is_redacted(encrypted: bool, prefix: str) -> None:
    body = [_random(64) for _ in range(8)]
    lines = ["-----BEGIN " + "RSA PRIVATE" + " KEY-----"]
    if encrypted:
        lines += ["Proc-Type: 4,ENCRYPTED", "DEK-Info: AES-128-CBC," + _random(32, "0123456789ABCDEF"), ""]
    text = "The key as pasted:\n" + "\n".join(prefix + line for line in lines + body) + "\n\nThen rotate it.\n"

    result = redact(text).text

    assert not any(line in result for line in body)
    assert result.startswith("The key as pasted:\n") and result.endswith("\nThen rotate it.\n")


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
        ("ey" + "J-") * 250_000,
        "Bearer " * 150_000,
        "Basic " * 170_000,
        "key: " * 200_000,
        "?token=" * 140_000,
        "aToken: " * 125_000,
        ("token " + "A" * 1024 + " ") * 1000,
        "token " + base64.b64encode(random.Random(9).randbytes(750_000)).decode("ascii"),
    ],
    ids=[
        "underscores",
        "dots",
        "pluses",
        "open-key-blocks",
        "assignments",
        "url-schemes",
        "jwt-head",
        "jwt-heads",
        "bearers",
        "basics",
        "key-labels",
        "query-tokens",
        "camel-tokens",
        "near-blob-runs",
        "base64-line",
    ],
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


def test_files_that_redact_alike_keep_their_own_provenance(loaded_manifest, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    notes = []
    for folder in ("first", "second"):
        note = tmp_path / folder / "pod-notes.md"
        note.parent.mkdir()
        note.write_text("RunPod API key: " + "rp" + "a_" + _random(40) + "\n", encoding="utf-8")
        notes.append(note)

    for note in notes:
        service.record_event(loaded_manifest, {"agent": "agent-a", "text": "Documented a pod", "source_files": [str(note)]})

    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        rows = connection.execute("SELECT sha256, source_path, metadata_json FROM artifacts").fetchall()
    assert len({row["sha256"] for row in rows}) == 1  # both notes redact to the same bytes
    provenance = {row["source_path"]: json.loads(row["metadata_json"])["source_sha256"] for row in rows}
    assert provenance == {str(note): hashlib.sha256(note.read_bytes()).hexdigest() for note in notes}


def test_a_long_source_name_still_fits_the_archive(loaded_manifest, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    note = tmp_path / ("pod-access-notes-" + "x" * 180 + ".md")
    note.write_text("RunPod API key: " + "rp" + "a_" + _random(40) + "\n", encoding="utf-8")

    receipt = service.record_event(loaded_manifest, {"agent": "agent-a", "text": "Documented a pod", "source_files": [str(note)]})

    [evidence] = receipt["evidence"]
    assert evidence["status"] == "archived" and evidence["redactions"] == {"runpod_key": 1}


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
