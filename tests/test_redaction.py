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


def _key_body(lines: int = 8) -> list[str]:
    return [_random(64, ALNUM + "+/") for _ in range(lines)]


_BEGIN = "-----BEGIN " + "RSA PRIVATE" + " KEY-----"
_END = "-----END " + "RSA PRIVATE" + " KEY-----"


@pytest.mark.parametrize("shape", ["long_line", "indented", "tabs", "on_begin_line"])
def test_a_cut_private_key_is_redacted_in_any_layout(shape: str) -> None:
    body = _key_body()
    if shape == "long_line":
        text = _BEGIN + "\n" + "".join(body) + "\n"
    elif shape == "on_begin_line":
        text = _BEGIN + "".join(body) + "\n"
    else:
        indent = " " * 12 if shape == "indented" else "\t" * 9
        text = "\n".join([indent + _BEGIN, *(indent + line for line in body)]) + "\n"

    result = redact("Pasted:\n" + text + "Then rotate it.\n").text

    assert not any(line[8:40] in result for line in body)
    assert result.endswith("Then rotate it.\n")


@pytest.mark.parametrize("shape", ["heading_begin", "heading_inside", "json_escaped", "code_string", "pgp"])
def test_a_whole_private_key_goes_whatever_surrounds_it(shape: str) -> None:
    body = _key_body()
    if shape == "heading_begin":
        text = "## " + _BEGIN + "\n" + "\n".join(body) + "\n" + _END
    elif shape == "heading_inside":
        text = _BEGIN + "\n" + "\n".join(body[:4]) + "\n## Pasted in the middle\n" + "\n".join(body[4:]) + "\n" + _END
    elif shape == "json_escaped":
        text = '{"private_key": "' + _BEGIN + "\\n" + "\\n".join(body) + "\\n" + _END + '\\n"}'
    elif shape == "code_string":
        text = 'key = ("' + _BEGIN + '\\n"\n' + "\n".join(f'       "{line}\\n"' for line in body) + '\n       "' + _END + '")'
    else:
        begin, end = "-----BEGIN PGP " + "PRIVATE KEY BLOCK-----", "-----END PGP " + "PRIVATE KEY BLOCK-----"
        text = begin + "\n\n" + "\n".join(body) + "\n" + end

    result = redact("Before.\n" + text + "\nAfter.\n").text

    assert not any(line[8:40] in result for line in body)
    assert result.startswith("Before.") and result.endswith("After.\n")


def test_prose_between_two_mentions_of_a_key_stays() -> None:
    text = ("A key file starts with " + _BEGIN + " on its own.\n\n## Where it lives\n"
            "The deploy key lives in the password manager entry farm.\n\nIt ends with " + _END + ".\n")

    result = redact(text).text

    assert "password manager entry farm" in result and "## Where it lives" in result


@pytest.mark.parametrize(
    "text",
    [
        "a_" * 500_000,
        "a." * 500_000,
        "a+" * 500_000,
        "-----BEGIN " + "PRIVATE KEY-----\n" * 40_000,
        " " * 1_000_000,
        "-----BEGIN " + "PRIVATE KEY-----" + " " * 1_000_000 + "x",
        ("BEGIN " + " " * 50 + "A" + " " * 50) * 9_000,
        "&nbsp;" * 170_000,
        "-----BEGIN " + "PRIVATE KEY-----\n" + "\\" * 1_000_000,
        ("\\/" + "Ab1" * 5) * 50_000 + "-----END " + "PRIVATE KEY-----",
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
        "spaces",
        "key-then-spaces",
        "spaced-markers",
        "html-spaces",
        "backslashes",
        "escaped-runs",
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


# Fourth review of #14: a key's region loses its key material however the key is laid out.

_OPENSSH_END = "-----END OPENSSH " + "PRIVATE KEY-----"


def _layouts(body: list[str]) -> dict[str, str]:
    pgp = "-----BEGIN PGP " + "PRIVATE KEY BLOCK-----"
    return {
        "space_after_begin": _BEGIN + " \n" + "\n".join(body),
        "tab_crlf_after_begin": _BEGIN + "\t\r\n" + "\r\n".join(body),
        "comment_header": _BEGIN + "\nComment: imported from backup\n" + "\n".join(body),
        "lone_cr": _BEGIN + "\r" + "\r".join(body),
        "code_fence": _BEGIN + "\n```\n" + "\n".join(body) + "\n```",
        "list_items": "- " + _BEGIN + "\n" + "\n".join("- " + line for line in body),
        "heading_inside": _BEGIN + "\n## inserted\n" + "\n".join(body),
        "text_on_begin_line": _BEGIN + " pasted here\n" + "\n".join(body),
        "pgp_version": pgp + "\nVersion: GnuPG v2\n\n" + "\n".join(body),
        "past_any_line_limit": _BEGIN + "\n" + "\n".join(body * 70) + "\n",
        "foreign_end_inside": _BEGIN + "\n" + "\n".join(body[:4]) + "\n" + _OPENSSH_END + "\n" + "\n".join(body[4:]) + "\n" + _END,
        "begin_cut_off": "\n".join(body) + "\n" + _END,
        "no_dashes": "BEGIN RSA " + "PRIVATE KEY\n" + "\n".join(body),
    }


@pytest.mark.parametrize("shape", list(_layouts(["x"])))
def test_a_key_region_keeps_no_key_material(shape: str) -> None:
    body = _key_body()
    text = "Before.\n" + _layouts(body)[shape] + "\n"

    result = redact(text)

    assert not any(line[i:i + 16] in result.text for line in body for i in range(0, 49, 16))
    assert result.text.startswith("Before.") and result.counts.get("private_key")


def test_a_whole_key_is_one_marker() -> None:
    result = redact("Before.\n" + _BEGIN + "\n" + "\n".join(_key_body()) + "\n" + _END + "\nAfter.\n")

    assert (result.text, result.counts) == ("Before.\n[REDACTED:private_key]\nAfter.\n", {"private_key": 1})


def test_words_and_digests_in_a_key_region_stay() -> None:
    digest = hashlib.sha1(b"public-build").hexdigest()
    text = ("## Format\nA key file starts with " + _BEGIN + ".\n\n## Deployment\nRun the deploy script, then check "
            f"build {digest} and the documentation for troubleshooting.\n\n## Terminator\nIt ends with " + _END + ".\n")

    result = redact(text).text

    assert digest in result and "## Deployment\nRun the deploy script" in result and "troubleshooting" in result


# Fifth review of #14: a text bearing a key loses the key however the key is spelled or encoded.

_PKCS1 = bytes.fromhex("308204a40201000282010100")
_PKCS8 = bytes.fromhex("30820276020100300d06092a864886f70d0101010500048202")


def _pem(label: str = "RSA ", prefix: bytes = _PKCS1, size: int = 1190) -> tuple[str, list[str]]:
    body = base64.b64encode(prefix + random.randbytes(size - len(prefix))).decode()
    lines = [body[i:i + 64] for i in range(0, len(body), 64)]
    begin, end = "-----BEGIN " + label + "PRIVATE" + " KEY-----", "-----END " + label + "PRIVATE" + " KEY-----"
    return "\n".join([begin, *lines, end]) + "\n", lines


def _spelled(pem: str) -> dict[str, str]:
    marker = "PRIVATE" + " KEY"
    return {
        "nine_spaces": pem.replace("BEGIN ", "BEGIN" + " " * 9).replace("RSA " + marker, "RSA" + " " * 9 + marker),
        "lower_case": pem.replace("BEGIN RSA " + marker, "begin rsa private key").replace("END RSA " + marker, "end rsa private key"),
        "split_marker": pem.replace(marker, "PRIVATE\nKEY"),
        "split_marker_crlf": pem.replace(marker, "PRIVATE\r\nKEY"),
        "json_slashes": json.dumps({"data": pem}).replace("/", "\\/"),
        "json_unicode_spaces": json.dumps({"data": pem}).replace(" ", "\\u0020"),
        "html": pem.replace("/", "&#47;").replace(" ", "&nbsp;"),
        "url_encoded": __import__("urllib.parse").parse.quote(pem, safe=""),
        "base64url": pem.replace("+", "-").replace("/", "_"),
        "early_quoted_end": pem.split("\n")[0] + "\nThe terminator is `" + pem.rstrip("\n").split("\n")[-1] + "`.\n\n"
                            + "\n".join(pem.split("\n")[1:4]),
        "no_markers": "\n".join(pem.split("\n")[1:-2]),
    }


@pytest.mark.parametrize("shape", list(_spelled(_pem()[0])))
def test_a_key_goes_however_it_is_spelled_or_encoded(shape: str) -> None:
    pem, lines = _pem()
    text = "Keys:\n" + _spelled(pem)[shape] + "\nRotate them.\n"

    result = redact(text).text
    readable = __import__("urllib.parse").parse.unquote(result.replace("\\/", "/").replace("&#47;", "/"))

    assert not any(line[i:i + 12] in readable for line in lines[:12] for i in range(len(line) - 11))
    assert result.startswith("Keys:") and result.endswith("Rotate them.\n")


@pytest.mark.parametrize("tail", [16, 12])
def test_the_short_last_line_of_a_key_goes_with_it(tail: int) -> None:
    size = {16: 634, 12: 631}[tail]  # base64 of these sizes ends in a line of `tail` characters with == padding
    pem, lines = _pem("", _PKCS8, size)

    result = redact("Export:\n" + pem).text

    assert len(lines[-1]) == tail and lines[-1].endswith("==")
    assert lines[-1] not in result and result == "Export:\n[REDACTED:private_key]\n"


def test_a_mention_of_a_key_keeps_paths_links_names_and_images() -> None:
    image = "data:image/png;base64," + base64.b64encode(b"\x89PNG\r\n\x1a\n" + random.randbytes(600)).decode()
    kept = ["/usr/local/bin/python3", "https://github.com/example/tool/commit/" + hashlib.sha1(b"c").hexdigest(),
            "https://example.org/api/v1/conversations", "release20260926candidate1", image]
    text = "A key file starts with " + "-----BEGIN " + "RSA PRIVATE" + " KEY-----" + ".\n\n" + "\n".join(kept) + "\n"

    result = redact(text).text

    assert all(item in result for item in kept)


# Sixth review of #14: key formats known by their content, escaped line breaks, and what stays.

_DER_STARTS = {  # structure only; the private bytes that follow are random
    "pkcs12": "308209f2020103308209a806092a864886f70d010701a082",
    "p384_pkcs8": "3081b6020100301006072a8648ce3d020106052b81040022",
    "p384_sec1": "3081a40201010430",
    "p384_pbes2": "30820124305f06092a864886f70d01050d3052303106092a",
    "p521_pkcs8": "3081ee020100301006072a8648ce3d020106052b81040023",
    "secp256k1_pkcs8": "308184020100301006072a8648ce3d020106052b8104000a",
    "secp256k1_sec1": "30740201010420",
    "secp256k1_pbes2": "3081f4305f06092a864886f70d01050d3052303106092a86",
    "openpgp_secret": "c5c2d8046ab80dd8",
}


def _b64_lines(data: bytes, width: int = 64) -> list[str]:
    body = base64.b64encode(data).decode()
    return [body[i:i + width] for i in range(0, len(body), width)]


@pytest.mark.parametrize("name", list(_DER_STARTS))
def test_a_bare_key_body_is_known_by_its_first_bytes(name: str) -> None:
    lines = _b64_lines(bytes.fromhex(_DER_STARTS[name]) + random.randbytes(300))
    result = redact("Pasted:\n" + "\n".join(lines) + "\nDone.\n").text

    assert not any(line[i:i + 12] in result for line in lines[1:] for i in range(len(line) - 11))
    assert result.startswith("Pasted:\n") and result.endswith("Done.\n")


def _ppk() -> tuple[str, list[str]]:
    private = _b64_lines(random.randbytes(640))
    text = ("PuTTY-User-Key-File-3: ssh-rsa\nEncryption: none\nComment: example\nPublic-Lines: 2\n"
            + "\n".join(_b64_lines(random.randbytes(90))) + f"\nPrivate-Lines: {len(private)}\n" + "\n".join(private)
            + "\nPrivate-MAC: " + random.randbytes(32).hex() + "\n")
    return text, private


def _jwk() -> tuple[str, list[str]]:
    def member() -> str:
        return base64.urlsafe_b64encode(random.randbytes(256)).decode().rstrip("=")

    values = {name: member() for name in ("n", "d", "p", "q", "dp", "dq", "qi")}
    return json.dumps({"kty": "RSA", "e": "AQAB", **values}, indent=2), [values[name] for name in ("d", "p", "q")]


def _wrapped_pem() -> tuple[str, list[str]]:
    pem, lines = _pem()
    return base64.b64encode(pem.encode()).decode(), [base64.b64encode(pem.encode()).decode()[200:600]]


@pytest.mark.parametrize("shape", ["ppk", "jwk", "kubernetes", "cloud_key_download", "non_image_data_uri"])
def test_keys_in_other_formats_go(shape: str) -> None:
    if shape == "ppk":
        text, secrets_ = _ppk()
    elif shape == "jwk":
        text, secrets_ = _jwk()
    elif shape == "kubernetes":
        encoded, secrets_ = _wrapped_pem()
        text = "apiVersion: v1\nkind: Secret\ndata:\n  tls.key: " + encoded + "\n"
    elif shape == "cloud_key_download":
        pem, _ = _pem("", _PKCS8)
        encoded = base64.b64encode(json.dumps({"type": "service_account", "private_key": pem}).encode()).decode()
        text, secrets_ = json.dumps({"privateKeyData": encoded}), [encoded[300:700]]
    else:
        pem, lines = _pem()
        text = "-----BEGIN RSA " + "PRIVATE KEY-----\ndata:application/octet-stream;base64," + "".join(lines) + "\n"
        secrets_ = lines[2:6]

    result = redact(text).text

    assert not any(value[i:i + 16] in result for value in secrets_ for i in range(len(value) - 15))


@pytest.mark.parametrize("wrap", ["json", "double_json", "toml", "html_br", "json_unicode", "html_named"])
def test_every_line_of_a_key_goes_through_escaped_line_breaks(wrap: str) -> None:
    pem, lines = _pem("", _PKCS8, 631)  # ends in a 12-character line
    presented = {
        "json": json.dumps({"key": pem}),
        "double_json": json.dumps({"payload": json.dumps({"key": pem})}),
        "toml": 'key = "' + pem.replace("\n", "\\n") + '"',
        "html_br": pem.replace("\n", "<br>"),
        "json_unicode": json.dumps({"key": pem}).replace("/", "\\u002f").replace("+", "\\u002b"),
        "html_named": "<pre>" + pem.replace("/", "&sol;").replace("+", "&plus;").replace("=", "&equals;") + "</pre>",
    }[wrap]

    result = redact(presented).text

    assert len(lines[-1]) == 12 and lines[-1].rstrip("=") not in result
    assert not any(line[i:i + 12] in result for line in lines for i in range(len(line) - 11))


def test_word_like_and_short_last_lines_go_with_their_key() -> None:
    body = [line for line in _pem()[1][:-1]] + ["kQymwxFEVEFbtfpW"]
    wrapped = [piece for line in body for piece in (line[i:i + 16] for i in range(0, len(line), 16))]
    for lines in (body, wrapped):
        result = redact("-----BEGIN RSA " + "PRIVATE KEY-----\n" + "\n".join(lines) + "\n-----END RSA " + "PRIVATE KEY-----\n")
        assert result.text == "[REDACTED:private_key]\n"


def test_text_before_a_key_and_prose_between_its_parts_stay() -> None:
    kept = ["/usr/lib/x86_64-linux-gnu", "/api/v1/users/123456", "CHRONICLE_DB=/tmp/demo.db",
            "AWS_DEFAULT_REGION=us-east-1", "foo_bar_baz_quux_123", "01234567-89aB-cDef-0123-456789abcdef",
            "A0b1C2d3E4f5A0b1C2d3E4f5A0b1C2d3E4f5A0b1"]
    pem, lines = _pem()
    head, tail = pem.split("\n", 5)[:5], pem.split("\n", 5)[5]
    text = "\n".join(kept) + "\n\n" + "\n".join(head) + "\nЭто ключ продакшена, не трогать до ротации.\n" + tail

    result = redact(text).text

    assert all(item in result for item in kept) and "Это ключ продакшена, не трогать до ротации." in result
    assert not any(line[i:i + 12] in result for line in lines for i in range(len(line) - 11))


def test_a_long_run_of_zeros_in_an_entity_does_not_break_the_filter() -> None:
    pem, _ = _pem()

    assert "[REDACTED:private_key]" in redact(pem + "&#" + "0" * 5000 + "47;").text


# Seventh review of #14: more real export formats, nested escaping, and text after a key.

_DER_STARTS_7 = {  # structure only
    "x25519": ("302e020100300506032b656e04220420", 32),
    "ed448": ("3047020100300506032b6571043b0439", 57),
    "x448": ("3046020100300506032b656f043a0438", 56),
}


@pytest.mark.parametrize("name", list(_DER_STARTS_7))
def test_bare_modern_curve_keys_go(name: str) -> None:
    start, size = _DER_STARTS_7[name]
    body = base64.b64encode(bytes.fromhex(start) + random.randbytes(size)).decode()

    result = redact("Pasted from the export:\n" + body + "\n").text  # no word that names a key

    assert body[-20:] not in result and result.startswith("Pasted from the export:")


def _jwk_members() -> dict[str, str]:
    return {name: base64.urlsafe_b64encode(random.randbytes(128)).decode().rstrip("=")
            for name in ("n", "p", "q", "dp", "dq", "qi", "d")}


@pytest.mark.parametrize("shape", ["d_last", "double_json", "python_repr"])
def test_a_jwk_goes_in_any_order_or_serialization(shape: str) -> None:
    members = _jwk_members()
    jwk = {"kty": "RSA", "e": "AQAB", **members}  # "d" comes last
    text = {"d_last": json.dumps(jwk, indent=2), "double_json": json.dumps({"key": json.dumps(jwk)}),
            "python_repr": repr(jwk)}[shape]

    result = redact(text).text

    assert not any(members[name][i:i + 16] in result for name in ("p", "q", "d") for i in range(0, 150, 16))


def test_a_dotnet_xml_key_goes() -> None:
    parts = {name: base64.b64encode(random.randbytes(128)).decode() for name in ("P", "Q", "DP", "DQ", "InverseQ", "D")}
    text = ("<RSAKeyValue><Modulus>" + base64.b64encode(random.randbytes(256)).decode() + "</Modulus><Exponent>AQAB"
            "</Exponent>" + "".join(f"<{name}>{value}</{name}>" for name, value in parts.items()) + "</RSAKeyValue>")

    result = redact(text).text

    assert not any(value[i:i + 16] in result for value in parts.values() for i in range(0, 150, 16))


def test_an_openssl_text_dump_goes() -> None:
    def dump(data: bytes) -> str:
        pairs = [f"{byte:02x}" for byte in data]
        return "\n".join("    " + ":".join(pairs[i:i + 15]) + ":" for i in range(0, len(pairs), 15))

    secret = random.randbytes(256)
    text = ("Private-Key: (2048 bit, 2 primes)\nmodulus:\n" + dump(random.randbytes(257)) + "\npublicExponent: 65537 "
            "(0x10001)\nprivateExponent:\n" + dump(secret) + "\n")

    result = redact(text).text

    assert not any(f"{secret[i]:02x}:{secret[i + 1]:02x}:{secret[i + 2]:02x}" in result for i in range(0, 250, 5))
    assert "publicExponent: 65537" in result


def test_a_key_in_json_nested_in_json_goes() -> None:
    pem, lines = _pem("", _PKCS8)
    inner = json.dumps({"data": pem}).replace("+", "\\u002b").replace("/", "\\u002f")

    result = redact(json.dumps({"message": inner})).text

    assert not any(line[i:i + 12] in result for line in lines for i in range(len(line) - 11))


def test_code_and_links_after_a_key_stay() -> None:
    pem, _ = _pem()
    kept = ["const client = new AWSKMSClientBuilder();", "class XMLHTTPRequestHandler:",
            "https://example.invalid/docs/XMLHTTPRequestHandler", "https://api.example.com/v1/users/123456",
            "git clone https://git.example.com/v1/api/v2/tool", "GET /v1/projects/42/instances/9"]

    result = redact(pem + "\n" + "\n".join(kept) + "\n").text

    assert all(item in result for item in kept)


def test_a_secret_name_covers_the_strings_of_its_list() -> None:
    value, counts = redact_value({"password": ["Correct-Horse-9", "Battery-Staple-7"], "tags": ["Release-Notes-2"]})

    assert value == {"password": ["[REDACTED:assigned_secret]"] * 2, "tags": ["Release-Notes-2"]}
    assert counts["assigned_secret"] == 2


def test_json_held_in_a_string_is_filtered_at_any_depth_of_encoding() -> None:
    inner = {"password": "Correct-Horse-9", "note": "rotate monthly"}
    once = json.dumps(inner)
    twice = json.dumps({"nested": once})

    value, _ = redact_value({"twice": twice, "plain": "{not json", "list": "[1, 2]"})

    assert "Correct-Horse-9" not in json.dumps(value)
    assert json.loads(json.loads(value["twice"])["nested"]) == {"password": "[REDACTED:assigned_secret]",
                                                                "note": "rotate monthly"}
    assert (value["plain"], value["list"]) == ("{not json", "[1, 2]")  # unchanged, byte for byte


def test_json_nested_too_deep_stays_text() -> None:
    deep = "[" * 1100 + "0" + "]" * 1100  # walking it as a value would exhaust the stack

    value, counts = redact_value({"text": deep})

    assert value == {"text": deep} and not counts


@pytest.mark.parametrize("depth", [2, 33, 100])
def test_a_deep_branch_does_not_hide_a_shallow_secret(depth) -> None:
    trace: object = 0
    for _ in range(depth):
        trace = {"child": trace}
    text = json.dumps({"password": ["Correct-Horse-9"], "encoded": json.dumps({"password": "Battery-Staple-7"}),
                       "trace": trace})

    value, counts = redact_value(text)

    assert "Correct-Horse-9" not in value and "Battery-Staple-7" not in value and counts["assigned_secret"] == 2


@pytest.mark.parametrize("depth", [31, 1100])
def test_json_encoded_twice_is_filtered_at_any_depth(depth) -> None:
    text = "[" * depth + json.dumps(json.dumps({"password": ["Correct-Horse-9"]})) + "]" * depth

    value, counts = redact_value(text)

    assert "Correct-Horse-9" not in value and counts["assigned_secret"] == 1


def test_a_deep_value_is_walked_without_exhausting_the_stack() -> None:
    key = "sk-" + "proj-" + "Zq8" * 12
    deep: object = {"note": f"the key {key}"}
    for _ in range(5000):
        deep = [deep]

    value, counts = redact_value(deep)

    for _ in range(5000):  # json.dumps and repr would recurse as deep
        value = value[0]
    assert key not in value["note"] and "[REDACTED:" in value["note"] and counts


def test_json_too_deep_to_parse_goes_whole_when_it_names_a_secret(monkeypatch) -> None:
    import max_chronicle.redaction as redaction_module

    def too_deep(text):
        raise RecursionError("maximum recursion depth exceeded while decoding a JSON array")

    monkeypatch.setattr(redaction_module.json, "loads", too_deep)  # as Python 3.11 does past ~1,000 levels
    named, counts = redact_value('[[{"password": ["Correct-Horse-9"]}]]')
    plain, _ = redact_value("[[0]]")

    assert (named, counts["assigned_secret"]) == ("[REDACTED:assigned_secret]", 1) and plain == "[[0]]"
