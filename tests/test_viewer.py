"""The read-only web view: every page renders, nothing recorded runs, nothing quarantined shows."""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from max_chronicle import service, viewer
from max_chronicle.mcp_server import build_server

BASE = "http://127.0.0.1:8093"


@pytest.fixture()
def client(chronicle_sandbox):
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app(), base_url=BASE) as test_client:
        yield test_client


def _record(manifest, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields},
                                append_compat=False)


def test_every_page_renders_with_the_security_headers(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to move the gateway to port 9000", project="alpha",
                     task_id="migrate", category="decision",
                     checkpoint={"goal": "Move the gateway", "next_steps": ["Switch DNS"]},
                     fact={"slot": "gateway.port", "value": "9000", "kind": "decision"})

    pages = {
        "/ui": "Move the gateway",
        "/ui/project/alpha": "gateway.port",
        "/ui/task/alpha/migrate": "Switch DNS",
        "/ui/facts": "gateway.port",
        "/ui/facts?slot=gateway.port": "9000",
        "/ui/search?q=gateway+port&project=alpha": "port 9000",
        f"/ui/event/{stored['id']}": "Decided to move the gateway",
    }
    for path, expected in pages.items():
        response = client.get(path)
        assert response.status_code == 200, path
        assert expected in response.text, path
        assert response.headers["content-type"].startswith("text/html")
        assert "script-src" not in response.headers["content-security-policy"]
        assert response.headers["content-security-policy"].startswith("default-src 'none'")
        assert response.headers["x-frame-options"] == "DENY"


def test_recorded_text_is_shown_as_text_never_as_markup(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, 'Payload <script>alert(1)</script> and <img src=x onerror="x()">',
                     project="alpha")

    for path in ("/ui", "/ui/project/alpha", f"/ui/event/{stored['id']}"):
        text = client.get(path).text
        assert "<script>alert" not in text and "<img src=x" not in text, path
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in text, path


def test_a_quarantined_event_never_appears(client, loaded_manifest) -> None:
    hidden = _record(loaded_manifest, "Quarantined zebra secret", project="alpha",
                     memory_guard={"visibility": "raw_only"})

    for path in ("/ui", "/ui/project/alpha", "/ui/search?q=zebra"):
        assert "zebra" not in client.get(path).text.replace("q=zebra", "").replace('value="zebra"', "").replace(
            "Search: zebra", ""), path
    assert client.get(f"/ui/event/{hidden['id']}").status_code == 404


def test_a_replaced_fact_keeps_its_history(client, loaded_manifest) -> None:
    first = _record(loaded_manifest, "Storage decision", project="alpha",
                    fact={"slot": "database", "value": "SQLite", "kind": "decision"})
    _record(loaded_manifest, "Storage decision", project="alpha",
            fact={"slot": "database", "value": "PostgreSQL", "kind": "decision", "supersedes": first["fact_id"]})

    current = client.get("/ui/facts").text
    history = client.get("/ui/facts?slot=database").text

    assert "PostgreSQL" in current and "SQLite" not in current
    assert history.index("PostgreSQL") < history.index("SQLite") and "replaced" in history


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({}, 200),
        ({"host": "localhost:8093"}, 200),
        ({"origin": BASE}, 200),  # a form on the page itself
        ({"sec-fetch-site": "same-origin"}, 200),
        ({"host": "evil.example"}, 403),  # DNS rebinding keeps the attacker's host name
        ({"origin": "http://evil.example"}, 403),
        ({"sec-fetch-site": "cross-site"}, 403),
        ({"sec-fetch-site": "same-site"}, 403),
    ],
    ids=["loopback", "localhost", "own-origin", "same-origin", "foreign-host", "foreign-origin", "cross-site",
         "same-site"],
)
def test_pages_answer_only_this_machine(client, loaded_manifest, headers, status) -> None:
    _record(loaded_manifest, "Something to show", project="alpha")

    assert client.get("/ui", headers=headers).status_code == status


def test_without_a_database_a_page_says_so(client) -> None:
    response = client.get("/ui")

    assert response.status_code == 503 and "FileNotFoundError" in response.text


def test_an_unknown_page_is_not_found(client) -> None:
    assert client.get("/ui/nowhere").status_code == 404
    assert client.get("/ui/event/does-not-exist").status_code == 404


def test_an_encoded_slash_stays_inside_its_segment() -> None:
    assert viewer.segments_of(b"/ui/task/alpha/a%2Fb") == ["task", "alpha", "a/b"]
    assert viewer.segments_of(b"/ui") == [] and viewer.segments_of(b"/ui/") == []


def test_a_task_id_with_a_slash_links_to_its_page(client, loaded_manifest) -> None:
    _record(loaded_manifest, "Handoff", project="alpha", task_id="fix/viewer",
            checkpoint={"goal": "Ship the viewer", "next_steps": ["Merge"]})

    assert "/ui/task/alpha/fix%2Fviewer" in client.get("/ui").text
    assert "Ship the viewer" in client.get("/ui/task/alpha/fix%2Fviewer").text


def test_a_quarantined_checkpoint_and_fact_stay_off_the_task_and_fact_pages(client, loaded_manifest) -> None:
    _record(loaded_manifest, "Visible step", project="alpha", task_id="t1",
            checkpoint={"goal": "Visible goal", "next_steps": ["Go on"]})
    _record(loaded_manifest, "Hidden step", project="alpha", task_id="t1",
            checkpoint={"goal": "Hiddengoal", "next_steps": ["Hiddenstep"]},
            fact={"slot": "hidden.slot", "value": "Hiddenvalue", "kind": "decision"},
            memory_guard={"visibility": "raw_only"})

    pages = [client.get(path).text for path in
             ("/ui", "/ui/project/alpha", "/ui/task/alpha/t1", "/ui/facts", "/ui/facts?slot=hidden.slot")]

    assert "Visible goal" in pages[2]
    assert not any(secret in page for page in pages for secret in ("Hiddengoal", "Hiddenstep", "Hiddenvalue"))
