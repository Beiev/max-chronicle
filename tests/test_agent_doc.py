"""docs/AGENT_PROTOCOL.md is the surface the server serves (docs == tools)."""

from __future__ import annotations

from max_chronicle import agent_doc


def test_the_agent_protocol_page_matches_the_registered_tools() -> None:
    page = agent_doc.PAGE.read_text(encoding="utf-8")

    assert page == agent_doc.render(), "docs/AGENT_PROTOCOL.md is stale: run python -m max_chronicle.agent_doc"


def test_every_default_tool_has_a_section() -> None:
    page = agent_doc.render()

    for name in agent_doc.TOOL_ORDER:
        assert f"### `{name}`" in page
