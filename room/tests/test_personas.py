"""Smoke checks for the Agent Room persona configs in `.kiro/agents/`.

Asserts the three room-*.json files exist, chat personas carry no tools,
and chat prompts contain the brevity instruction, the Mention convention,
and the roster of other personas (Req 4.1, 4.5, 4.6, 4.7).
"""

import json
from pathlib import Path

import pytest

AGENTS_DIR = Path(__file__).resolve().parents[2] / ".kiro" / "agents"

PERSONA_FILES = ["room-pm.json", "room-architect.json", "room-prd-writer.json"]
CHAT_PERSONAS = ["room-pm.json", "room-architect.json"]

# Each chat persona's prompt must name the other chat persona (the roster).
OTHER_PERSONA = {"room-pm.json": "room-architect", "room-architect.json": "room-pm"}


def load_config(filename: str) -> dict:
    """Parse a persona config as JSON."""
    return json.loads((AGENTS_DIR / filename).read_text())


@pytest.mark.parametrize("filename", PERSONA_FILES)
def test_persona_file_exists_and_parses(filename):
    """All three room-*.json configs exist and parse as JSON (Req 4.1)."""
    path = AGENTS_DIR / filename
    assert path.is_file(), f"missing persona config: {path}"
    assert isinstance(load_config(filename), dict)


@pytest.mark.parametrize("filename", CHAT_PERSONAS)
def test_chat_persona_has_no_tools(filename):
    """Chat personas are voices, not actors: tools must be empty (Req 4.7)."""
    config = load_config(filename)
    assert config["tools"] == []


@pytest.mark.parametrize("filename", CHAT_PERSONAS)
def test_chat_prompt_has_brevity_instruction(filename):
    """Chat prompts instruct 2-4 sentence replies (Req 4.5)."""
    prompt = load_config(filename)["prompt"]
    assert "2-4 sentences" in prompt
    assert "chat room, not a design review" in prompt


@pytest.mark.parametrize("filename", CHAT_PERSONAS)
def test_chat_prompt_has_mention_convention_and_roster(filename):
    """Chat prompts state the @-mention convention and list the other
    room persona (Req 4.6)."""
    prompt = load_config(filename)["prompt"]
    assert "@" in prompt
    assert OTHER_PERSONA[filename] in prompt
