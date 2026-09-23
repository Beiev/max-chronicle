"""Redact likely credentials before text is stored, indexed, or archived.

Detection reads content, never file names: known key prefixes, secret-named
assignments, bearer tokens, URL passwords, private key blocks, and long
high-entropy tokens on a line that speaks of keys, tokens, or passwords.
Memory is full of commit hashes, sha256 digests, UUIDs, and file or folder ids,
so a high-entropy token alone is not enough, and hexadecimal strings never
count; such a secret is caught by its prefix or by the name it is assigned to.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
import math
import re
from typing import Any

MARKER = "[REDACTED:{kind}]"
ENTROPY_MIN_LENGTH = 32
# Share of the highest entropy reachable for a token's length and alphabet.
# Random keys reach ~0.9 of it; file names and slugs built from words stay below.
ENTROPY_MIN_SHARE = 0.85
_CHARACTER_CLASSES = ((r"[a-z]", 26), (r"[A-Z]", 26), (r"[0-9]", 10), (r"[\-_+]", 3))
ENTROPY_MAX_SEPARATOR_SHARE = 0.1  # slugs are joined by - or _ every few letters
ASSIGNED_VALUE_MIN_LENGTH = 8

# Most specific first: an Anthropic or OpenRouter key also starts with "sk-".
_PREFIXED = tuple(
    (kind, re.compile(pattern))
    for kind, pattern in (
        ("private_key", r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----"),
        ("anthropic_key", r"\bsk-ant-[A-Za-z0-9_\-]{20,}"),
        ("openrouter_key", r"\bsk-or-v1-[A-Za-z0-9]{20,}"),
        ("openai_key", r"\bsk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_\-]{20,}"),
        ("github_token", r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
        ("huggingface_token", r"\bhf_[A-Za-z0-9]{30,}"),
        ("xai_key", r"\bxai-[A-Za-z0-9]{20,}"),
        ("runpod_key", r"\brpa_[A-Za-z0-9]{20,}"),
        ("replicate_token", r"\br8_[A-Za-z0-9]{20,}"),
        ("aws_access_key", r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        ("google_api_key", r"\bAIza[0-9A-Za-z_\-]{35}"),
        ("slack_token", r"\bxox[abposr]-[A-Za-z0-9\-]{10,}"),
        ("stripe_key", r"\b(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{20,}"),
        ("jwt", r"\beyJ[A-Za-z0-9_\-]{8,}\.eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}"),
        ("telegram_bot_token", r"\b\d{8,10}:AA[A-Za-z0-9_\-]{33}\b"),
    )
)
# A value assigned to a name that ends in a secret word: API_KEY=..., "token": "...".
_ASSIGNED = re.compile(
    r"(?i)(?<![A-Za-z0-9])(?P<name>(?:[A-Za-z0-9]+[_\-.])*"
    r"(?:api[_\-]?key|secret(?:[_\-]?key)?|token|passw(?:or)?d|pwd|private[_\-]?key|credentials?))"
    r"(?![A-Za-z0-9])[\"']?\s*[:=]\s*[\"']?(?P<value>[^\s\"'`,;<>(){}\[\]\\]+)"
)
_BEARER = re.compile(r"(?i)\bbearer\s+(?P<value>[A-Za-z0-9_\-.=~+/]{16,})")
_URL_PASSWORD = re.compile(r"(?i)\b[a-z][a-z0-9+.\-]*://[^\s:/@]+:(?P<value>[^\s@/]{3,})@")
# A digest after its algorithm name (an SSH fingerprint) is public, not a secret.
_TOKEN = re.compile(
    r"(?<![A-Za-z0-9_\-+])(?<![Ss][Hh][Aa]256:)(?<![Ss][Hh][Aa]512:)[A-Za-z0-9_\-+]{%d,}={0,2}" % ENTROPY_MIN_LENGTH
)
_HEX = re.compile(r"[0-9a-fA-F\-]+")
# Words that make a random-looking token on the same line likely a credential.
_SECRET_CUE = re.compile(
    r"(?i)(?<![A-Za-z])(?:keys?|api[_\-]?keys?|tokens?|secrets?|passw\w*|pwd|credentials?|bearer|auth\w*)(?![A-Za-z])"
    # Russian nouns with their case endings; "ключевой" (main, adj.) is no cue.
    r"|(?<!\w)(?:ключ(?:а|у|ом|е|и|ей|ам|ами|ах)?|токен\w{0,3}|парол[ьяюеи]\w{0,2}|секрет(?:а|у|ом|е|ы|ов|ами|ах)?)(?!\w)"
)
_PLACEHOLDER = re.compile(r"(?i)^(?:\$\{?|%|<|\[redacted|x{4,}|\*{3,}|\.{3}|your[_\-]|none$|null$|true$|false$)")
# An assigned value that names another variable (TOKEN=OTHER_API_KEY) is a reference.
_VARIABLE_NAME = re.compile(r"[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+")


@dataclass(frozen=True)
class Redaction:
    """Text with likely secrets replaced, and what was replaced."""

    text: str
    counts: Mapping[str, int]

    @property
    def count(self) -> int:
        return sum(self.counts.values())


def redact(text: str) -> Redaction:
    """Replace every likely secret in ``text`` with a ``[REDACTED:<kind>]`` marker."""
    counts: Counter[str] = Counter()

    def mark(kind: str) -> str:
        counts[kind] += 1
        return MARKER.format(kind=kind)

    for kind, pattern in _PREFIXED:
        text = pattern.sub(lambda match, kind=kind: mark(kind), text)
    text = _replace_group(_ASSIGNED, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_BEARER, text, "bearer_token", mark)
    text = _replace_group(_URL_PASSWORD, text, "url_password", mark)
    text = "".join(
        _TOKEN.sub(lambda match: mark("high_entropy") if _high_entropy(match.group()) else match.group(), line)
        if _SECRET_CUE.search(line)
        else line
        for line in text.splitlines(keepends=True)
    )
    return Redaction(text=text, counts=dict(counts))


def _replace_group(pattern: re.Pattern[str], text: str, kind: str, mark, *, check=None) -> str:
    def replace(match: re.Match[str]) -> str:
        value = match.group("value")
        if check is not None and not check(value):
            return match.group()
        start, end = match.span("value")
        whole_start = match.start()
        return match.group()[: start - whole_start] + mark(kind) + match.group()[end - whole_start :]

    return pattern.sub(replace, text)


def _plausible_assigned_value(value: str) -> bool:
    if len(value) < ASSIGNED_VALUE_MIN_LENGTH or _PLACEHOLDER.match(value) or _VARIABLE_NAME.fullmatch(value):
        return False
    # A credential mixes character classes; a plain word or number does not.
    classes = sum(
        bool(re.search(pattern, value)) for pattern in (r"[a-z]", r"[A-Z]", r"[0-9]", r"[^A-Za-z0-9]")
    )
    return classes >= 2


def _high_entropy(token: str) -> bool:
    token = token.rstrip("=")
    if _HEX.fullmatch(token) or not re.search(r"[A-Za-z]", token) or not re.search(r"[0-9]", token):
        return False
    separators = sum(char in "-_" for char in token)
    if separators / len(token) > ENTROPY_MAX_SEPARATOR_SHARE:
        return False
    alphabet = sum(size for pattern, size in _CHARACTER_CLASSES if re.search(pattern, token))
    reachable = min(math.log2(len(token)), math.log2(alphabet))
    return _shannon_bits(token) >= ENTROPY_MIN_SHARE * reachable


def _shannon_bits(token: str) -> float:
    total = len(token)
    return -sum(n / total * math.log2(n / total) for n in Counter(token).values())


def redact_value(value: Any, *, skip_keys: frozenset[str] = frozenset()) -> tuple[Any, Counter[str]]:
    """Redact every string inside ``value``, except under ``skip_keys`` of a mapping."""
    counts: Counter[str] = Counter()
    if isinstance(value, str):
        result = redact(value)
        counts.update(result.counts)
        return result.text, counts
    if isinstance(value, list):
        items = []
        for item in value:
            redacted, found = redact_value(item, skip_keys=skip_keys)
            items.append(redacted)
            counts.update(found)
        return items, counts
    if isinstance(value, dict):
        mapping = {}
        for key, item in value.items():
            if key in skip_keys:
                mapping[key] = item
                continue
            redacted, found = redact_value(item, skip_keys=skip_keys)
            mapping[key] = redacted
            counts.update(found)
        return mapping, counts
    return value, counts
