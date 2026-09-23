"""Redact likely credentials before text is stored, indexed, or archived.

Detection reads content, never file names: known key prefixes, values assigned
to secret names, labelled keys, bearer tokens, URL passwords, private key
blocks, and long high-entropy tokens on a line that speaks of tokens,
passwords, or secrets. Memory is full of commit hashes, sha256 digests, UUIDs,
and file or folder ids, so a high-entropy token alone is not enough, and
hexadecimal strings never count as one; such a secret is caught by its prefix
or by the name it is assigned to.

Every pattern does bounded work per match attempt, so redaction stays linear
in its input: it runs inside the single write slot, where one slow call would
stall every agent's writes.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
import re
from typing import Any

MARKER = "[REDACTED:{kind}]"
ENTROPY_MIN_LENGTH = 32
# Share of the highest entropy reachable for a token's length and alphabet.
# Random keys reach ~0.9 of it; file names and slugs built from words stay below.
ENTROPY_MIN_SHARE = 0.85
ENTROPY_MAX_SEPARATOR_SHARE = 0.1  # slugs are joined by - or _ every few letters
# Longer lines are data (an encoded image, a minified file), not "name: value"
# notes; a cue word found in one is noise, so the entropy rule skips them.
ENTROPY_MAX_LINE_LENGTH = 2000
ASSIGNED_VALUE_MIN_LENGTH = 8
PRIVATE_KEY_MAX_LENGTH = 16384
_CHARACTER_CLASSES = ((r"[a-z]", 26), (r"[A-Z]", 26), (r"[0-9]", 10), (r"[\-_+]", 3))

# Most specific first: an Anthropic or OpenRouter key also starts with "sk-".
_PREFIXED = tuple(
    (kind, re.compile(pattern))
    for kind, pattern in (
        ("anthropic_key", r"\bsk-ant-[A-Za-z0-9_\-]{20,512}"),
        ("openrouter_key", r"\bsk-or-v1-[A-Za-z0-9]{20,512}"),
        ("openai_key", r"\bsk-(?:proj-|svcacct-|admin-)?[A-Za-z0-9_\-]{20,512}"),
        ("github_token", r"\b(?:gh[pousr]_[A-Za-z0-9]{30,255}|github_pat_[A-Za-z0-9_]{30,255})"),
        ("huggingface_token", r"\bhf_[A-Za-z0-9]{30,255}"),
        ("xai_key", r"\bxai-[A-Za-z0-9]{20,255}"),
        ("runpod_key", r"\brpa_[A-Za-z0-9]{20,255}"),
        ("replicate_token", r"\br8_[A-Za-z0-9]{20,255}"),
        ("aws_access_key", r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        ("google_api_key", r"\bAIza[0-9A-Za-z_\-]{35}"),
        ("slack_token", r"\bxox[abposr]-[A-Za-z0-9\-]{10,255}"),
        ("stripe_key", r"\b(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{20,255}"),
        ("jwt", r"\beyJ[A-Za-z0-9_\-]{8,4096}\.eyJ[A-Za-z0-9_\-]{8,4096}\.[A-Za-z0-9_\-]{8,4096}"),
        ("telegram_bot_token", r"\b\d{8,10}:AA[A-Za-z0-9_\-]{33}\b"),
    )
)
# A whole block, its body never crossing another BEGIN; then a block cut short.
_PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----"
    r"(?:(?!-----BEGIN )[\s\S]){0,%d}?-----END [A-Z0-9 ]{0,40}PRIVATE KEY-----" % PRIVATE_KEY_MAX_LENGTH
)
_PRIVATE_KEY_OPEN = re.compile(
    r"-----BEGIN [A-Z0-9 ]{0,40}PRIVATE KEY-----[A-Za-z0-9+/=\s]{0,%d}" % PRIVATE_KEY_MAX_LENGTH
)
# A value assigned to a name ending in a secret word: API_KEY=..., "token": "...".
# The word itself anchors the match; a separator or nothing may precede it.
_SECRET_WORD = r"(?:api[_\-]?key|secret(?:[_\-]?key)?|token|passw(?:or)?d|pwd|private[_\-]?key|credentials?)"
_ASSIGNED = re.compile(
    r"(?i)(?<![A-Za-z0-9])" + _SECRET_WORD + r"(?![A-Za-z0-9])"
    r"[\"']?\s{0,8}[:=]\s{0,8}[\"']?(?P<value>[^\s\"'`,;&<>(){}\[\]\\]{1,512})"
)
_SECRET_NAME = re.compile(r"(?i)(?:^|[_\-.])" + _SECRET_WORD + r"$")
# "WaveSpeed key: <random>": a label, then a value that must look random.
_KEY_LABEL = re.compile(r"(?i)(?<![A-Za-z0-9])keys?\s{0,4}[:=]\s{0,4}[\"'`]?(?P<value>[A-Za-z0-9_\-+/=]{16,512})")
_BEARER = re.compile(r"(?i)\bbearer\s{1,8}(?P<value>[A-Za-z0-9_\-.=~+/]{16,4096})")
_URL_PASSWORD = re.compile(r"(?i)\b[a-z][a-z0-9+.\-]{0,31}://[^\s:/@]{1,256}:(?P<value>[^\s@/]{3,256})@")
# A digest after its algorithm name (an SSH fingerprint) is public, not a secret.
_TOKEN = re.compile(
    r"(?<![A-Za-z0-9_\-+])(?<![Ss][Hh][Aa]256:)(?<![Ss][Hh][Aa]512:)[A-Za-z0-9_\-+]{%d,}={0,2}" % ENTROPY_MIN_LENGTH
)
_HEX = re.compile(r"[0-9a-fA-F\-]+")
# Words that make a random-looking token on the same line likely a credential.
# English cues must stand alone: inside base64, "key" or "auth" occur by chance.
_SECRET_CUE = re.compile(
    r"(?i)(?<![A-Za-z0-9+/=_\-])(?:api[\s_\-]?keys?|tokens?|secrets?|passw\w*|pwd|credentials?|bearer)"
    r"(?![A-Za-z0-9+/=_\-])"
    # Russian nouns with their case endings; "ключевой" (main, adj.) is no cue.
    r"|(?<!\w)(?:ключ(?:а|у|ом|е|и|ей|ам|ами|ах)?|токен\w{0,3}|парол[ьяюеи]\w{0,2}|секрет(?:а|у|ом|е|ы|ов|ами|ах)?)(?!\w)"
)
_PLACEHOLDER = re.compile(
    r"(?i)^(?:\$\{?|%|<|/|~/|\[redacted|x{4,}|\*{3,}|\.{3}|your[_\-]|none$|null$|true$|false$)"
)
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

    text = _PRIVATE_KEY_BLOCK.sub(lambda match: mark("private_key"), text)
    text = _PRIVATE_KEY_OPEN.sub(lambda match: mark("private_key"), text)
    for kind, pattern in _PREFIXED:
        text = pattern.sub(lambda match, kind=kind: mark(kind), text)
    text = _replace_group(_ASSIGNED, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_KEY_LABEL, text, "labelled_key", mark, check=_high_entropy)
    text = _replace_group(_BEARER, text, "bearer_token", mark, check=_not_a_marker)
    text = _replace_group(_URL_PASSWORD, text, "url_password", mark, check=_not_a_marker)
    text = "".join(
        _TOKEN.sub(lambda match: mark("high_entropy") if _high_entropy(match.group()) else match.group(), line)
        if len(line) <= ENTROPY_MAX_LINE_LENGTH and _SECRET_CUE.search(line)
        else line
        for line in text.splitlines(keepends=True)
    )
    return Redaction(text=text, counts=dict(counts))


def _replace_group(
    pattern: re.Pattern[str],
    text: str,
    kind: str,
    mark: Callable[[str], str],
    *,
    check: Callable[[str], bool],
) -> str:
    def replace(match: re.Match[str]) -> str:
        if not check(match.group("value")):
            return match.group()
        start, end = match.span("value")
        whole_start = match.start()
        return match.group()[: start - whole_start] + mark(kind) + match.group()[end - whole_start :]

    return pattern.sub(replace, text)


def _not_a_marker(value: str) -> bool:
    return not value.startswith("[REDACTED:")


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
    if len(token) < ENTROPY_MIN_LENGTH // 2 or _HEX.fullmatch(token):
        return False
    if not re.search(r"[A-Za-z]", token) or not re.search(r"[0-9]", token):
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
    """Redact every string inside ``value``, except under ``skip_keys`` of a mapping.

    A string under a key named like a secret ("password", "api_key") is
    redacted whole when it looks like a credential, since its own text
    would not say so.
    """
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
            if (
                isinstance(key, str)
                and isinstance(item, str)
                and _SECRET_NAME.search(key)
                and _plausible_assigned_value(item)
            ):
                mapping[key] = MARKER.format(kind="assigned_secret")
                counts["assigned_secret"] += 1
                continue
            redacted, found = redact_value(item, skip_keys=skip_keys)
            mapping[key] = redacted
            counts.update(found)
        return mapping, counts
    return value, counts
