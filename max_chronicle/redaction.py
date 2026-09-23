"""Redact likely credentials before text is stored, indexed, or archived.

Detection reads content, never file names: known key prefixes, values assigned
to secret names (snake, kebab, or camel case), labelled keys, bearer and basic
credentials, URL passwords, private key blocks, and long high-entropy tokens
near a word that speaks of tokens, keys, passwords, or secrets. Memory is full
of commit hashes, sha256 digests, UUIDs, and file or folder ids, so a
high-entropy token alone is not enough, and hexadecimal strings or pieces of a
long base64 run never count as one; such a secret is caught by its prefix or by
the name it is assigned to.

Every pattern does bounded work per match attempt, so redaction stays linear
in its input: it runs inside the single write slot, where one slow call would
stall every agent's writes.
"""

from __future__ import annotations

import base64
import binascii
import bisect
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import functools
import math
import re
from typing import Any

MARKER = "[REDACTED:{kind}]"
ENTROPY_MIN_LENGTH = 32
# How far a token's entropy may fall below that expected of a random token with
# the same length and alphabet, in bits times the square root of the length. A
# random key falls further about once in a thousand draws. Identifiers that mix
# words and digits often stay within it too, which is why a cue must be near.
ENTROPY_MAX_SHORTFALL = 3.0
# A short line is one note: a cue anywhere in it covers every token. On a longer
# line (a JSON log, a minified file) only the text around each cue is examined.
ENTROPY_WHOLE_LINE_LENGTH = 2000
ENTROPY_CUE_REACH = 256  # how far from a cue a token may start or end in a long line
# Longer runs of base64 characters are encoded data (an image, a bundle), not a
# credential; tokens inside them are never examined.
ENTROPY_MAX_TOKEN_LENGTH = 1024
ASSIGNED_VALUE_MIN_LENGTH = 8
VALUE_MAX_LENGTH = 4096
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
        ("gitlab_token", r"\bglpat-[A-Za-z0-9_\-]{20,255}"),
        ("huggingface_token", r"\bhf_[A-Za-z0-9]{30,255}"),
        ("xai_key", r"\bxai-[A-Za-z0-9]{20,255}"),
        ("runpod_key", r"\brpa_[A-Za-z0-9]{20,255}"),
        ("replicate_token", r"\br8_[A-Za-z0-9]{20,255}"),
        ("aws_access_key", r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
        ("google_api_key", r"\bAIza[0-9A-Za-z_\-]{35}"),
        ("slack_token", r"\bxox[abposr]-[A-Za-z0-9\-]{10,255}"),
        ("stripe_key", r"\b(?:sk|rk)_(?:live|test)_[0-9A-Za-z]{20,255}"),
        # Anchored at a token start: "eyJ-eyJ-..." cannot restart inside a run.
        (
            "jwt",
            r"(?<![A-Za-z0-9_\-])eyJ[A-Za-z0-9_\-]{8,65536}\.eyJ[A-Za-z0-9_\-]{8,65536}\.[A-Za-z0-9_\-]{8,65536}",
        ),
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
_SECRET_WORD = (
    r"(?:api[_\-]?key|access[_\-]?key|secret(?:[_\-]?key)?|token|passw(?:or)?d|pwd|private[_\-]?key|credentials?)"
)
_ASSIGN = r"[\"']?[ \t]{0,64}[:=][ \t]{0,64}[\"']?"
_VALUE = r"(?P<value>[^\s\"'`,;<>(){}\[\]\\]{1,%d})" % VALUE_MAX_LENGTH
# In a URL query or fragment a value ends at "&"; anywhere else "&" is part of it.
_QUERY_PARAM = re.compile(
    r"(?i)(?<=[?&#])[A-Za-z0-9_.\-]{0,64}?"
    + _SECRET_WORD
    + r"=(?P<value>[^&#\s\"'<>]{1,%d})" % VALUE_MAX_LENGTH
)
# A value assigned to a name ending in a secret word: API_KEY=..., "token": "...".
_ASSIGNED = re.compile(r"(?i)(?<![A-Za-z0-9])" + _SECRET_WORD + r"(?![A-Za-z0-9])" + _ASSIGN + _VALUE)
# The same in camel case: authToken: ..., clientSecret = ...
_ASSIGNED_CAMEL = re.compile(
    r"(?<=[a-z])(?:Token|Secret|Password|Passwd|ApiKey|AccessKey|PrivateKey|Credentials?)(?![a-z0-9])"
    + _ASSIGN
    + _VALUE
)
# A mapping key naming a secret ("password", "AWS_SECRET_ACCESS_KEY", "accessToken").
_SECRET_NAME = re.compile(r"(?i)" + _SECRET_WORD + r"$")
# "Production key <random>", "WaveSpeed key: <random>": the value must look random.
_KEY_LABEL = re.compile(
    r"(?i)(?<![A-Za-z0-9])keys?(?:[ \t]{1,4}[:=]?|[:=])[ \t]{0,4}[\"'`]?(?P<value>[A-Za-z0-9_\-+/=]{16,%d})"
    % VALUE_MAX_LENGTH
)
_BEARER = re.compile(r"(?i)\bbearer[ \t]{1,8}(?P<value>[A-Za-z0-9_\-.=~+/]{16,%d})" % VALUE_MAX_LENGTH)
_BASIC = re.compile(r"(?i)\bbasic[ \t]{1,8}(?P<value>[A-Za-z0-9+/]{4,%d}={0,2})" % VALUE_MAX_LENGTH)
_URL_PASSWORD = re.compile(r"(?i)\b[a-z][a-z0-9+.\-]{0,31}://[^\s:/@]{1,256}:(?P<value>[^\s@/]{1,256})@")
# A digest after its algorithm name (an SSH fingerprint) is public, not a secret.
_TOKEN = re.compile(
    r"(?<![A-Za-z0-9_\-+])(?<![Ss][Hh][Aa]256:)(?<![Ss][Hh][Aa]512:)[A-Za-z0-9_\-+]{%d,}={0,2}" % ENTROPY_MIN_LENGTH
)
_HEX = re.compile(r"[0-9a-fA-F\-]+")
# Parts of a slug or file name: a short word or number, or a longer word, camel
# case included, with digits on one side only (batch8, 720p, SeriesB, chromeProfile).
_SLUG_SHORT_PART = re.compile(r"[a-z0-9]{1,5}|[A-Z0-9]{1,5}|[A-Z][a-z0-9]{1,4}")
_SLUG_WORD = re.compile(r"[a-z]+[0-9]*|[0-9]+[a-z]*|[A-Z]?[a-z]+(?:[A-Z][a-z]+)*[A-Z]?[0-9]*|[A-Z]+[0-9]*|[0-9]+[A-Z]+")
# Anchored at a run start, so each run is scanned once.
_BLOB = re.compile(r"(?<![A-Za-z0-9+/=_\-])[A-Za-z0-9+/=_\-]{%d,}" % (ENTROPY_MAX_TOKEN_LENGTH + 1))
# Words that make a random-looking token nearby likely a credential. English
# cues stand apart from base64 characters, where "key" or "token" occur by chance;
# "_" and "-" may join them to a name (ACCESS_TOKEN), and camel case counts too.
_SECRET_CUE = re.compile(
    r"(?i)(?<![A-Za-z0-9+/=])"
    r"(?:api[\s_\-]?keys?|access[\s_\-]?keys?|tokens?|secrets?|passw(?:or)?ds?|pwd|credentials?|bearer)"
    r"(?![A-Za-z0-9+/=])"
    r"|(?-i:(?<=[a-z])(?:Token|Secret|Password|Key)(?![a-z]))"
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
    text = _replace_group(_QUERY_PARAM, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_ASSIGNED, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_ASSIGNED_CAMEL, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_KEY_LABEL, text, "labelled_key", mark, check=_high_entropy)
    text = _replace_group(_BEARER, text, "bearer_token", mark, check=_not_a_marker)
    text = _replace_group(_BASIC, text, "basic_credential", mark, check=_basic_credential)
    text = _replace_group(_URL_PASSWORD, text, "url_password", mark, check=_not_a_marker)
    text = "".join(_redact_entropy(line, mark) for line in text.splitlines(keepends=True))
    return Redaction(text=text, counts=dict(counts))


def _redact_entropy(line: str, mark: Callable[[str], str]) -> str:
    """Replace high-entropy tokens that stand near a secret cue in one line."""
    cues = [(cue.start(), cue.end()) for cue in _SECRET_CUE.finditer(line)]
    if not cues:
        return line
    starts = [start for start, _ in cues]
    blobs = [(blob.start(), blob.end()) for blob in _BLOB.finditer(line)]
    blob_starts = [start for start, _ in blobs]

    def near_cue(match: re.Match[str]) -> bool:
        if len(line) <= ENTROPY_WHOLE_LINE_LENGTH:
            return True
        # The last cue starting before the token's reach ends furthest right.
        index = bisect.bisect_right(starts, match.end() + ENTROPY_CUE_REACH) - 1
        return index >= 0 and cues[index][1] >= match.start() - ENTROPY_CUE_REACH

    def in_blob(match: re.Match[str]) -> bool:
        index = bisect.bisect_right(blob_starts, match.start()) - 1
        return index >= 0 and blobs[index][1] > match.start()

    def replace(match: re.Match[str]) -> str:
        if near_cue(match) and not in_blob(match) and _high_entropy(match.group()):
            return mark("high_entropy")
        return match.group()

    return _TOKEN.sub(replace, line)


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


def _basic_credential(value: str) -> bool:
    """An HTTP Basic credential: base64 of "user:password"."""
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return False
    return b":" in decoded and decoded.isascii()


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
    if not ENTROPY_MIN_LENGTH // 2 <= len(token) <= ENTROPY_MAX_TOKEN_LENGTH or _HEX.fullmatch(token):
        return False
    if not re.search(r"[A-Za-z]", token) or not re.search(r"[0-9]", token) or _slug(token):
        return False
    alphabet = sum(size for pattern, size in _CHARACTER_CLASSES if re.search(pattern, token))
    shortfall = _random_entropy(len(token), alphabet) - _shannon_bits(token)
    return shortfall * math.sqrt(len(token)) <= ENTROPY_MAX_SHORTFALL


def _slug(token: str) -> bool:
    """Words and numbers joined by -, _, or +, like a file name or a branch."""
    parts = [part for part in re.split(r"[-_+]+", token) if part]
    return len(parts) > 1 and all(_SLUG_SHORT_PART.fullmatch(part) or _SLUG_WORD.fullmatch(part) for part in parts)


def _shannon_bits(token: str) -> float:
    total = len(token)
    return -sum(n / total * math.log2(n / total) for n in Counter(token).values())


@functools.lru_cache(maxsize=4096)
def _random_entropy(length: int, alphabet: int) -> float:
    """Expected Shannon entropy, in bits, of a uniformly random token.

    Each symbol's count is Binomial(length, 1/alphabet); the sum runs until the
    remaining counts are too unlikely to matter.
    """
    p = 1 / alphabet
    probability = (1 - p) ** length  # of a count of zero
    mean = length * p
    bits = 0.0
    for count in range(1, length + 1):
        probability *= (length - count + 1) / count * p / (1 - p)
        bits -= probability * count / length * math.log2(count / length)
        if count > mean and probability < 1e-12:
            break
    return alphabet * bits


def redact_value(value: Any, *, skip_keys: frozenset[str] = frozenset()) -> tuple[Any, Counter[str]]:
    """Redact every string inside ``value``, except under ``skip_keys`` of a mapping.

    A string under a key named like a secret ("password", "AWS_SECRET_ACCESS_KEY",
    "accessToken") is redacted whole when it looks like a credential, since its
    own text would not say so.
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
