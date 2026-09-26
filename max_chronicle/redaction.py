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
import json
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
ENTROPY_MIN_ALPHABET = 16  # a narrower alphabet is a pattern (ab12ab12...), not a key
# A short line is one note: a cue anywhere in it covers every token. On a longer
# line (a JSON log, a minified file) only the text around each cue is examined.
ENTROPY_WHOLE_LINE_LENGTH = 2000
ENTROPY_CUE_REACH = 256  # how far from a cue a token may start or end in a long line
# Longer runs of base64 characters are encoded data (an image, a bundle), not a
# credential; tokens inside them are never examined.
ENTROPY_MAX_TOKEN_LENGTH = 1024
ASSIGNED_VALUE_MIN_LENGTH = 8
VALUE_MAX_LENGTH = 4096
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
# Private keys, whole or cut short, however they are wrapped, escaped or cut.
# A text bears a key from the line where it first shows a private key marker
# (BEGIN or END ... PRIVATE KEY in any case, spacing or escaping) or a private
# key's own bytes: the start of a PKCS#1, PKCS#8 (RSA, EC, Ed/X25519, Ed/X448),
# SEC1, encrypted PKCS#8 or PKCS#12 body, an OpenSSH or OpenPGP secret key, a
# PEM encoded once more in base64 (Kubernetes secrets, cloud key downloads), a
# PuTTY key file, a private member of a JWK (in any order or JSON escaping), an
# XML RSA key (.NET), or OpenSSL's text dump of a key. From there on the text
# loses its markers, colon-separated hex dumps of three or more bytes, and:
# - every line right after a line of key material that holds nothing but one
#   run of 8 or more characters, or one ending in padding (however its line
#   break is escaped): a short or word-like last line goes with its key, a
#   word alone on a line or a sentence does not;
# - every other run of 16 or more base64 characters (JSON, HTML and URL escapes
#   of "/", "+" and "=" decoded, however often the backslash was escaped; "-"
#   and "_" joined, for base64url) that reads as random bytes: two of lower
#   case, upper case and digits, not a hex digest or UUID, not mostly words,
#   acronyms or numbers, judged per side of an "=", not a path or URL (mostly
#   text between its slashes), and not the data of an image data: URI.
# Text before that line is left as it is. A key body cut into pieces shorter
# than 16 characters and interrupted by other text is not recognised. Markers
# and pieces with nothing but separators between them become one marker.
PRIVATE_KEY_MIN_PIECE = 16
PRIVATE_KEY_MIN_TAIL = 8  # a shorter last line counts when it ends in padding; a word alone on a line does not
PRIVATE_KEY_MAX_WORDINESS = 0.75  # a random base64 line of 64 characters stays under 0.6
PRIVATE_KEY_PATH_TEXT = 0.6  # share of a run in text segments between slashes that makes it a path
_ENTITY_ZEROS = r"0{0,3}"  # bounded: an entity's value is decoded, and a long run of zeros is not one
_SEP = r"(?:[ \t\r\n\f\v]|\\[nrt]|\\u0020|&nbsp;|&#" + _ENTITY_ZEROS + r"32;|%20)"
_DASH = r"(?:-|&#" + _ENTITY_ZEROS + r"45;|%2[dD])"
# Separators before BEGIN are bounded: a search tries the pattern at every position.
_KEY_MARKER = (
    r"(?i:" + _DASH + r"{0,5}+" + _SEP + r"{0,4}+\b(?P<edge>BEGIN|END)" + _SEP + r"++(?:[A-Z0-9]{1,20}" + _SEP
    + r"++){0,3}?PRIVATE" + _SEP + r"++KEY(?:" + _SEP + r"++BLOCK)?+(?:" + _SEP + r"*+" + _DASH + r"{1,5}+)?+)"
)
_KEY_SIGNATURE = (
    r"MII[A-Za-z0-9+/]{3}IBAAK[BC]|IBADANBgkqhkiG9w0BAQEFAAS|AgEAMB[AM]GByqGSM49AgE|CAQAwBQYDK2V[uvwx]"
    r"|M[HI][A-Za-z]CAQEE[IB]|MIGkAgEBBD|MIHcAgEBBE|MII[A-Za-z0-9+/]{3}IBAzCC"
    r"|BgkqhkiG9w0BBQ0w|GCSqGSIb3DQEFDT|YJKoZIhvcNAQUN|b3BlbnNzaC1rZXktdjE"
    r"|UFJJVkFURSBLRVk|BSSVZBVEUgS0VZ|QUklWQVRFIEtFW"
    r"|(?<![A-Za-z0-9+/])(?:x[Q-Zc-f]|lQ)[A-Za-z0-9+/]{2}B[E-H]"
    r"|PuTTY-User-Key-File-|\\{0,8}[\"'](?:d|p|q|dp|dq|qi)\\{0,8}[\"'][ \t]*+:[ \t]*+\\{0,8}[\"'][A-Za-z0-9_-]{16}"
    r"|<(?:D|P|Q|DP|DQ|InverseQ)>[A-Za-z0-9+/]{16}|Private-Key: \(|\bprivateExponent:|(?m:\bpriv:[ \t]*+$)"
)
_KEY_TRIGGER = re.compile(_KEY_MARKER + "|" + _KEY_SIGNATURE)
_KEY_SIGNED = re.compile(_KEY_SIGNATURE)
_KEY_RUN_ESCAPE = (
    r"\\{1,8}/|\\{1,8}u00(?:2[bBfF]|3[dD])|&#" + _ENTITY_ZEROS + r"(?:4[37]|61);|&#[xX]" + _ENTITY_ZEROS
    + r"(?:2[bBfF]|3[dD]);|&(?:sol|plus|equals);|%2[bBfF]|%3[dD]"
)
# Markup, entities, percent escapes and escaped line breaks are read whole, so
# their letters (the n of \n, the br of <br>) never count as a run of text.
_KEY_ITEM = re.compile(
    r"(?P<marker>" + _KEY_MARKER + r")|(?P<hexdump>(?:[0-9a-fA-F]{2}:){3,}+[0-9a-fA-F]{0,2}+)"
    r"|(?<!\\)(?P<run>(?:[A-Za-z0-9+/=_-]|" + _KEY_RUN_ESCAPE + r")++)"
    r"|(?P<skip><[^<>]{0,20}>|&#?[A-Za-z0-9]{1,10};|%[0-9A-Fa-f]{2}|\\{1,8}[nrt]|\\{1,8}u[0-9a-fA-F]{4})"
)
_KEY_UNESCAPE = re.compile(_KEY_RUN_ESCAPE)
_UNESCAPED = {"sol": "/", "plus": "+", "equals": "="}
# Between two pieces of one key: separators, quotes, list and quote marks, and
# escaped or marked-up line breaks, but no letter or digit of any script.
_LINE_BREAK = r"\r\n|[\r\n]|(?:\\{1,8}r)?+\\{1,8}n|\\{1,8}r|(?:\\u000[dD])?+\\u000[aA]|\\u000[dD]|<br\s*+/?>|&#" + _ENTITY_ZEROS + r"1[03];|&#[xX]" + _ENTITY_ZEROS + r"[aAdD];|%0[aAdD]"
_GAP_UNIT = (
    r"[^\w\\&<%]|" + _LINE_BREAK + r"|\\{1,8}(?:[tbf\"'/]|u[0-9a-fA-F]{4})?+"
    r"|</?(?:pre|code|p|div|span)\s*+>|&(?:#[xX]?[0-9a-fA-F]{1,6}|[a-zA-Z]{2,8});|%[0-9a-fA-F]{2}|[&<%]"
)
_KEY_GAP = re.compile(r"(?:" + _GAP_UNIT + r")*+")
_HAS_LINE_BREAK = re.compile(_LINE_BREAK)
_REST_OF_LINE = re.compile(r"(?:(?!" + _LINE_BREAK + r")(?:" + _GAP_UNIT + r"))*+(?:" + _LINE_BREAK + r"|\Z)")
_SENTENCE_END = re.compile(r"[.,;:!?]")
_IMAGE_DATA = re.compile(r"data:image/[\w.+-]{1,40};base64,\Z")
_HEX_OR_UUID = re.compile(r"(?i:[0-9a-f-]+)")
_SEGMENT = re.compile(r"=++(?=[^=])")  # base64 has "=" only at its end; "A=B" is an assignment
# What makes a run read as text: words, hex digests and numbers (a commit URL, a release name).
_WORD = re.compile(r"[0-9a-f]{7,}|[0-9]{4,}|[A-Z]{1,8}(?=[A-Z][a-z]{3})|[A-Z]?[a-z]{3,}")  # acronyms: AWSKMSClient
_LOWER, _UPPER, _DIGIT = re.compile(r"[a-z]"), re.compile(r"[A-Z]"), re.compile(r"[0-9]")


def _unescaped(escape: str) -> str:
    """The character a JSON, HTML or URL escape of "/", "+" or "=" stands for."""
    lowered = escape.lower().lstrip("\\")  # nested JSON escapes the backslash of an escape again
    if lowered == "/":
        return "/"
    if lowered.startswith("&") and lowered[1:-1] in _UNESCAPED:
        return _UNESCAPED[lowered[1:-1]]
    if lowered.startswith("u"):
        return chr(int(lowered[1:], 16))
    code = lowered.strip("&#%;x")
    return chr(int(code, 16) if lowered.startswith("%") or "x" in lowered else int(code))


def _unescape_run(run: str) -> str:
    if not any(sign in run for sign in "\\&%"):
        return run
    return _KEY_UNESCAPE.sub(lambda match: _unescaped(match.group()), run)


def _random_segment(segment: str) -> bool:
    if len(segment) < PRIVATE_KEY_MIN_PIECE or _HEX_OR_UUID.fullmatch(segment):
        return False
    if sum(bool(pattern.search(segment)) for pattern in (_LOWER, _UPPER, _DIGIT)) < 2:
        return False
    return sum(len(word) for word in _WORD.findall(segment)) / len(segment) < PRIVATE_KEY_MAX_WORDINESS


def _reads_as_text(segment: str) -> bool:
    """A path segment of text: short (v1, 42), one kind of character, mostly words, or a number."""
    kinds = sum(bool(pattern.search(segment)) for pattern in (_LOWER, _UPPER, _DIGIT))
    return len(segment) <= 3 or kinds <= 1 or sum(len(word) for word in _WORD.findall(segment)) >= PRIVATE_KEY_MAX_WORDINESS * len(segment)


def _path_like(core: str) -> bool:
    """Whether most of a run with slashes is text between them (a path, a URL), which key bytes never are."""
    segments = [segment for segment in core.split("/") if segment]
    return len(segments) >= 2 and sum(len(s) for s in segments if _reads_as_text(s)) >= PRIVATE_KEY_PATH_TEXT * len(core)


def _key_piece(run: str, before: str) -> bool:
    """Whether a base64 run standing on its own can be key bytes, rather than words, a path or a digest."""
    decoded = _unescape_run(run)
    if _KEY_SIGNED.search(decoded):
        return True
    if len(decoded) < PRIVATE_KEY_MIN_PIECE or _IMAGE_DATA.search(before):
        return False
    core = decoded.rstrip("=")
    if len(core) < PRIVATE_KEY_MIN_PIECE or _path_like(core):
        return False
    return any(_random_segment(part) for part in _SEGMENT.split(core))


def _redact_private_keys(text: str, mark: Callable[[str], str]) -> str:
    """A text bearing a private key without its markers and key material; linear in *text*."""
    first = _KEY_TRIGGER.search(text)
    if first is None:
        return text
    # From the line of the first sign of a key; an END with no BEGIN before it closes a body above it.
    start = 0 if (first.group("edge") or "").upper() == "END" else text.rfind("\n", 0, first.start()) + 1
    pieces, position = [text[:start]], start
    previous_end, marked, continues = start, False, False
    for item in _KEY_ITEM.finditer(text, start):
        if item.group("skip") is not None:
            continue  # part of the gap before the next item
        run = item.group("run")
        if run is not None and not continues and len(run) < PRIVATE_KEY_MIN_PIECE:
            previous_end, marked = item.end(), False  # a word: text, whatever the gap before it
            continue
        gap_clean = _KEY_GAP.fullmatch(text, previous_end, item.start()) is not None
        if run is None:
            key = True
        elif continues and gap_clean and len(_HAS_LINE_BREAK.findall(text, previous_end, item.start())) == 1:
            tail = ((len(run) >= PRIVATE_KEY_MIN_TAIL or run.endswith("=")) and not _HEX_OR_UUID.fullmatch(run)
                    and not _SENTENCE_END.match(text, item.end()))
            key = (tail and _REST_OF_LINE.match(text, item.end()) is not None) or _key_piece(run, "")
        else:
            key = _key_piece(run, text[max(0, item.start() - 48):item.start()])
        previous_end = item.end()
        if not key:
            marked = continues = False
            continue
        if not (marked and gap_clean):  # marked: nothing but this gap since the last marker
            pieces += [text[position:item.start()], mark("private_key")]
        marked, position = True, item.end()
        continues = run is not None  # the first line after a marker stands on its own
    pieces.append(text[position:])
    return "".join(pieces)


_SECRET_WORD = (
    r"(?:api[_\-]?key|access[_\-]?key|secret(?:[_\-]?key)?|token|passw(?:or)?d|pwd|private[_\-]?key|credentials?)"
)
_ASSIGN = r"[\"']?[ \t]{0,64}[:=][ \t]{0,64}[\"']?"
# "&" belongs to a value (a password may hold one) unless a "name=" follows it,
# which starts the next parameter of a query string.
_VALUE = r"(?P<value>(?:[^\s\"'`,;<>(){}\[\]\\&]|&(?![A-Za-z0-9_.\-]{1,64}=)){1,%d})" % VALUE_MAX_LENGTH
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
# Parts of a slug or file name: up to three characters (v3, i2v, ES3), a number,
# a word with digits on one side (batch8, 720p), an acronym (IMG, GPU2), or camel
# case (nightlyRenderQueue, PyQt6, OpenGL). A group of a license key (7K3QX) or
# a long run of capitals (ABCDEFG2) is none of these.
_SLUG_SHORT_PART = re.compile(r"[a-z0-9]{1,3}|[A-Z0-9]{1,3}|[A-Z][a-z0-9]{1,2}")
_SLUG_WORD = re.compile(
    r"[0-9]+|[a-z]+[0-9]*|[0-9]+[a-z]+|[A-Z]{2,5}[0-9]*|[A-Z]?[a-z]+(?:[A-Z][a-z]+)*(?:[A-Z]{1,2})?[0-9]*"
)
# Anchored at a run start, so each run is scanned once.
_BLOB = re.compile(r"(?<![A-Za-z0-9+/=_\-])[A-Za-z0-9+/=_\-]{%d,}" % (ENTROPY_MAX_TOKEN_LENGTH + 1))
# Words that make a random-looking token nearby likely a credential. English
# cues stand apart from base64 characters, where "key" or "token" occur by chance.
# A name may end in one (GITHUB_TOKEN, DB_PASSWORD, apiKey); a plural joined to
# another word counts things (max_tokens), and nothing may follow a cue (token_count).
# Camel case counts too, but not a bare "Key": publicKey or primaryKey is no secret.
_SECRET_CUE = re.compile(
    r"(?i)(?<![A-Za-z0-9+/=])"
    r"(?:(?:api|access|secret|private)[\s_\-]?key|token|secret|passw(?:or)?d|pwd|credential|bearer)"
    r"(?![A-Za-z0-9+/=_\-])"
    r"|(?<![A-Za-z0-9+/=_\-])"
    r"(?:(?:api|access)[\s_\-]?keys|tokens|secrets|passw(?:or)?ds|credentials)"
    r"(?![A-Za-z0-9+/=_\-])"
    r"|(?-i:(?<=[a-z])(?:Token|Secret|Password)(?![a-z]))"
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

    text = _redact_private_keys(text, mark)
    for kind, pattern in _PREFIXED:
        text = pattern.sub(lambda match, kind=kind: mark(kind), text)
    text = _replace_group(_QUERY_PARAM, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_ASSIGNED, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_ASSIGNED_CAMEL, text, "assigned_secret", mark, check=_plausible_assigned_value)
    text = _replace_group(_KEY_LABEL, text, "labelled_key", mark, check=_labelled_value)
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


def _labelled_value(value: str) -> bool:
    """A value right after "key": random-looking, and it may be long."""
    return _high_entropy(value, max_length=VALUE_MAX_LENGTH)


def _high_entropy(token: str, *, max_length: int = ENTROPY_MAX_TOKEN_LENGTH) -> bool:
    token = token.rstrip("=")
    if not ENTROPY_MIN_LENGTH // 2 <= len(token) <= max_length or _HEX.fullmatch(token):
        return False
    if not re.search(r"[A-Za-z]", token) or not re.search(r"[0-9]", token) or _slug(token):
        return False
    alphabet = sum(size for pattern, size in _CHARACTER_CLASSES if re.search(pattern, token))
    distinct = len(set(token))
    # A long random token shows nearly all of its alphabet, which may be narrower
    # than its character classes: base32 is A-Z and 2-7, not every digit.
    if len(token) >= 2 * alphabet and ENTROPY_MIN_ALPHABET <= distinct < alphabet:
        alphabet = distinct
    shortfall = _random_entropy(len(token), alphabet) - _shannon_bits(token)
    return shortfall * math.sqrt(len(token)) <= ENTROPY_MAX_SHORTFALL


def _slug(token: str) -> bool:
    """Words and numbers joined by -, _, or +, like a file name or a branch."""
    parts = [part for part in re.split(r"[-_+]+", token) if part]
    return len(parts) > 1 and all(_SLUG_SHORT_PART.fullmatch(part) or _SLUG_WORD.fullmatch(part) for part in parts)


def _shannon_bits(token: str) -> float:
    total = len(token)
    return -sum(n / total * math.log2(n / total) for n in Counter(token).values())


@functools.lru_cache(maxsize=16384)
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


def redact_value(value: Any, *, skip_keys: frozenset[str] = frozenset(),
                 secret: bool = False) -> tuple[Any, Counter[str]]:
    """Redact every string inside ``value``, except under ``skip_keys`` of a mapping.

    A string under a key named like a secret ("password", "AWS_SECRET_ACCESS_KEY",
    "accessToken"), directly or in a list, is redacted whole when it looks like a
    credential, since its own text would not say so. A string that holds a JSON
    object or array is decoded and filtered as a value too, however many times it
    was encoded, and written back as JSON when anything in it was redacted.
    """
    counts: Counter[str] = Counter()
    return _redact_tree(value, skip_keys, secret, counts), counts


JSON_TEXT_MAX_LENGTH = 1 << 20


def _redact_tree(root: Any, skip_keys: frozenset[str], secret: bool, counts: Counter[str]) -> Any:
    """*root* with its strings redacted, walked with a stack of its own, so no depth exhausts Python's."""
    holder: list[Any] = [None]
    pending: list[tuple[Any, bool, Any, Any]] = [(root, secret, holder, 0)]  # value, secret, parent, key
    while pending:
        value, under_secret, parent, key = pending.pop()
        if isinstance(value, str):
            parent[key] = _redact_text(value, under_secret, counts)
        elif isinstance(value, list):
            items: list[Any] = [None] * len(value)
            parent[key] = items
            pending.extend((item, under_secret, items, index) for index, item in enumerate(value))
        elif isinstance(value, dict):
            mapping: dict[Any, Any] = {}
            parent[key] = mapping
            for name, item in value.items():
                mapping[name] = item  # keeps the order; replaced below unless skipped
                if name not in skip_keys:
                    named = isinstance(name, str) and _SECRET_NAME.search(name) is not None
                    pending.append((item, named and not isinstance(item, dict), mapping, name))
        else:
            parent[key] = value
    return holder[0]


def _redact_text(value: str, secret: bool, counts: Counter[str]) -> str:
    """One string of a value: whole when it is a credential under a secret name, else filtered, JSON included."""
    if secret and _plausible_assigned_value(value):
        counts["assigned_secret"] += 1
        return MARKER.format(kind="assigned_secret")
    result = redact(value)
    counts.update(result.counts)
    decoded = _json_container(result.text)
    if decoded is _TOO_DEEP:
        # JSON nested deeper than the json module parses (about 1,000 levels
        # before Python 3.12): what a secret name marks inside cannot be told.
        if _SECRET_WORD_ANYWHERE.search(result.text):
            counts["assigned_secret"] += 1
            return MARKER.format(kind="assigned_secret")
        return result.text
    if decoded is None:
        return result.text
    # Recursion here follows levels of encoding only, and each one adds escapes.
    found: Counter[str] = Counter()
    redacted = _redact_tree(decoded, frozenset(), False, found)
    if not found:
        return result.text
    counts.update(found)
    try:
        return json.dumps(redacted, ensure_ascii=False)
    except (ValueError, RecursionError):
        return MARKER.format(kind="assigned_secret")


_TOO_DEEP = object()
_SECRET_WORD_ANYWHERE = re.compile(r"(?i)" + _SECRET_WORD)


def _json_container(text: str) -> Any:
    """The object or array that *text* holds as JSON, None, or _TOO_DEEP when it nests too deep to parse."""
    stripped = text.strip()
    if stripped[:1] not in ("{", "[") or len(stripped) > JSON_TEXT_MAX_LENGTH:
        return None
    try:
        decoded = json.loads(stripped)
    except RecursionError:
        return _TOO_DEEP
    except ValueError:
        return None
    return decoded if isinstance(decoded, (dict, list)) else None
