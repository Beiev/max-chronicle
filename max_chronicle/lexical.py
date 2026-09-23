"""Lexical query building shared by event and fact search.

Recall used to demand every word of a question, function words included, so a
question in natural phrasing ("почему отказались от X?") matched nothing. It
also split words at any letter outside [A-Za-zА-Яа-я] (ё, і, ї, є, ґ). Here
queries keep every Unicode letter, fold ё into е (the index is folded the same
way by migration 0011), and drop function words. A strict query still needs
every remaining term; a relaxed one matches word stems and keeps only evidence
that covers most of them.
"""

from __future__ import annotations

import math
import re
import unicodedata

# Letters, digits, and the joiners that keep "gpt-image-2" or "v0.11.0" whole.
_TERM = re.compile(r"[\w$+.\-]+")
_EDGE_JOINERS = "$+.-_"
# A word as FTS5's unicode61 tokenizer sees one: letters and digits only, so
# "_", "-", and "." separate words ("gpt_image_2" is gpt, image, 2).
_INDEX_WORD = re.compile(r"[^\W_]+")
RELAXED_STEM_MIN_LENGTH = 5  # shorter words must match whole
RELAXED_SUFFIX_LENGTH = 2  # letters an inflection may change at the end
RELAXED_STEM_FLOOR = 4
RELAXED_MIN_COVERAGE = 2 / 3  # share of query terms relaxed evidence must contain

# Negations (not, no, не, нет, ни) and "may" stay searchable: they change what a
# question asks, and May is a month.
STOPWORDS = frozenset(
    """
    a an the and or but if of to in on at for from by with about as into onto than then so
    is are was were be been being do does did done have has had it its this that these those
    there here what which who whom whose when where why how we you they he she i me my our
    your their us them can could should would will shall might must any all some
    up out over after before again just also only very still now yet
    и в во что он на я с со как а то все она так его но да ты к у же вы за бы по только
    ее мне было вот от меня еще о об из ему теперь когда даже ну ли если уже или быть
    был была были будет него до вас нибудь опять уж вам ведь там потом себя ничего ей может
    они тут где есть надо ней для мы тебя их чем сам чтоб без чего раз тоже себе под ж тогда
    кто это этот этого эта эти этой этом эту того тем том тот какой какая какие каким какую каких
    каком какого как почему зачем куда сколько сейчас можно нужно при после над через про
    всего всех них нас им ним нее
    і й та з із зі що це ці цей ця чи для від як коли де чому який яка які якою яким якому
    хто його їх ми ви вони він вона але або ще вже тут там було був була були є бути мене
    мені нам вам про по за до на у в
    """.split()
)


def fold(text: str) -> str:
    """Normalize, case-fold, and merge ё into е, as the full-text indexes store text."""
    return unicodedata.normalize("NFC", text).casefold().replace("ё", "е")


def query_terms(query: str) -> list[str]:
    """Content terms of a query, folded, without function words or repeats.

    A query made only of function words ("что это?") keeps them all instead of
    matching nothing.
    """
    words: list[str] = []
    for raw in _TERM.findall(fold(query)):
        word = raw.strip(_EDGE_JOINERS)
        if len(word) >= 2 and word not in words:
            words.append(word)
    return [word for word in words if word not in STOPWORDS] or words


def index_words(text: str) -> list[str]:
    """The words of ``text`` as the full-text index holds them.

    Mirrors unicode61 with its default remove_diacritics=1: accents on Latin
    letters are dropped (café is cafe), other scripts keep theirs (й is not и).
    """
    kept: list[str] = []
    latin_base = False
    for char in unicodedata.normalize("NFD", fold(text)):
        if not unicodedata.combining(char):
            latin_base = char.isascii()
        elif latin_base:
            continue
        kept.append(char)
    return _INDEX_WORD.findall(unicodedata.normalize("NFC", "".join(kept)))


def _phrase(term: str) -> str:
    return '"' + term.replace('"', '""') + '"'


def _stem(term: str) -> str | None:
    """The prefix a relaxed query matches, or None when the term must match whole."""
    if not term.isalpha() or len(term) < RELAXED_STEM_MIN_LENGTH:
        return None
    return term[: max(RELAXED_STEM_FLOOR, len(term) - RELAXED_SUFFIX_LENGTH)]


def strict_query(terms: list[str]) -> str:
    """FTS5 MATCH text requiring every term."""
    return " AND ".join(_phrase(term) for term in terms) if terms else '""'


def relaxed_query(terms: list[str]) -> str:
    """FTS5 MATCH text accepting any term, long words by their stem."""
    parts = [_phrase(stem) + "*" if (stem := _stem(term)) else _phrase(term) for term in terms]
    return " OR ".join(parts) if parts else '""'


def covers(terms: list[str], text: str) -> bool:
    """Whether ``text`` holds enough of ``terms`` to count as relaxed evidence.

    A term is found the way the index would find it: a stem as a word prefix,
    any other term as its run of words ("gpt-image-2" as gpt image 2).
    """
    if not terms:
        return False
    words = index_words(text)
    joined = " " + " ".join(words) + " "
    found = 0
    for term in terms:
        term_words = index_words(term)
        stem = _stem(term_words[0]) if len(term_words) == 1 else None
        if stem:
            found += any(word.startswith(stem) for word in words)
        elif term_words:
            found += " " + " ".join(term_words) + " " in joined
    return found >= math.ceil(len(terms) * RELAXED_MIN_COVERAGE)
