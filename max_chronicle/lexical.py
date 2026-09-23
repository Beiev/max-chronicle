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

# Letters, digits, and the joiners that keep "gpt-image-2" or "v0.11.0" whole.
_TERM = re.compile(r"[\w$+.\-]+")
_EDGE_JOINERS = "$+.-_"
RELAXED_STEM_MIN_LENGTH = 5  # shorter words must match whole
RELAXED_SUFFIX_LENGTH = 2  # letters an inflection may change at the end
RELAXED_STEM_FLOOR = 4
RELAXED_MIN_COVERAGE = 2 / 3  # share of query terms relaxed evidence must contain

STOPWORDS = frozenset(
    """
    a an the and or but if of to in on at for from by with about as into onto than then so
    is are was were be been being do does did done have has had it its this that these those
    there here what which who whom whose when where why how we you they he she i me my our
    your their us them can could should would will shall may might must not no any all some
    up out over after before again just also only very still now yet
    и в во не что он на я с со как а то все она так его но да ты к у же вы за бы по только
    ее мне было вот от меня еще нет о об из ему теперь когда даже ну ли если уже или ни быть
    был была были будет него до вас нибудь опять уж вам ведь там потом себя ничего ей может
    они тут где есть надо ней для мы тебя их чем сам чтоб без чего раз тоже себе под ж тогда
    кто это этот этого эта эти этой этом эту того тем том тот какой какая какие каким какую каких
    каком какого как почему зачем куда сколько сейчас можно нужно при после над через про
    всего всех них нас им ним нее
    і й та з із зі що це ці цей ця чи для від як коли де чому який яка які якою яким якому
    хто його їх ми ви вони він вона але або ще вже тут там було був була були є бути мене
    мені нам вам про по за до на у в не
    """.split()
)


def fold(text: str) -> str:
    """Case-fold and merge ё into е, as the full-text indexes store text."""
    return text.casefold().replace("ё", "е")


def query_terms(query: str) -> list[str]:
    """Content terms of a query, folded, without function words or repeats."""
    terms: list[str] = []
    for raw in _TERM.findall(fold(query)):
        term = raw.strip(_EDGE_JOINERS)
        if len(term) >= 2 and term not in STOPWORDS and term not in terms:
            terms.append(term)
    return terms


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
    """Whether ``text`` holds enough of ``terms`` to count as relaxed evidence."""
    if not terms:
        return False
    folded = fold(text)
    found = 0
    for term in terms:
        stem = _stem(term)
        pattern = rf"(?<!\w){re.escape(stem)}" if stem else rf"(?<!\w){re.escape(term)}(?!\w)"
        found += re.search(pattern, folded) is not None
    return found >= math.ceil(len(terms) * RELAXED_MIN_COVERAGE)
