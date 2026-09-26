"""One registry for the names of agents, projects, tasks and domains (FR-14).

The same thing gets written in several ways: one project as ``status``,
``chronicle`` and ``max-chronicle``, one task as ``Launch_plan`` and
``launch_plan``, one agent as ``Codex`` and ``codex``. The manifest names each
project, domain and agent once and lists its other spellings, and ``fold``
makes case, spaces and underscores irrelevant, so a filter by any spelling
finds all of them.

Stored history is not rewritten. A filter expands a name to every spelling of
it; a write stores the canonical name and keeps the one it was given beside
it (``actor_raw``, ``project_raw``, ``domain_raw``).
"""

from __future__ import annotations

import string
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

UNKNOWN_AGENT = "mcp"

# Agents every installation meets. A manifest [[agents]] entry with the same id
# replaces its default. After exact aliases, rules are tried in this order, so
# ``opencode-glm5.2`` is GLM's before it is opencode's. ZCode runs GLM.
DEFAULT_AGENTS: tuple[dict[str, Any], ...] = (
    {"id": "claude", "prefixes": ["claude"], "aliases": ["operator"]},
    {"id": "glm", "prefixes": ["zcode"], "contains": ["glm"]},
    {"id": "deepseek", "contains": ["deepseek"]},
    {"id": "codex", "prefixes": ["codex"]},
    {"id": "gemini", "prefixes": ["gemini"]},
    {"id": "opencode", "prefixes": ["opencode"]},
    {"id": "transcript-analyst", "prefixes": ["transcript-analyst"]},
)

_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def fold(name: str) -> str:
    """*name* with case, spaces and underscores made irrelevant.

    Equal to ``fold_sql`` in SQLite, whose lower() changes ASCII letters only,
    so this changes only those too.
    """
    return name.strip(" ").replace(" ", "-").replace("_", "-").translate(_ASCII_LOWER)


def fold_sql(expression: str) -> str:
    """``fold`` of an SQL *expression*."""
    return f"lower(replace(replace(trim({expression}), ' ', '-'), '_', '-'))"


def _list(entry: dict[str, Any], key: str, kind: str, name: str) -> list[str]:
    """A list-valued key of a registry entry; a bare string is a mistake, not a list of letters."""
    value = entry.get(key) or []
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise ValueError(f"[[{kind}]] {name!r}: {key} must be a list of names")
    return [str(item) for item in value if str(item).strip()]


def _names(entry: dict[str, Any], kind: str) -> tuple[str, list[str]]:
    name = str(entry.get("id") or "").strip()
    if not name:
        raise ValueError(f"Every [[{kind}]] entry in the manifest needs an id")
    return name, [fold(name)] + [fold(alias) for alias in _list(entry, "aliases", kind, name)]


@dataclass(frozen=True, eq=False)
class Names:
    """The canonical names of one kind, and every declared spelling of each."""

    canonical: dict[str, str] = field(default_factory=dict)
    spellings: dict[str, tuple[str, ...]] = field(default_factory=dict)

    @classmethod
    def build(cls, entries: Iterable[dict[str, Any]], kind: str) -> Names:
        canonical: dict[str, str] = {}
        spellings: dict[str, tuple[str, ...]] = {}
        for entry in entries:
            name, forms = _names(entry, kind)
            for form in forms:
                owner = canonical.setdefault(form, name)
                if owner != name:
                    raise ValueError(f"The {kind} spelling {form!r} belongs to both {owner!r} and {name!r}")
            # Two entries with one id add up: neither loses its spellings.
            spellings[name] = tuple(dict.fromkeys(spellings.get(name, ()) + tuple(forms)))
        return cls(canonical, spellings)

    def resolve(self, name: str | None) -> str | None:
        """The canonical name of *name*; an undeclared name as it was given."""
        if name is None or not name.strip():
            return None
        return self.canonical.get(fold(name), name.strip())

    def forms(self, name: str) -> tuple[str, ...]:
        """Every folded spelling of the identity *name* stands for."""
        resolved = self.resolve(name) or ""
        return self.spellings.get(resolved, (fold(resolved),))


class DomainMap(dict):
    """``manifest["domain_map"]`` that also answers for a domain's other spellings (FR-14)."""

    def __init__(self, domains: dict[str, Any], names: Names) -> None:
        super().__init__(domains)
        self._names = names

    def _key(self, key: Any) -> Any:
        if isinstance(key, str) and not dict.__contains__(self, key):
            return self._names.resolve(key) or key
        return key

    def __getitem__(self, key: Any) -> Any:
        return super().__getitem__(self._key(key))

    def __contains__(self, key: object) -> bool:
        return super().__contains__(self._key(key))

    def get(self, key: Any, default: Any = None) -> Any:
        return super().get(self._key(key), default)


@dataclass(frozen=True)
class AgentRule:
    agent: str
    prefixes: tuple[str, ...] = ()
    contains: tuple[str, ...] = ()

    def matches(self, folded: str) -> bool:
        return any(folded.startswith(prefix) for prefix in self.prefixes) or any(
            part in folded for part in self.contains
        )


@dataclass(frozen=True)
class Scope:
    """What a query is limited to, with every spelling of each name (FR-14).

    ``domain`` and ``project`` are canonical; ``task_id`` is as given, since
    tasks have no declared names and match by ``fold`` alone.
    """

    domain: str | None = None
    project: str | None = None
    task_id: str | None = None
    domains: tuple[str, ...] = ()
    projects: tuple[str, ...] = ()

    @property
    def key(self) -> list[str | None]:
        """The scope a cursor is bound to: the same for every spelling of it."""
        return [fold(name) if name else None for name in (self.domain, self.project, self.task_id)]

    def where(
        self,
        *,
        domain: str,
        project: str,
        task: str,
        project_or_unset: bool = False,
        task_or_unset: bool = False,
    ) -> tuple[str, tuple[str, ...]]:
        """SQL conditions that keep rows in this scope, and their parameters.

        *domain*, *project* and *task* are the SQL expressions holding those
        names. With ``project_or_unset`` or ``task_or_unset``, a row without
        that name also stays in scope.
        """
        clauses: list[str] = []
        params: list[str] = []
        for expression, forms, or_unset in (
            (domain, self.domains, False),
            (project, self.projects, project_or_unset),
        ):
            if forms:
                condition = f"{fold_sql(expression)} IN ({', '.join('?' for _ in forms)})"
                clauses.append(f"({condition} OR {expression} IS NULL)" if or_unset else condition)
                params.extend(forms)
        if self.task_id:
            condition = f"{fold_sql(task)} = ?"
            clauses.append(f"({condition} OR {task} IS NULL)" if task_or_unset else condition)
            params.append(fold(self.task_id))
        return " AND ".join(clauses) or "1", tuple(params)


@dataclass(frozen=True, eq=False)
class Registry:
    """Canonical names for agents, projects and domains, from the manifest."""

    projects: Names = field(default_factory=Names)
    domains: Names = field(default_factory=Names)
    agent_aliases: dict[str, str] = field(default_factory=dict)
    agent_rules: tuple[AgentRule, ...] = ()

    @classmethod
    def from_manifest(cls, manifest: dict[str, Any] | None) -> Registry:
        """The registry declared by *manifest* ([[projects]], [[domains]], [[agents]])."""
        manifest = manifest or {}
        agents: dict[str, dict[str, list[str]]] = {}
        declared: set[str] = set()
        for from_manifest, entries in ((False, DEFAULT_AGENTS), (True, manifest.get("agents", []))):
            for entry in entries:
                name, _ = _names(entry, "agents")
                fields = {key: _list(entry, key, "agents", name) for key in ("aliases", "prefixes", "contains")}
                if from_manifest and name not in declared:
                    agents.pop(name, None)  # the manifest's first entry for an id replaces the built-in one
                    declared.add(name)
                merged = agents.setdefault(name, {"aliases": [], "prefixes": [], "contains": []})
                for key, values in fields.items():
                    merged[key] += values  # more entries with the id add up
        aliases: dict[str, str] = {}
        rules: list[AgentRule] = []
        for name, entry in agents.items():
            for form in [fold(name)] + [fold(alias) for alias in entry["aliases"]]:
                owner = aliases.setdefault(form, name)
                if owner != name:
                    raise ValueError(f"The agents spelling {form!r} belongs to both {owner!r} and {name!r}")
            rule = AgentRule(name, tuple(fold(item) for item in entry["prefixes"]),
                             tuple(fold(item) for item in entry["contains"]))
            if rule.prefixes or rule.contains:
                rules.append(rule)
        return cls(
            Names.build(manifest.get("projects", []), "projects"),
            Names.build(manifest.get("domains", []), "domains"),
            aliases,
            tuple(rules),
        )

    def project(self, name: str | None) -> str | None:
        return self.projects.resolve(name)

    def domain(self, name: str | None) -> str | None:
        return self.domains.resolve(name)

    def agent(self, name: str | None) -> str:
        """The canonical agent for *name*; an undeclared one folded."""
        if name is None or not str(name).strip():
            return UNKNOWN_AGENT
        folded = fold(str(name))
        if folded in self.agent_aliases:
            return self.agent_aliases[folded]
        for rule in self.agent_rules:
            if rule.matches(folded):
                return rule.agent
        return folded

    def scope(
        self, *, domain: str | None = None, project: str | None = None, task_id: str | None = None
    ) -> Scope:
        """The scope these names describe, whichever of their spellings was given."""
        domain = self.domain(domain)
        project = self.project(project)
        task_id = task_id.strip() if task_id and task_id.strip() else None
        return Scope(
            domain,
            project,
            task_id,
            self.domains.forms(domain) if domain else (),
            self.projects.forms(project) if project else (),
        )

    def project_sql(self, expression: str) -> tuple[str, tuple[str, ...]]:
        """SQL naming one project for every spelling *expression* may hold, and its parameters."""
        folded = fold_sql(expression)
        pairs = [
            (form, fold(name))
            for name, forms in self.projects.spellings.items()
            for form in forms
            if form != fold(name)
        ]
        if not pairs:
            return folded, ()
        whens = " ".join("WHEN ? THEN ?" for _ in pairs)
        return f"CASE {folded} {whens} ELSE {folded} END", tuple(value for pair in pairs for value in pair)

    def canonical_entry(self, entry: dict[str, Any]) -> None:
        """Give a write canonical names, keeping any other spelling beside them."""
        for key, raw_key, resolve in (
            ("agent", "actor_raw", self.agent),
            ("project", "project_raw", self.project),
            ("domain", "domain_raw", self.domain),
        ):
            raw = entry.get(key)
            if not isinstance(raw, str) or not raw.strip():
                continue
            canonical = resolve(raw)
            if canonical != raw:
                entry[key] = canonical
                entry[raw_key] = raw
