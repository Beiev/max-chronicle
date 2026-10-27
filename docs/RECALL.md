# Recall

How `query_memory` finds and ranks memory, what it says about its own confidence,
what never gets stored, and how to measure recall on your own corpus.

## Channels and ranking

- **Lexical:** SQLite FTS5/BM25 over event text and why, and over explicit
  current fact values. Queries drop function words (English, Russian,
  Ukrainian) but keep negations, keep every Unicode letter, and fold ё into е on
  both sides. Evidence must contain every remaining term; when no event or fact
  does, recall retries with word stems, keeps only events holding two thirds of
  the terms, and returns `relaxed: true` because that match is weaker.
- **Vector:** optional local index built through Ollama. The default model is
  `qwen3-embedding:0.6b` (multilingual; `ollama pull qwen3-embedding:0.6b`).
  `CHRONICLE_EMBED_MODEL` selects another; `nomic-embed-text` keeps an index
  built before 0.12 usable. Every vector is stored under its model, so a new
  model is backfilled beside the old index and recall reads only the active one.
  Queries carry the model's retrieval instruction, and an event longer than the
  model's context is embedded from its start instead of failing. Scope and
  visibility are checked before ranking. `vector_coverage` discloses gaps, and
  an active model with no index at all degrades the channel
  (`embedding_index_empty`). `chronicle embed-backfill` fills the active index;
  `chronicle embed-prune --model KEY` counts another model's vectors, and with
  `--apply` deletes them (never the active model's).
  If native scheduling is enabled, daily capture also retries up to 10 missing
  or incompatible embeddings per run. Disabling event embeddings disables this repair.
- **Recency:** breaks ties between equally relevant candidates; it never supplies
  or promotes unrelated answers.
- **Confidence:** `no_confident_match: true`, with a `hint`, when no result holds
  every query term or reaches the model's confident similarity (`0.6` for
  `qwen3-embedding:0.6b`; none for `nomic-embed-text`, whose similarity does not
  tell related from unrelated text). The results are still returned as leads.
  `CHRONICLE_VECTOR_CONFIDENT_SIMILARITY` overrides the floor.
- **Threshold:** a candidate found only by the vector channel needs a cosine
  similarity of at least the model's floor: `0.5` for Qwen3, `0.65` for
  `nomic-embed-text`. `CHRONICLE_VECTOR_MIN_SIMILARITY` overrides it. Evaluate
  it on your own corpus with `chronicle eval`; cosine and reciprocal-rank scores
  are not confidence values.

## How much stands behind a hit

Each recalled event carries `trust`, read from all of its visible observations:
- `assumption`: its author marked the fact it set as an assumption; nothing
  attached outranks that;
- `evidence`: a source file was archived when it was recorded. The file backs
  the record; it does not prove the text, and the label stays if the archive is
  later purged;
- `confirmed`: more than one agent (by canonical name) recorded the same text;
- `assertion`: one agent's word, with nothing archived.

A recalled note carries `trust: note`: a file someone keeps on purpose, whose
author and history are the file's own.

A mistaken or poisoned record most often looks like the last. `trust` ranks
nothing; it tells the reader what to check before acting on a hit.

## Secrets

A content-based filter replaces likely credentials with
`[REDACTED:<kind>]` before an event (with its checkpoint and fact), a UTF-8
evidence file, or a generated text artifact (daybook, commit summary, audit
report) is stored. It catches known key prefixes, values assigned to secret
names, labelled keys (`key: <random>`), bearer and basic credentials, URL
passwords, private keys, and high-entropy tokens near a word such as
token, password, secret, or API key (anywhere on a short line, within 256
characters on a long one). Hex digests, UUIDs, and pieces of long base64 runs
(encoded images) never count as high-entropy tokens.
Private keys are found by their BEGIN or END line (in any case, spacing or
escaping) or by their own bytes: PKCS#1, PKCS#8, SEC1, encrypted PKCS#8 and
PKCS#12 bodies, OpenSSH and OpenPGP secret keys, a PEM encoded again in base64
(Kubernetes secrets, cloud key downloads), X25519, Ed448 and X448 keys,
PuTTY key files, JWK private members (in any order, quoting or escaping),
.NET XML RSA keys, and `openssl ... -text` dumps. From that line on, the
markers go, colon-separated hex dumps go, every line that continues a key
goes (however its line breaks are escaped), and so does every run of 16 or
more base64 characters that reads as random bytes; words, paths, links,
assignments, hex digests, UUIDs and image data stay, and so does everything
before the key. A key body cut into pieces shorter than 16 characters and
interrupted by other text is not recognised, and a random-looking
identifier after a key (an SSH fingerprint, a random URL id) may be
removed with it.
Identifiers such as `request_id` and paths are left as given, the source file is
never modified, and binary evidence is archived unchanged. `query_context`
filters what it reads from status files. Not filtered yet: snapshot excerpts and
legacy imports. Time is linear in the
input, so a hostile or huge text cannot stall the write path.

## Measuring recall

`chronicle eval --golden questions.jsonl` asks each question through recall and
scores the ranking: hit@1/5/10, recall@5 and MRR@10 on answerable questions,
abstention on questions memory cannot answer, stale-over-current ordering on
knowledge updates, and latency percentiles, overall and per category and language.

```json
{"id": "why-sqlite", "query": "why did we pick SQLite", "category": "rationale", "lang": "en", "expected": ["event:<id>"], "scope": {"project": "demo"}}
{"id": "db-now", "query": "current database", "category": "knowledge_update", "expected": ["event:<new>"], "stale": ["event:<old>"]}
{"id": "unknown", "query": "office wifi password", "category": "abstention", "expected": []}
{"id": "backup-rotation", "query": "how often do backups rotate", "category": "fact", "expected": ["note:~/notes/backups.md"]}
```

A case expects events (`event:<id>`) or indexed notes (`note:<path>`), never both.
Note cases are scored on the `notes` of the answer and ask recall for notes;
event cases leave notes out, so their scores stay comparable across versions.
The report adds a breakdown per surface.

Categories follow LongMemEval: `fact`, `rationale`, `knowledge_update`, `temporal`,
`handoff`, `abstention`. `--json` prints the full report, `--out` saves it, and
`--fail-under hit@5=0.6` exits 1 below a floor. The test suite runs a synthetic
bilingual set (`tests/fixtures/eval/`) and pins which cases lexical recall passes.

