-- 0015: at most one active value per fact slot (W8).
--
-- A slot is (domain, project, task, slot_key); project and task are the JSON
-- pair in group_id, and a missing one counts as ''. Values recorded under other
-- spellings of a name before the identity registry (FR-14) are other slots
-- here; replacing the value settles them (memory._record_fact).
--
-- A database that already holds two active values of one slot keeps the
-- newest. The older ones retire as a replacement would retire them, with a
-- transaction and a mutation log entry that say why.

-- One pass over the active single-value facts, ranked newest first within
-- each slot; a fact without group_id is in the widest scope, as in
-- memory._active_facts.
CREATE TEMP TABLE facts_0015_older AS
SELECT id, domain FROM (
    SELECT id, domain, ROW_NUMBER() OVER (
        PARTITION BY domain,
                     ifnull(json_extract(group_id, '$[0]'), ''),
                     ifnull(json_extract(group_id, '$[1]'), ''),
                     slot_key
        ORDER BY recorded_at_utc DESC, row_id DESC) AS newer_values
    FROM facts
    WHERE status = 'active' AND expired_at_utc IS NULL AND cardinality = 'single'
      AND (group_id IS NULL OR json_valid(group_id)))
WHERE newer_values > 1;

INSERT INTO fact_transactions(domain, operation, actor, recorded_at_utc, reason)
SELECT DISTINCT domain, 'retire', 'chronicle', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
       'migration 0015: one active value per slot'
FROM facts_0015_older;

CREATE TEMP TABLE facts_0015_tx AS
SELECT domain, max(id) AS tx_id FROM fact_transactions
WHERE reason = 'migration 0015: one active value per slot'
GROUP BY domain;

UPDATE facts
SET status = 'retired',
    expired_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    valid_to_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    expired_tx_id = (SELECT tx.tx_id FROM facts_0015_tx AS tx WHERE tx.domain = facts.domain)
WHERE id IN (SELECT id FROM facts_0015_older);

INSERT INTO fact_mutation_log(domain, action, fact_id, previous_fact_id, tx_id, reason, recorded_at_utc)
SELECT older.domain, 'retire', older.id, NULL, tx.tx_id,
       'a newer value of the slot was active', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM facts_0015_older AS older JOIN facts_0015_tx AS tx ON tx.domain = older.domain;

DROP TABLE facts_0015_older;
DROP TABLE facts_0015_tx;

CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_one_active_slot ON facts(
    domain,
    ifnull(json_extract(group_id, '$[0]'), ''),
    ifnull(json_extract(group_id, '$[1]'), ''),
    slot_key)
WHERE status = 'active' AND expired_at_utc IS NULL AND cardinality = 'single'
  AND (group_id IS NULL OR json_valid(group_id));
