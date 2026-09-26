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

CREATE TEMP TABLE facts_0015_older AS
SELECT older.id, older.domain
FROM facts AS older
WHERE older.status = 'active' AND older.expired_at_utc IS NULL AND older.cardinality = 'single'
  AND json_valid(older.group_id)
  AND EXISTS (
      SELECT 1 FROM facts AS newer
      WHERE newer.status = 'active' AND newer.expired_at_utc IS NULL AND newer.cardinality = 'single'
        AND json_valid(newer.group_id)
        AND newer.domain = older.domain AND newer.slot_key = older.slot_key
        AND ifnull(json_extract(newer.group_id, '$[0]'), '') = ifnull(json_extract(older.group_id, '$[0]'), '')
        AND ifnull(json_extract(newer.group_id, '$[1]'), '') = ifnull(json_extract(older.group_id, '$[1]'), '')
        AND (newer.recorded_at_utc, newer.row_id) > (older.recorded_at_utc, older.row_id));

INSERT INTO fact_transactions(domain, operation, actor, recorded_at_utc, reason)
SELECT DISTINCT domain, 'retire', 'chronicle', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
       'migration 0015: one active value per slot'
FROM facts_0015_older;

UPDATE facts
SET status = 'retired',
    expired_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    valid_to_utc = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
    expired_tx_id = (SELECT max(tx.id) FROM fact_transactions AS tx
                     WHERE tx.domain = facts.domain AND tx.reason = 'migration 0015: one active value per slot')
WHERE id IN (SELECT id FROM facts_0015_older);

INSERT INTO fact_mutation_log(domain, action, fact_id, previous_fact_id, tx_id, reason, recorded_at_utc)
SELECT older.domain, 'retire', older.id, NULL,
       (SELECT max(tx.id) FROM fact_transactions AS tx
        WHERE tx.domain = older.domain AND tx.reason = 'migration 0015: one active value per slot'),
       'a newer value of the slot was active', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM facts_0015_older AS older;

DROP TABLE facts_0015_older;

CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_one_active_slot ON facts(
    domain,
    ifnull(json_extract(group_id, '$[0]'), ''),
    ifnull(json_extract(group_id, '$[1]'), ''),
    slot_key)
WHERE status = 'active' AND expired_at_utc IS NULL AND cardinality = 'single' AND json_valid(group_id);
