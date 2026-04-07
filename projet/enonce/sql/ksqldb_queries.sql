-- ksqlDB Queries
-- On s'assure de traiter les données depuis le début des topics
SET 'auto.offset.reset'='earliest';

-- =============================================================================
-- TODO 1 : STREAM sur vote_events_valid
-- =============================================================================
CREATE STREAM IF NOT EXISTS vote_events_valid_stream (
  vote_id VARCHAR,
  election_id VARCHAR,
  event_time VARCHAR,
  city_code VARCHAR,
  city_name VARCHAR,
  department_code VARCHAR,
  region_code VARCHAR,
  polling_station_id VARCHAR,
  candidate_id VARCHAR,
  candidate_name VARCHAR,
  candidate_party VARCHAR,
  candidate_block VARCHAR,
  channel VARCHAR,
  signature_ok BOOLEAN,
  voter_hash VARCHAR,
  ingestion_ts VARCHAR
) WITH (
  KAFKA_TOPIC='vote_events_valid',
  VALUE_FORMAT='JSON'
);

-- =============================================================================
-- TODO 2 : STREAM sur vote_events_rejected
-- =============================================================================
CREATE STREAM IF NOT EXISTS vote_events_rejected_stream (
  vote_id VARCHAR,
  election_id VARCHAR,
  event_time VARCHAR,
  city_code VARCHAR,
  city_name VARCHAR,
  department_code VARCHAR,
  region_code VARCHAR,
  polling_station_id VARCHAR,
  candidate_id VARCHAR,
  candidate_name VARCHAR,
  candidate_party VARCHAR,
  candidate_block VARCHAR,
  channel VARCHAR,
  signature_ok BOOLEAN,
  voter_hash VARCHAR,
  ingestion_ts VARCHAR,
  error_reason VARCHAR
) WITH (
  KAFKA_TOPIC='vote_events_rejected',
  VALUE_FORMAT='JSON'
);

-- =============================================================================
-- TODO 3 : TABLE comptage par candidat (flux VALIDE)
-- =============================================================================
CREATE TABLE IF NOT EXISTS vote_count_by_candidate AS
SELECT
  candidate_id,
  AS_VALUE(candidate_id) AS candidate_id_v,
  COUNT(*) AS total_votes
FROM vote_events_valid_stream
GROUP BY candidate_id
EMIT CHANGES;

-- =============================================================================
-- TODO 4 : TABLE fenêtre 1 minute par ville + candidat
-- =============================================================================
CREATE TABLE IF NOT EXISTS vote_count_by_city_minute
WITH ( KEY_FORMAT='JSON' ) AS
SELECT
  city_code,
  candidate_id,
  AS_VALUE(city_code) AS city_code_v,
  AS_VALUE(candidate_id) AS candidate_id_v,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  COUNT(*) AS votes_in_minute
FROM vote_events_valid_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY city_code, candidate_id
EMIT CHANGES;

-- =============================================================================
-- TODO 5 : TABLE rejets par raison
-- =============================================================================
CREATE TABLE IF NOT EXISTS rejected_by_reason AS
SELECT
  error_reason,
  AS_VALUE(error_reason) AS error_reason_v,
  COUNT(*) AS rejected_count
FROM vote_events_rejected_stream
GROUP BY error_reason
EMIT CHANGES;

-- =============================================================================
-- TODO 6 : TABLE département × bloc (carte)
-- =============================================================================
CREATE TABLE IF NOT EXISTS vote_count_by_dept_block
WITH ( KEY_FORMAT='JSON' ) AS
SELECT
  department_code,
  candidate_block,
  AS_VALUE(department_code) AS department_code_v,
  AS_VALUE(candidate_block) AS block_v,
  COUNT(*) AS total_votes
FROM vote_events_valid_stream
WHERE department_code IS NOT NULL AND candidate_block IS NOT NULL
GROUP BY department_code, candidate_block
EMIT CHANGES;