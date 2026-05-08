-- =====================================================================
-- 02-tables-and-metric-views.sql
-- Bronze / Silver / Gold tables + the wide call view + the canonical
-- mv_calls METRIC VIEW for the AI/BI contact-centre golden demo.
--
-- Architecture:
--   silver_calls_wide  — wide row-level view: calls × agents × sites,
--                        with pre-baked SLA / abandon / negative flags.
--                        This is the main dashboard dataset and the
--                        source for the metric view.
--   mv_calls           — Unity Catalog METRIC VIEW. Canonical KPI
--                        semantic layer for Genie. Dashboard counters
--                        can use it for governance, but the wide view
--                        is preferred for cross-filtered widgets.
--
-- Executed by the /golden-demo build orchestrator. ${var.catalog} and
-- ${var.schema} are substituted by the data-generation notebook before
-- each statement is sent to spark.sql().
-- =====================================================================

-- SETUP ---------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ${var.catalog}.${var.schema};
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};

-- BRONZE --------------------------------------------------------------
CREATE OR REPLACE TABLE bronze_sites AS
  SELECT * FROM read_files('/Volumes/${var.catalog}/${var.schema}/landing/bronze_sites.parquet', format => 'parquet');
COMMENT ON TABLE bronze_sites IS 'Raw site roster from HR-ish source';

CREATE OR REPLACE TABLE bronze_agents AS
  SELECT * FROM read_files('/Volumes/${var.catalog}/${var.schema}/landing/bronze_agents.parquet', format => 'parquet');
COMMENT ON TABLE bronze_agents IS 'Raw agent roster';

CREATE OR REPLACE TABLE bronze_calls AS
  SELECT * FROM read_files('/Volumes/${var.catalog}/${var.schema}/landing/bronze_calls.parquet', format => 'parquet');
COMMENT ON TABLE bronze_calls IS 'Raw call fact — one row per completed call';

-- SILVER --------------------------------------------------------------
CREATE OR REPLACE TABLE silver_sites (
  site_id STRING NOT NULL COMMENT 'Unique site identifier, e.g. Birmingham',
  region STRING COMMENT 'Geographic region: EMEA / AMER / APAC',
  timezone STRING COMMENT 'IANA timezone',
  capacity INT COMMENT 'Max concurrent agents supported at this site',
  CONSTRAINT pk_silver_sites PRIMARY KEY (site_id)
) COMMENT 'Site dimension — Genie-exposed';

INSERT INTO silver_sites SELECT site_id, region, timezone, capacity FROM bronze_sites;

CREATE OR REPLACE TABLE silver_agents (
  agent_id STRING NOT NULL COMMENT 'Unique agent identifier',
  site_id STRING NOT NULL COMMENT 'FK → silver_sites.site_id',
  team STRING COMMENT 'Team name: Alpha / Bravo / Charlie / Delta',
  hire_date DATE COMMENT 'Date the agent was hired',
  full_time BOOLEAN COMMENT 'TRUE if full-time employee',
  tenure_days INT COMMENT 'Days since hire_date',
  full_name STRING COMMENT 'Agent display name — surfaced in dashboards and Genie',
  CONSTRAINT pk_silver_agents PRIMARY KEY (agent_id),
  CONSTRAINT fk_silver_agents_site FOREIGN KEY (site_id) REFERENCES silver_sites(site_id)
) COMMENT 'Agent dimension — Genie-exposed';

INSERT INTO silver_agents
SELECT agent_id, site_id, team, hire_date::DATE, full_time, tenure_days, full_name
FROM bronze_agents;

CREATE OR REPLACE TABLE silver_calls (
  call_id STRING NOT NULL,
  started_at TIMESTAMP, ended_at TIMESTAMP,
  channel STRING, queue STRING, topic STRING,
  agent_id STRING, site_id STRING,
  handle_time_sec INT, wait_time_sec INT,
  abandoned BOOLEAN, csat_score INT, sentiment_score DOUBLE,
  CONSTRAINT pk_silver_calls PRIMARY KEY (call_id)
) COMMENT 'Internal silver fact — superseded by gold_calls_enriched and silver_calls_wide';

INSERT INTO silver_calls
SELECT call_id, started_at::TIMESTAMP, ended_at::TIMESTAMP, channel, queue, topic,
       agent_id, site_id, handle_time_sec, wait_time_sec, abandoned, csat_score, sentiment_score
FROM bronze_calls;

-- GOLD ----------------------------------------------------------------
-- gold_calls_enriched: per-call fact with AI-summary text on the bottom
-- 500 most-negative rows. Kept for individual-call lookups via Genie.
CREATE OR REPLACE TABLE gold_calls_enriched (
  call_id STRING NOT NULL COMMENT 'Unique call identifier',
  started_at TIMESTAMP COMMENT 'Call start timestamp',
  ended_at TIMESTAMP COMMENT 'Call end timestamp',
  channel STRING COMMENT 'voice / chat / email',
  queue STRING COMMENT 'Queue identifier',
  topic STRING COMMENT 'Call topic category',
  agent_id STRING COMMENT 'FK → silver_agents.agent_id',
  site_id STRING COMMENT 'FK → silver_sites.site_id',
  handle_time_sec INT COMMENT 'Handle time in seconds',
  wait_time_sec INT COMMENT 'Wait time in seconds',
  abandoned BOOLEAN COMMENT 'True if call was abandoned',
  csat_score INT COMMENT 'CSAT 1-5, nullable',
  sentiment_score DOUBLE COMMENT 'Seeded sentiment in [-1, 1]',
  ai_summary STRING COMMENT 'AI-generated summary via ai_summarize (NULL except on bottom-500 sentiment slice)',
  ai_sentiment_label STRING COMMENT 'AI sentiment label via ai_analyze_sentiment (NULL except on bottom-500 sentiment slice)',
  CONSTRAINT pk_gold_calls_enriched PRIMARY KEY (call_id)
) COMMENT 'Per-call fact with AI-derived summary + sentiment label on the bottom 500 calls. Genie row-level source.';

INSERT INTO gold_calls_enriched
SELECT
  c.*,
  CAST(NULL AS STRING) AS ai_summary,
  CAST(NULL AS STRING) AS ai_sentiment_label
FROM silver_calls c;

MERGE INTO gold_calls_enriched g
USING (
  SELECT c.call_id,
         ai_summarize(bc.text, 100) AS ai_summary,
         ai_analyze_sentiment(bc.text) AS ai_sentiment_label
  FROM (
    SELECT call_id
    FROM silver_calls
    WHERE sentiment_score < -0.3
    ORDER BY sentiment_score ASC
    LIMIT 500
  ) c
  JOIN bronze_calls bc USING (call_id)
) src ON g.call_id = src.call_id
WHEN MATCHED THEN UPDATE SET
  g.ai_summary = src.ai_summary,
  g.ai_sentiment_label = src.ai_sentiment_label;

-- gold_forecast_volume: 30-day call volume forecast.
-- ai_forecast() works on a SQL warehouse but not on the serverless
-- notebook compute that runs this DDL (`aiFunctionPreviewUnavailable`).
-- Stub instead with weekly seasonality (weekend dip) + a mild upward
-- trend, so the forecast chart shows realistic shape rather than a flat
-- line. Re-test ai_forecast availability when the runtime catches up;
-- the column shape is identical so the swap is mechanical.
CREATE OR REPLACE TABLE gold_forecast_volume AS
WITH recent AS (
  SELECT AVG(daily_calls) AS mean_y, STDDEV(daily_calls) AS std_y
  FROM (
    SELECT date(started_at) AS ds, COUNT(*) AS daily_calls
    FROM silver_calls
    WHERE started_at >= current_date() - INTERVAL 90 DAYS
    GROUP BY 1
  )
),
days AS (
  SELECT
    current_date() + i AS ds,
    -- Weekend dip: typical contact-centre dayofweek pattern. Weekdays
    -- compensate so the weekly average matches the historical mean.
    CASE EXTRACT(DAYOFWEEK FROM current_date() + i)
      WHEN 1 THEN 0.45  -- Sunday
      WHEN 7 THEN 0.55  -- Saturday
      ELSE 1.10         -- Mon–Fri
    END AS dow_factor,
    -- Mild +0.3% per day = ~9% trend over the 30-day horizon.
    1.0 + 0.003 * i AS trend_factor
  FROM (SELECT explode(sequence(1, 30)) AS i)
)
SELECT
  d.ds,
  CAST(r.mean_y * d.dow_factor * d.trend_factor AS DOUBLE) AS y_hat,
  CAST(r.mean_y * d.dow_factor * d.trend_factor - 1.96 * r.std_y AS DOUBLE) AS y_hat_lower,
  CAST(r.mean_y * d.dow_factor * d.trend_factor + 1.96 * r.std_y AS DOUBLE) AS y_hat_upper
FROM days d CROSS JOIN recent r
ORDER BY ds;
COMMENT ON TABLE gold_forecast_volume IS '30-day call volume forecast — stub with weekly seasonality + trend (ai_forecast not yet available on serverless notebook compute)';

-- Column-level comments — needed because gold_forecast_volume is created
-- via CTAS (no inline column comments are possible). Re-apply on every
-- run so they survive the CREATE OR REPLACE TABLE above.
ALTER TABLE gold_forecast_volume ALTER COLUMN ds          COMMENT 'Forecast date (DATE).';
ALTER TABLE gold_forecast_volume ALTER COLUMN y_hat       COMMENT 'Point forecast — predicted call volume for the day.';
ALTER TABLE gold_forecast_volume ALTER COLUMN y_hat_lower COMMENT 'Lower bound of the forecast confidence interval (~95% CI).';
ALTER TABLE gold_forecast_volume ALTER COLUMN y_hat_upper COMMENT 'Upper bound of the forecast confidence interval (~95% CI).';

-- WIDE ----------------------------------------------------------------
-- silver_calls_wide is the workhorse. One row per call, joined to agent
-- and site dimensions, with pre-baked flags so widget encodings stay
-- shallow (AVG(meets_sla), AVG(is_abandoned), etc.). The metric view
-- below sits on top of this — single source of truth for KPIs.
--
-- meets_sla is channel-aware:
--   voice ≤ 20s, chat ≤ 60s, email ≤ 24h. Abandoned calls count as 0.
-- Materialized as a TABLE rather than a view because Spark Connect (the
-- notebook compute) doesn't support `ALTER VIEW ALTER COLUMN COMMENT` or
-- inline column-list COMMENTs in CREATE VIEW — both throw PARSE_SYNTAX_ERROR.
-- ALTER TABLE ALTER COLUMN COMMENT works everywhere, so the cleanest path
-- to per-column metadata is a CTAS table. Storage cost is small (1M rows
-- × 30 cols ≈ 300MB) and reads are faster because the join isn't recomputed.
--
-- Drop any pre-existing object first; CREATE OR REPLACE TABLE refuses to
-- replace an object of a different type. Earlier builds shipped this as
-- a view, then a table — DROP TABLE IF EXISTS handles the steady-state
-- (it's now always a table). For workspaces still on the old VIEW shape,
-- run `DROP VIEW IF EXISTS silver_calls_wide` once by hand before the
-- first build; the per-build DDL can't safely guess which type exists
-- (Spark Connect raises DROP_COMMAND_TYPE_MISMATCH on a wrong-type drop
-- even with IF EXISTS).
DROP TABLE IF EXISTS silver_calls_wide;
CREATE OR REPLACE TABLE silver_calls_wide AS
SELECT
  c.call_id,
  c.started_at,
  c.ended_at,
  date(c.started_at)                               AS date,
  date_trunc('week', c.started_at)                 AS week,
  c.channel,
  c.queue,
  c.topic,
  c.agent_id,
  a.full_name                                      AS agent_name,
  a.team,
  a.hire_date,
  a.full_time,
  a.tenure_days,
  CASE WHEN a.tenure_days <= 90  THEN 'new'
       WHEN a.tenure_days <= 365 THEN 'junior'
       ELSE 'tenured' END                          AS tenure_bucket,
  c.site_id,
  s.region,
  s.timezone,
  s.capacity                                       AS site_capacity,
  c.handle_time_sec,
  c.handle_time_sec / 60.0                         AS handle_time_min,
  c.wait_time_sec,
  c.wait_time_sec / 60.0                           AS wait_time_min,
  CASE WHEN c.channel IN ('voice', 'chat')
       THEN c.wait_time_sec END                    AS wait_time_voice_chat_sec,
  c.abandoned,
  c.csat_score,
  c.sentiment_score,
  c.ai_summary,
  c.ai_sentiment_label,
  CASE
    WHEN c.abandoned                                          THEN 0.0
    WHEN c.channel = 'voice' AND c.wait_time_sec <= 20        THEN 1.0
    WHEN c.channel = 'chat'  AND c.wait_time_sec <= 60        THEN 1.0
    WHEN c.channel = 'email' AND c.wait_time_sec <= 86400     THEN 1.0
    ELSE 0.0
  END                                              AS meets_sla,
  CASE WHEN c.abandoned THEN 1.0 ELSE 0.0 END      AS is_abandoned,
  CASE WHEN c.sentiment_score < -0.2 THEN 1.0
       ELSE 0.0 END                                AS is_negative
FROM gold_calls_enriched c
LEFT JOIN silver_agents a ON c.agent_id = a.agent_id
LEFT JOIN silver_sites  s ON c.site_id  = s.site_id;

COMMENT ON TABLE silver_calls_wide IS
  'Wide call grain — calls × agents × sites with pre-baked SLA / abandon / negative flags and a voice/chat-only wait column. Source for the dashboard wide_calls dataset and the mv_calls metric view. One row per call.';

-- Column comments via ALTER VIEW. Spark Connect (the notebook compute) does
-- NOT accept inline COMMENT clauses in `CREATE VIEW (col COMMENT ...)`
-- column-lists — silently throws PARSE_SYNTAX_ERROR at the first quote.
-- ALTER VIEW ALTER COLUMN works on every parser path. Re-applied after
-- every CREATE OR REPLACE since the view is rebuilt from scratch.
ALTER TABLE silver_calls_wide ALTER COLUMN call_id                  COMMENT 'Unique call identifier (FK → gold_calls_enriched.call_id).';
ALTER TABLE silver_calls_wide ALTER COLUMN started_at               COMMENT 'Call start timestamp.';
ALTER TABLE silver_calls_wide ALTER COLUMN ended_at                 COMMENT 'Call end timestamp.';
ALTER TABLE silver_calls_wide ALTER COLUMN date                     COMMENT 'Calendar date the call started — DATE type, use for daily aggregates.';
ALTER TABLE silver_calls_wide ALTER COLUMN week                     COMMENT 'Monday-anchored week truncation of started_at — TIMESTAMP, use for weekly aggregates.';
ALTER TABLE silver_calls_wide ALTER COLUMN channel                  COMMENT 'voice, chat, or email.';
ALTER TABLE silver_calls_wide ALTER COLUMN queue                    COMMENT 'Inbound queue identifier — queue_01 through queue_08.';
ALTER TABLE silver_calls_wide ALTER COLUMN topic                    COMMENT 'Call topic: Billing, Order Status, Account Access, Technical Issue, Cancellation, General Enquiry.';
ALTER TABLE silver_calls_wide ALTER COLUMN agent_id                 COMMENT 'Agent that handled the call (FK → silver_agents.agent_id).';
ALTER TABLE silver_calls_wide ALTER COLUMN agent_name               COMMENT 'Agent display name — denormalised from silver_agents.full_name for Genie usability.';
ALTER TABLE silver_calls_wide ALTER COLUMN team                     COMMENT 'Agent team: Alpha / Bravo / Charlie / Delta.';
ALTER TABLE silver_calls_wide ALTER COLUMN hire_date                COMMENT 'Date the agent was hired.';
ALTER TABLE silver_calls_wide ALTER COLUMN full_time                COMMENT 'TRUE if the agent is full-time.';
ALTER TABLE silver_calls_wide ALTER COLUMN tenure_days              COMMENT 'Days since hire_date as of the call.';
ALTER TABLE silver_calls_wide ALTER COLUMN tenure_bucket            COMMENT '"new" (<=90 days), "junior" (<=365 days), or "tenured" (>365 days). Use this for new-hire-vs-tenured comparisons.';
ALTER TABLE silver_calls_wide ALTER COLUMN site_id                  COMMENT 'Site that handled the call: Athens, Birmingham, Cambridge, Dublin, Manchester, Toledo.';
ALTER TABLE silver_calls_wide ALTER COLUMN region                   COMMENT 'Region of the site: EMEA / AMER / APAC.';
ALTER TABLE silver_calls_wide ALTER COLUMN timezone                 COMMENT 'IANA timezone of the site.';
ALTER TABLE silver_calls_wide ALTER COLUMN site_capacity            COMMENT 'Max concurrent agents the site is provisioned for.';
ALTER TABLE silver_calls_wide ALTER COLUMN handle_time_sec          COMMENT 'Handle time (seconds) — agent talk/work time. Aka AHT.';
ALTER TABLE silver_calls_wide ALTER COLUMN handle_time_min          COMMENT 'Handle time in minutes (handle_time_sec / 60).';
ALTER TABLE silver_calls_wide ALTER COLUMN wait_time_sec            COMMENT 'Customer wait time before the call was answered, in seconds. Includes email response times (up to 24h).';
ALTER TABLE silver_calls_wide ALTER COLUMN wait_time_min            COMMENT 'Wait time in minutes (wait_time_sec / 60).';
ALTER TABLE silver_calls_wide ALTER COLUMN wait_time_voice_chat_sec COMMENT 'Wait time for voice/chat calls only — NULL for email. Use this for ASA (avg speed of answer) so email response times do not skew the average.';
ALTER TABLE silver_calls_wide ALTER COLUMN abandoned                COMMENT 'TRUE if the call was abandoned before being answered.';
ALTER TABLE silver_calls_wide ALTER COLUMN csat_score               COMMENT 'Customer satisfaction 1-5. NULL for ~30% of calls (no survey response).';
ALTER TABLE silver_calls_wide ALTER COLUMN sentiment_score          COMMENT 'Seeded sentiment score in [-1, 1]. <-0.2 is negative.';
ALTER TABLE silver_calls_wide ALTER COLUMN ai_summary               COMMENT 'AI-generated call summary (ai_summarize) — populated only on the bottom 500 calls by sentiment. NULL otherwise.';
ALTER TABLE silver_calls_wide ALTER COLUMN ai_sentiment_label       COMMENT 'AI sentiment label (ai_analyze_sentiment) — populated on the same bottom-500 slice as ai_summary. NULL otherwise.';
ALTER TABLE silver_calls_wide ALTER COLUMN meets_sla                COMMENT 'Channel-aware SLA flag (0.0 / 1.0). Voice <=20s, chat <=60s, email <=24h, abandoned counts as 0. Use AVG(meets_sla) for SLA %.';
ALTER TABLE silver_calls_wide ALTER COLUMN is_abandoned             COMMENT 'Numeric form of abandoned (0.0 / 1.0). Use AVG(is_abandoned) for abandon rate.';
ALTER TABLE silver_calls_wide ALTER COLUMN is_negative              COMMENT 'Numeric flag: 1.0 if sentiment_score < -0.2, else 0.0. Use AVG(is_negative) for negative-call share.';

-- PK/FK constraints (informational only — Spark/Delta doesn't enforce
-- them, but Unity Catalog uses them to surface relationships in Catalog
-- Explorer and to help Genie pick the right join keys).
ALTER TABLE silver_calls_wide ALTER COLUMN call_id   SET NOT NULL;
ALTER TABLE silver_calls_wide ADD CONSTRAINT pk_silver_calls_wide PRIMARY KEY (call_id);
ALTER TABLE silver_calls_wide ADD CONSTRAINT fk_silver_calls_wide_agent
  FOREIGN KEY (agent_id) REFERENCES silver_agents(agent_id);
ALTER TABLE silver_calls_wide ADD CONSTRAINT fk_silver_calls_wide_site
  FOREIGN KEY (site_id) REFERENCES silver_sites(site_id);

-- SUMMARY -------------------------------------------------------------
-- gold_kpi_summary: pre-aggregated KPI scan target. One row per
-- (date, week, site_id, region, channel, topic, queue, tenure_bucket).
-- Reduces ~1M-row silver_calls_wide scans to ~50-80k for KPI questions.
-- Both the metric view and dashboard widgets that aggregate at this
-- grain or coarser source from here for ~10-50x speedup.
--
-- Stores SUM and COUNT separately for each measure so re-aggregation
-- by the metric view stays mathematically correct (AVG of AVGs is
-- wrong; SUM/SUM isn't). agent_id and agent_name are deliberately NOT
-- in the grouping — including them would push row count to ~5M and
-- defeat the purpose. Agent-level KPI questions use silver_calls_wide.
DROP TABLE IF EXISTS gold_kpi_summary;
CREATE OR REPLACE TABLE gold_kpi_summary AS
SELECT
  date,
  week,
  site_id,
  region,
  channel,
  topic,
  queue,
  tenure_bucket,
  COUNT(*)                                                        AS calls,
  SUM(handle_time_sec)                                            AS handle_time_sum,
  SUM(wait_time_sec)                                              AS wait_time_sum,
  SUM(wait_time_voice_chat_sec)                                   AS wait_time_vc_sum,
  SUM(CASE WHEN wait_time_voice_chat_sec IS NOT NULL THEN 1 ELSE 0 END) AS wait_time_vc_count,
  SUM(meets_sla)                                                  AS sla_sum,
  SUM(is_abandoned)                                               AS abandon_sum,
  SUM(is_negative)                                                AS negative_sum,
  SUM(csat_score)                                                 AS csat_sum,
  SUM(CASE WHEN csat_score IS NOT NULL THEN 1 ELSE 0 END)         AS csat_count,
  SUM(sentiment_score)                                            AS sentiment_sum
FROM silver_calls_wide
GROUP BY date, week, site_id, region, channel, topic, queue, tenure_bucket;

COMMENT ON TABLE gold_kpi_summary IS 'Pre-aggregated KPI scan target — calls grouped by (date, week, site, region, channel, topic, queue, tenure_bucket). Sources mv_calls and the dashboard summary_calls dataset. Re-aggregate measures via SUM/SUM ratios so weighting stays correct.';

ALTER TABLE gold_kpi_summary ALTER COLUMN date               COMMENT 'Calendar date (DATE).';
ALTER TABLE gold_kpi_summary ALTER COLUMN week               COMMENT 'Monday-anchored week truncation (TIMESTAMP).';
ALTER TABLE gold_kpi_summary ALTER COLUMN site_id            COMMENT 'Site identifier — Athens, Birmingham, Cambridge, Dublin, Manchester, Toledo.';
ALTER TABLE gold_kpi_summary ALTER COLUMN region             COMMENT 'Region of the site — EMEA / AMER / APAC.';
ALTER TABLE gold_kpi_summary ALTER COLUMN channel            COMMENT 'voice, chat, or email.';
ALTER TABLE gold_kpi_summary ALTER COLUMN topic              COMMENT 'Call topic.';
ALTER TABLE gold_kpi_summary ALTER COLUMN queue              COMMENT 'Inbound queue identifier.';
ALTER TABLE gold_kpi_summary ALTER COLUMN tenure_bucket      COMMENT 'new / junior / tenured.';
ALTER TABLE gold_kpi_summary ALTER COLUMN calls              COMMENT 'Number of calls in this group. Use SUM(calls) for total volume.';
ALTER TABLE gold_kpi_summary ALTER COLUMN handle_time_sum    COMMENT 'Sum of handle_time_sec. Compute AHT as SUM(handle_time_sum) / SUM(calls).';
ALTER TABLE gold_kpi_summary ALTER COLUMN wait_time_sum      COMMENT 'Sum of wait_time_sec across all channels.';
ALTER TABLE gold_kpi_summary ALTER COLUMN wait_time_vc_sum   COMMENT 'Sum of wait_time_voice_chat_sec (voice + chat only). Pair with wait_time_vc_count to compute ASA.';
ALTER TABLE gold_kpi_summary ALTER COLUMN wait_time_vc_count COMMENT 'Count of voice/chat calls in the group. Use SUM(wait_time_vc_sum)/SUM(wait_time_vc_count) for ASA.';
ALTER TABLE gold_kpi_summary ALTER COLUMN sla_sum            COMMENT 'Sum of meets_sla flag. Compute SLA % as SUM(sla_sum)/SUM(calls).';
ALTER TABLE gold_kpi_summary ALTER COLUMN abandon_sum        COMMENT 'Sum of is_abandoned flag. Compute abandon rate as SUM(abandon_sum)/SUM(calls).';
ALTER TABLE gold_kpi_summary ALTER COLUMN negative_sum       COMMENT 'Sum of is_negative flag. Compute negative share as SUM(negative_sum)/SUM(calls).';
ALTER TABLE gold_kpi_summary ALTER COLUMN csat_sum           COMMENT 'Sum of csat_score (excludes NULLs). Pair with csat_count to compute CSAT.';
ALTER TABLE gold_kpi_summary ALTER COLUMN csat_count         COMMENT 'Count of calls with non-null csat_score. Use SUM(csat_sum)/SUM(csat_count) for CSAT average.';
ALTER TABLE gold_kpi_summary ALTER COLUMN sentiment_sum      COMMENT 'Sum of sentiment_score. Compute avg sentiment as SUM(sentiment_sum)/SUM(calls).';

-- PK / FK on the summary: composite PK matches the GROUP BY (week and
-- region are functionally dependent on date and site_id respectively, so
-- they're not in the PK). FK to silver_sites surfaces the geography join.
ALTER TABLE gold_kpi_summary ALTER COLUMN date          SET NOT NULL;
ALTER TABLE gold_kpi_summary ALTER COLUMN site_id       SET NOT NULL;
ALTER TABLE gold_kpi_summary ALTER COLUMN channel       SET NOT NULL;
ALTER TABLE gold_kpi_summary ALTER COLUMN topic         SET NOT NULL;
ALTER TABLE gold_kpi_summary ALTER COLUMN queue         SET NOT NULL;
ALTER TABLE gold_kpi_summary ALTER COLUMN tenure_bucket SET NOT NULL;
ALTER TABLE gold_kpi_summary ADD CONSTRAINT pk_gold_kpi_summary
  PRIMARY KEY (date, site_id, channel, topic, queue, tenure_bucket);
ALTER TABLE gold_kpi_summary ADD CONSTRAINT fk_gold_kpi_summary_site
  FOREIGN KEY (site_id) REFERENCES silver_sites(site_id);

-- METRIC VIEW ---------------------------------------------------------
-- mv_calls is the canonical KPI semantic layer. Genie reads MEASURE()
-- definitions as authoritative; the dashboard's KPI counters mirror
-- these expressions. Slice by any dimension, aggregate any measure.
--
-- Old mv_ops_kpis / mv_agent_kpis are subsumed: same numbers come out
-- of mv_calls when you slice by date/channel/site or by agent.
-- mv_calls now sources from gold_kpi_summary (pre-aggregated) instead of
-- silver_calls_wide (row-level). 10-50x faster KPI queries — same MEASURE
-- names and definitions, just SUM/SUM ratios under the hood. agent_id /
-- agent_name dimensions were dropped because including agents in the
-- summary's GROUP BY would push row count from ~50k to ~5M and defeat
-- the purpose. Agent-level KPI questions go directly to silver_calls_wide.
CREATE OR REPLACE VIEW mv_calls
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: 'Meridian Customer Operations KPI semantic layer. One source of truth for SLA, ASA, AHT, abandon, CSAT, sentiment, and call volume — same definitions the dashboard reads. Sources gold_kpi_summary for fast KPI scans. For agent-level KPIs use silver_calls_wide directly. Use MEASURE() to retrieve any measure.'
source: SELECT * FROM ${var.catalog}.${var.schema}.gold_kpi_summary
dimensions:
  - name: date
    expr: date
    comment: 'Calendar date (DATE).'
  - name: week
    expr: week
    comment: 'Monday-anchored week truncation (TIMESTAMP). Use for weekly aggregates.'
  - name: channel
    expr: channel
    comment: 'voice, chat, or email.'
  - name: queue
    expr: queue
    comment: 'Inbound queue identifier (queue_01 .. queue_08).'
  - name: topic
    expr: topic
    comment: 'Call topic — Billing, Order Status, Account Access, Technical Issue, Cancellation, General Enquiry.'
  - name: site_id
    expr: site_id
    comment: 'Site that handled the call: Athens, Birmingham, Cambridge, Dublin, Manchester, Toledo.'
  - name: region
    expr: region
    comment: 'Region of the site: EMEA / AMER / APAC.'
  - name: tenure_bucket
    expr: tenure_bucket
    comment: '"new" (≤90 days), "junior" (≤365 days), or "tenured" (>365 days).'
measures:
  - name: total_calls
    expr: SUM(calls)
    comment: 'Total calls (handled + abandoned).'
  - name: handled_calls
    expr: SUM(calls) - SUM(abandon_sum)
    comment: 'Calls answered (excludes abandoned).'
  - name: abandon_rate
    expr: SUM(abandon_sum) / SUM(calls)
    comment: 'Share of calls abandoned before answer (0.0 - 1.0). Industry target ≤5%.'
  - name: sla_pct
    expr: SUM(sla_sum) / SUM(calls)
    comment: 'Service-level percentage. Channel-aware threshold: voice ≤20s, chat ≤60s, email ≤24h. Abandoned counts as missed SLA. Industry target ≥80%.'
  - name: avg_handle_sec
    expr: SUM(handle_time_sum) / SUM(calls)
    comment: 'Average handle time in seconds (AHT). Industry target ≤300s.'
  - name: avg_wait_voice_chat_sec
    expr: SUM(wait_time_vc_sum) / SUM(wait_time_vc_count)
    comment: 'Average speed of answer (ASA) for voice + chat in seconds. Email excluded. Industry target ≤30s.'
  - name: csat
    expr: SUM(csat_sum) / SUM(csat_count)
    comment: 'Average customer satisfaction (1-5 scale). Industry target ≥4.0. Excludes the ~30% of calls with no survey response.'
  - name: avg_sentiment
    expr: SUM(sentiment_sum) / SUM(calls)
    comment: 'Average sentiment score in [-1, 1].'
  - name: negative_share
    expr: SUM(negative_sum) / SUM(calls)
    comment: 'Share of calls with sentiment_score < -0.2.'
$$;

-- Drop the now-superseded views from prior demo iterations. IF EXISTS
-- so re-runs against a fresh schema don't fail.
DROP VIEW IF EXISTS mv_ops_kpis;
DROP VIEW IF EXISTS mv_agent_kpis;
DROP TABLE IF EXISTS gold_topic_sentiment_daily;
DROP TABLE IF EXISTS gold_topic_channel_flow;

-- DIM-TABLE DOC -------------------------------------------------------
-- silver_agents / silver_sites / gold_calls_enriched were originally
-- created via column-list CREATE TABLE blocks higher up that don't carry
-- inline comments (Spark Connect parser rejects `column TYPE COMMENT '…'`
-- in CREATE TABLE on serverless notebook compute). Synced from the live
-- workspace 2026-04-30 — keeps Catalog Explorer / Genie picker rich.
COMMENT ON TABLE ${var.catalog}.${var.schema}.silver_agents
  IS 'Agent dimension — Genie-exposed';
COMMENT ON TABLE ${var.catalog}.${var.schema}.silver_sites
  IS 'Site dimension — Genie-exposed';
COMMENT ON TABLE ${var.catalog}.${var.schema}.gold_calls_enriched
  IS 'Per-call fact with AI-derived summary + sentiment label on the bottom 500 calls. Genie row-level source.';

ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN agent_id    COMMENT 'Unique agent identifier';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN full_name   COMMENT 'Agent display name — surfaced in dashboards and Genie';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN full_time   COMMENT 'TRUE if full-time employee';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN hire_date   COMMENT 'Date the agent was hired';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN site_id     COMMENT 'FK → silver_sites.site_id';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN team        COMMENT 'Team name: Alpha / Bravo / Charlie / Delta';
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents ALTER COLUMN tenure_days COMMENT 'Days since hire_date';

ALTER TABLE ${var.catalog}.${var.schema}.silver_sites ALTER COLUMN site_id  COMMENT 'Unique site identifier, e.g. Birmingham';
ALTER TABLE ${var.catalog}.${var.schema}.silver_sites ALTER COLUMN region   COMMENT 'Geographic region: EMEA / AMER / APAC';
ALTER TABLE ${var.catalog}.${var.schema}.silver_sites ALTER COLUMN timezone COMMENT 'IANA timezone';
ALTER TABLE ${var.catalog}.${var.schema}.silver_sites ALTER COLUMN capacity COMMENT 'Max concurrent agents supported at this site';

ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN call_id            COMMENT 'Unique call identifier';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN started_at         COMMENT 'Call start timestamp';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN ended_at           COMMENT 'Call end timestamp';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN channel            COMMENT 'voice / chat / email';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN queue              COMMENT 'Queue identifier';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN topic              COMMENT 'Call topic category';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN agent_id           COMMENT 'FK → silver_agents.agent_id';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN site_id            COMMENT 'FK → silver_sites.site_id';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN handle_time_sec    COMMENT 'Handle time in seconds';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN wait_time_sec      COMMENT 'Wait time in seconds';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN abandoned          COMMENT 'True if call was abandoned';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN csat_score         COMMENT 'CSAT 1-5, nullable';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN sentiment_score    COMMENT 'Seeded sentiment in [-1, 1]';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN ai_summary         COMMENT 'AI-generated summary via ai_summarize (NULL except on bottom-500 sentiment slice)';
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched ALTER COLUMN ai_sentiment_label COMMENT 'AI sentiment label via ai_analyze_sentiment (NULL except on bottom-500 sentiment slice)';

-- GRANTS --------------------------------------------------------------
-- Schema-level grants only. Catalog-level GRANT USE CATALOG requires
-- MANAGE on the catalog, which a demo user typically doesn't have.
GRANT USE SCHEMA ON SCHEMA ${var.catalog}.${var.schema} TO `account users`;
GRANT SELECT ON SCHEMA ${var.catalog}.${var.schema} TO `account users`;

-- CERTIFICATION --------------------------------------------------------
-- Tag the canonical Genie surface as Certified. The tag shows up as a
-- chip in Catalog Explorer + the Genie data-source picker. We tag the
-- metric view, the wide row-grain table, the pre-aggregated KPI summary,
-- the forecast, and the dim lookups — everything an analyst would touch
-- through Genie. (`system.Certified` is a reserved key — use the bare
-- `Certified` tag here; UC SQL tag API rejects keys with `.`.)
ALTER VIEW  ${var.catalog}.${var.schema}.mv_calls             SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.silver_calls_wide    SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.gold_kpi_summary     SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.gold_forecast_volume SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.gold_calls_enriched  SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.silver_agents        SET TAGS ('Certified' = 'true');
ALTER TABLE ${var.catalog}.${var.schema}.silver_sites         SET TAGS ('Certified' = 'true');
