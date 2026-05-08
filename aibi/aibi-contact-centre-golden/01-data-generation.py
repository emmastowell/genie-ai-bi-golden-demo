# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Customer Operations — Data Generation
# MAGIC
# MAGIC Generates Faker-based bronze parquet (sites, agents, calls) with 5 built-in
# MAGIC anomalies. The logic here is an inlined copy of `golden_demo.data_generator`
# MAGIC from the sibling `golden-demo-skill` repo — inlined so the notebook runs on
# MAGIC any cluster without installing the skill package. Keep the two in sync when
# MAGIC either changes.

# COMMAND ----------

# MAGIC %pip install faker>=25.0 pyyaml>=6.0 pydantic>=2.6 pandas>=2.2 pyarrow>=15
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Parameters — configurable via job parameters or notebook widgets.
dbutils.widgets.text("catalog", "demo_golden_dev", "Unity Catalog")
dbutils.widgets.text("schema", "contact_centre", "Schema")
dbutils.widgets.text("sql_path", "", "Absolute path to 02-tables-and-metric-views.sql (set by DAB)")
dbutils.widgets.text("n_calls", "500000", "Number of calls to generate")
dbutils.widgets.text("n_agents", "2000", "Number of agents")
dbutils.widgets.text("days", "540", "History window in days")
dbutils.widgets.text("seed", "42", "Random seed")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SQL_PATH = dbutils.widgets.get("sql_path")
N_CALLS = int(dbutils.widgets.get("n_calls"))
N_AGENTS = int(dbutils.widgets.get("n_agents"))
DAYS = int(dbutils.widgets.get("days"))
SEED = int(dbutils.widgets.get("seed"))

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# The catalog is assumed to exist (creating catalogs requires metastore-admin
# privileges that a typical demo user doesn't have). The schema is created
# by the DAB `schemas:` resource at deploy time; this IF NOT EXISTS is a
# belt-and-braces no-op. The Volume is per-schema so we can create it here.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

import os
os.makedirs(VOLUME_PATH, exist_ok=True)
print(f"Target: {CATALOG}.{SCHEMA} → {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly config
# MAGIC Embedded verbatim from `data-generation/anomalies.yaml`. Update both places
# MAGIC when changing. Five anomalies: Monday spike, surprise topic spike, degrading
# MAGIC site, new-hire AHT cluster, billing sentiment drift.

# COMMAND ----------

import yaml

ANOMALIES_YAML = """
monday_spike: { window_start: "08:00", window_end: "10:00", multiplier: 1.6 }
surprise_spike: { start_day_offset: -30, duration_days: 3, topic: "Technical Issue", volume_delta: 0.40, sentiment_delta: -0.35 }
degrading_site: { site_id: "Birmingham", metric: "handle_time_sec", drift_pct: 0.30, duration_days: 60 }
new_hire_cluster: { count: 12, hire_window_days: 90, aht_delta_pct: 0.60 }
billing_sentiment_drift: { topic: "Billing", drift: -0.20, duration_months: 6 }
"""

from pydantic import BaseModel

class MondaySpike(BaseModel):
    window_start: str; window_end: str; multiplier: float

class SurpriseSpike(BaseModel):
    start_day_offset: int; duration_days: int; topic: str
    volume_delta: float; sentiment_delta: float

class DegradingSite(BaseModel):
    site_id: str; metric: str; drift_pct: float; duration_days: int

class NewHireCluster(BaseModel):
    count: int; hire_window_days: int; aht_delta_pct: float

class BillingSentimentDrift(BaseModel):
    topic: str; drift: float; duration_months: int

class AnomalyConfig(BaseModel):
    monday_spike: MondaySpike
    surprise_spike: SurpriseSpike
    degrading_site: DegradingSite
    new_hire_cluster: NewHireCluster
    billing_sentiment_drift: BillingSentimentDrift

CFG = AnomalyConfig.model_validate(yaml.safe_load(ANOMALIES_YAML))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension generators — sites and agents

# COMMAND ----------

import random
from datetime import datetime, timedelta
from faker import Faker

REGIONS = ["EMEA", "AMER", "APAC"]

# City names that read naturally as either US or European places.
# Birmingham (UK / Alabama), Manchester (UK / NH), Dublin (IE / OH),
# Toledo (Spain / OH), Cambridge (UK / MA), Athens (Greece / GA).
SITE_NAMES = ["Birmingham", "Manchester", "Dublin", "Toledo", "Cambridge", "Athens"]

# Per-site profile multipliers. Athens vs Toledo widened to a clean 2× CSAT
# ratio (PATTERN #2 — segment winner): Athens weights skew 4-5, Toledo 1-2.
#   wait_factor  — multiplier on wait_time_sec (higher = worse SLA)
#   aht_factor   — multiplier on handle_time_sec
#   csat_weights — weighted choices over [1,2,3,4,5]
SITE_PROFILES = {
    "Athens":     {"wait_factor": 0.70, "aht_factor": 0.80, "csat_weights": [0,  0,  2,  8, 15]},  # ~4.5
    "Cambridge":  {"wait_factor": 0.90, "aht_factor": 0.92, "csat_weights": [1,  2,  5,  9,  9]},  # ~3.7
    "Manchester": {"wait_factor": 1.00, "aht_factor": 1.00, "csat_weights": [1,  3,  6,  8,  7]},  # ~3.6
    "Dublin":     {"wait_factor": 1.05, "aht_factor": 1.05, "csat_weights": [2,  4,  7,  7,  5]},  # ~3.4
    "Birmingham": {"wait_factor": 1.10, "aht_factor": 1.00, "csat_weights": [3,  5,  7,  6,  4]},  # ~3.1
    "Toledo":     {"wait_factor": 1.40, "aht_factor": 1.25, "csat_weights": [10, 8,  5,  3,  1]},  # ~2.1
}

# Per-topic profile — tightens the per-topic spread on CSAT, sentiment, and
# AHT so widgets like "Top topics" actually have a story to tell instead of
# all categories looking the same.
#   csat_bias       — additive shift on CSAT (probabilistic +/- 1)
#   sentiment_bias  — additive shift on sentiment_score in [-1,1]
#   aht_factor      — multiplier on handle_time_sec
TOPIC_PROFILES = {
    "Billing":         {"csat_bias": -0.80, "sentiment_bias": -0.30, "aht_factor": 1.10},
    "Account Access":  {"csat_bias": -0.10, "sentiment_bias": -0.05, "aht_factor": 1.40},
    "Technical Issue": {"csat_bias": -0.05, "sentiment_bias": -0.10, "aht_factor": 1.30},
    "Cancellation":    {"csat_bias": +0.10, "sentiment_bias": -0.20, "aht_factor": 0.95},
    "General Enquiry": {"csat_bias": +0.20, "sentiment_bias": +0.10, "aht_factor": 0.80},
    "Order Status":    {"csat_bias": +0.50, "sentiment_bias": +0.20, "aht_factor": 0.70},
}

# Channel CSAT bias — drives PATTERN #3 (hidden anomaly: email = best CSAT,
# worst SLA). Customers prefer the async channel; the SLA threshold (24h) is
# what marks email "bad" operationally, not the customer experience. The
# email bonus is large enough to outrun the new wait-time penalty introduced
# for PATTERN #4 — without it, emails dragged through long queues come out
# with bad CSAT, defeating the inversion.
CHANNEL_CSAT_BIAS = {"voice": 0.0, "chat": -0.30, "email": +1.00}

# Per-topic volume weights — drives the dashboard's "Top topics" widget so
# the bars actually have a story instead of all topics being ~16% of volume.
# Distribution mirrors typical contact-centre reality: Billing dominates
# (everyone cares about money), Cancellation is rarer (customers usually
# self-serve via portal). Picking is rng.choices()-weighted so the
# distribution is exact in expectation.
TOPIC_VOLUME_WEIGHTS = {
    "Billing":         28,   # 28% — dominant; ties to PATTERN #1 (CSAT decline driven by Billing)
    "Order Status":    20,   # 20%
    "General Enquiry": 18,   # 18%
    "Technical Issue": 14,   # 14%
    "Account Access":  12,   # 12%
    "Cancellation":     8,   #  8%
}

# Per-topic 6-month sentiment drift. Each rate is the total shift over the
# 6-month visible window, applied linearly so the trend is smooth on the
# monthly chart. Mirrors and replaces the single-topic billing_sentiment_drift
# anomaly so every line on the topic-sentiment chart has visible movement
# instead of all-but-one being flat.
#   negative = sentiment got worse over the period
#   positive = sentiment improved
TOPIC_SENTIMENT_DRIFT_6M = {
    "Billing":         -0.20,  # ongoing degradation — drives the demo arc
    "Account Access":  -0.12,  # gradual decline (volume up, longer waits)
    "Cancellation":    -0.06,  # slight downward
    "General Enquiry":  0.00,  # stable baseline
    "Technical Issue": +0.08,  # recovery after Mar 6 stress event
    "Order Status":    +0.18,  # explicit improvement programme — counter-narrative
}

# Channel AHT multipliers — voice fastest, chat mid, email slowest.
CHANNEL_AHT_FACTORS = {"voice": 1.00, "chat": 1.30, "email": 1.70}

# Day-of-week weights for call volume rejection sampling.
DOW_VOLUME_WEIGHTS = {0: 1.00, 1: 1.00, 2: 1.00, 3: 1.00,
                     4: 0.95, 5: 0.55, 6: 0.45}

# PATTERN #1 — trend: CSAT in Q2 (Apr 2026 onward) drops by Q2_CSAT_DRAG
# probabilistic levels relative to Q1, producing a ~21% Q-over-Q decline.
# Reflects the Toledo new-hire onboarding wave + ongoing Billing sentiment
# drift compounding into measurable CSAT collapse.
Q2_START = datetime(2026, 4, 1)
Q2_CSAT_DRAG = 0.85   # probability of additional -1 step on Q2 calls

# PATTERN #5 — week-level volume noise. Multiplicative factor per ISO week,
# clipped so weeks vary between ~65% and 140% of baseline. Two manually-
# placed spike weeks land 1.5× volume so the dashboard "are there spikes?"
# question has a clear answer.
WEEK_NOISE_SIGMA = 0.18
WEEK_NOISE_CLIP  = (0.65, 1.40)


def _week_factor(dt, base_seed):
    """Deterministic per-ISO-week volume multiplier in [0.65, 1.40]."""
    week_idx = dt.toordinal() // 7
    rng = random.Random(week_idx ^ base_seed ^ 0xBEEF)
    return max(WEEK_NOISE_CLIP[0],
               min(WEEK_NOISE_CLIP[1], rng.gauss(1.0, WEEK_NOISE_SIGMA)))


def _is_spike_week(dt, today):
    """Two narrative spike weeks: end-of-Feb billing cycle + late-Jan launch."""
    week_idx = dt.toordinal() // 7
    spike_a = (today - timedelta(days=58)).toordinal() // 7  # ~Feb 22 — billing cycle
    spike_b = (today - timedelta(days=85)).toordinal() // 7  # ~Jan 26 — post-launch
    return week_idx in {spike_a, spike_b}


def _pick_csat(rng, base_weights, wait_time_sec, channel, day_stress,
               topic_bias, is_q2):
    """CSAT in [1,5] biased by:
      - site_weights (segment winner: Athens vs Toledo)
      - wait penalty (PATTERN #4 — wait↔CSAT correlation, smooth not bucketed)
      - channel bias (PATTERN #3 — email +, chat -, voice 0)
      - topic bias (per-topic baselines)
      - Q2 trend drag (PATTERN #1 — Q-over-Q decline)
      - day-stress (existing event-driven stress days)
    Bias is applied as probabilistic +/-1 steps so the distribution is real
    1-5 integers, not a continuous score."""
    raw = rng.choices([1, 2, 3, 4, 5], weights=base_weights, k=1)[0]
    sla_threshold = {"voice": 20, "chat": 60, "email": 86400}[channel]

    # PATTERN #4 — smooth wait→CSAT penalty proportional to SLA-multiple of
    # wait. 0.45 per SLA-multiple gives a continuous gradient across the
    # whole wait range, which is what produces a strong correlation
    # (target around -0.5).
    relative_wait = wait_time_sec / sla_threshold
    drop_prob = min(1.6, relative_wait * 0.45)

    if day_stress > 2.0:                   drop_prob += 0.30
    if day_stress > 3.5:                   drop_prob += 0.30

    # PATTERN #1 — Q2 trend drag, bumped to land in the 18-22% range
    if is_q2 and rng.random() < Q2_CSAT_DRAG:
        drop_prob += 0.55

    # PATTERN #3 — combined per-call shift = topic bias + channel bias
    shift = topic_bias + CHANNEL_CSAT_BIAS[channel]
    if shift > 0:
        if rng.random() < shift:           raw = min(5, raw + 1)
        if rng.random() < shift * 0.4:     raw = min(5, raw + 1)
    elif shift < 0:
        if rng.random() < -shift:          raw = max(1, raw - 1)
        if rng.random() < -shift * 0.4:    raw = max(1, raw - 1)

    if rng.random() < drop_prob:           raw = max(1, raw - 1)
    if rng.random() < drop_prob * 0.6:     raw = max(1, raw - 1)
    return raw


# Per-site capacity (max concurrent agents the site is provisioned for).
# Hand-set so the "Site utilisation vs capacity" chart on the dashboard
# tells a real story: most sites run close to capacity (3x roster vs ~1
# concurrent shift), Toledo is overprovisioned (the "expansion site that
# scaled ahead of demand" narrative — high capacity but worst SLA, so the
# manager learns it's an execution problem not a capacity problem).
SITE_CAPACITIES = {
    "Athens":     130,   # mature site, densely staffed
    "Cambridge":  130,
    "Manchester": 130,
    "Dublin":     140,
    "Birmingham": 130,
    "Toledo":     220,   # overprovisioned — capacity ratio nearly 2x other sites
}


def generate_sites(n: int = 6, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    if n > len(SITE_NAMES):
        raise ValueError(f"max {len(SITE_NAMES)} sites supported")
    return [
        {
            "site_id": SITE_NAMES[i],
            "region": rng.choice(REGIONS),
            "timezone": rng.choice(["UTC", "PST", "JST", "CET", "GMT"]),
            "capacity": SITE_CAPACITIES[SITE_NAMES[i]],
        }
        for i in range(n)
    ]

def generate_agents(n: int, sites: list[dict], new_hire_count: int,
                    hire_window_days: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    fake = Faker()
    fake.seed_instance(seed)
    today = datetime(2026, 4, 22)
    out = []
    for i in range(n):
        if i < new_hire_count:
            tenure_days = rng.randint(0, hire_window_days)
        else:
            tenure_days = rng.randint(hire_window_days + 1, 365 * 5)
        out.append({
            "agent_id": f"agent_{i:05d}",
            "site_id": rng.choice(sites)["site_id"],
            "team": rng.choice(["Alpha", "Bravo", "Charlie", "Delta"]),
            "hire_date": (today - timedelta(days=tenure_days)).date().isoformat(),
            "full_time": rng.random() > 0.15,
            "tenure_days": tenure_days,
            "full_name": fake.name(),
        })
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calls fact — transcripts + 5 anomalies

# COMMAND ----------

TOPICS = ["Billing", "Order Status", "Account Access", "Technical Issue", "Cancellation", "General Enquiry"]
CHANNELS = ["voice", "chat", "email"]

_TOPIC_OPENERS = {
    "Billing": [
        "Customer calling about an unexpected charge on the latest bill.",
        "Disputing a line item on their monthly invoice.",
        "Asking why the bill is higher than last month.",
        "Requesting a refund for what they believe is an incorrect charge.",
    ],
    "Order Status": [
        "Wants to know where their order is and when it will arrive.",
        "Tracking number is not updating; asking for an ETA.",
        "Delivery has been delayed and customer wants a firm date.",
        "Checking when their recent order is expected to ship.",
    ],
    "Account Access": [
        "Locked out of their account and cannot sign in.",
        "Password reset email is not arriving in their inbox.",
        "Two-factor authentication code is not working.",
        "Account was suspended unexpectedly; wants it reinstated.",
    ],
    "Technical Issue": [
        "App is crashing on startup; unable to use the service.",
        "Reporting a site outage that is blocking their workflow.",
        "Login succeeds but the dashboard will not load.",
        "Integration with their system has stopped working this morning.",
    ],
    "Cancellation": [
        "Wants to cancel their subscription effective immediately.",
        "Downgrading to the free plan due to cost concerns.",
        "Closing the account after switching to another provider.",
        "Requesting cancellation and confirmation that data will be deleted.",
    ],
    "General Enquiry": [
        "Asking whether a specific feature is available on their plan.",
        "Comparing plans before committing to an upgrade.",
        "Following up on an earlier support ticket.",
        "Wants to know the business hours for their region.",
    ],
}

_TONE_POSITIVE = [
    "Tone was friendly and cooperative throughout.",
    "Customer was patient and grateful for the help.",
    "Polite exchange; customer expressed thanks at close.",
]
_TONE_NEUTRAL = [
    "Tone was matter-of-fact and businesslike.",
    "Neutral, brief interaction with no emotional signal.",
    "Customer was reserved but cooperative.",
]
_TONE_NEGATIVE = [
    "Customer was frustrated and raised their voice several times.",
    "Tone was angry throughout; customer threatened to escalate.",
    "Customer interrupted frequently and expressed dissatisfaction.",
]
_RESOLUTIONS = [
    "Agent resolved the issue in under five minutes.",
    "Resolution required a transfer to tier-two support.",
    "Closed with a callback scheduled for later in the week.",
    "Resolved with a one-time goodwill credit applied to the account.",
    "Unresolved at end of call; follow-up ticket created.",
]

def _make_transcript(topic, sentiment, rng):
    opener = rng.choice(_TOPIC_OPENERS[topic])
    if sentiment > 0.2:
        tone = rng.choice(_TONE_POSITIVE)
    elif sentiment < -0.2:
        tone = rng.choice(_TONE_NEGATIVE)
    else:
        tone = rng.choice(_TONE_NEUTRAL)
    return f"{opener} {tone} {rng.choice(_RESOLUTIONS)}"

def _topic_channel_weights(topic):
    return {
        "Billing":          {"voice": 0.70, "chat": 0.20, "email": 0.10},
        "Order Status":     {"voice": 0.15, "chat": 0.65, "email": 0.20},
        "Account Access":   {"voice": 0.40, "chat": 0.45, "email": 0.15},
        "Technical Issue":  {"voice": 0.35, "chat": 0.55, "email": 0.10},
        "Cancellation":     {"voice": 0.15, "chat": 0.20, "email": 0.65},
        "General Enquiry":  {"voice": 0.30, "chat": 0.40, "email": 0.30},
    }[topic]

def _window_minute_bounds(window_start, window_end):
    sh, sm = (int(p) for p in window_start.split(":"))
    eh, em = (int(p) for p in window_end.split(":"))
    return sh * 60 + sm, eh * 60 + em

def generate_calls(n, agents, anomalies, days, seed=42):
    rng = random.Random(seed)
    today = datetime(2026, 4, 22)
    start_date = today - timedelta(days=days)
    ms = anomalies.monday_spike
    window_start_min, window_end_min = _window_minute_bounds(ms.window_start, ms.window_end)
    resample_prob = 1 - 1 / ms.multiplier
    spike = anomalies.surprise_spike
    spike_start = today + timedelta(days=spike.start_day_offset)
    spike_end = spike_start + timedelta(days=spike.duration_days)
    ds = anomalies.degrading_site
    bsd = anomalies.billing_sentiment_drift
    calls = []
    for i in range(n):
        # Rejection-sample for weekend dip × week-level volume noise (PATTERN
        # #5). The accept probability multiplies the DOW weight by the per-
        # week factor (capped at 1.4), so quiet weeks generate ~65% of the
        # baseline daily volume and busy weeks ~140%.
        while True:
            day_offset = rng.randint(0, days - 1)
            dt = start_date + timedelta(days=day_offset, seconds=rng.randint(0, 86_399))
            week_factor = _week_factor(dt, seed)
            if _is_spike_week(dt, today):
                week_factor = max(week_factor, 1.5)
            accept_prob = DOW_VOLUME_WEIGHTS[dt.weekday()] * (week_factor / WEEK_NOISE_CLIP[1])
            if rng.random() < accept_prob:
                break
        if dt.weekday() == 0 and rng.random() < resample_prob:
            mod = rng.randint(window_start_min, window_end_min - 1)
            dt = dt.replace(hour=mod // 60, minute=mod % 60)

        # PATTERN #6 — load-vs-quality coupling. load_factor scales sub-
        # super-linearly in week_factor (exponent 1.2): a 1.4× volume week
        # → 1.49× wait, abandon, SLA miss; quiet weeks yield the symmetric
        # improvement. The earlier exponent of 1.5 was punishing voice/chat
        # SLA enough to wipe out PATTERN #3's email-is-worst inversion.
        load_factor = week_factor ** 1.2

        topic = rng.choices(list(TOPIC_VOLUME_WEIGHTS.keys()),
                            weights=list(TOPIC_VOLUME_WEIGHTS.values()),
                            k=1)[0]
        if spike_start <= dt < spike_end and rng.random() < spike.volume_delta:
            topic = spike.topic
        topic_profile = TOPIC_PROFILES[topic]
        channel = rng.choices(CHANNELS, weights=list(_topic_channel_weights(topic).values()), k=1)[0]
        agent = rng.choice(agents)
        site_profile = SITE_PROFILES[agent["site_id"]]

        base_aht = rng.randint(240, 540)
        base_aht = int(base_aht * site_profile["aht_factor"])      # site
        base_aht = int(base_aht * CHANNEL_AHT_FACTORS[channel])    # channel
        base_aht = int(base_aht * topic_profile["aht_factor"])     # topic
        if agent["tenure_days"] <= anomalies.new_hire_cluster.hire_window_days:
            base_aht = int(base_aht * (1 + anomalies.new_hire_cluster.aht_delta_pct))
        if agent["site_id"] == ds.site_id:
            days_into_drift = max(0, (dt - (today - timedelta(days=ds.duration_days))).days)
            drift = min(1.0, days_into_drift / ds.duration_days) * ds.drift_pct
            base_aht = int(base_aht * (1 + drift))

        sentiment = rng.uniform(-0.4, 0.8) + topic_profile["sentiment_bias"]
        if spike_start <= dt < spike_end and topic == spike.topic:
            sentiment += spike.sentiment_delta
        # Per-topic 6-month sentiment drift — applies linearly so the
        # monthly chart shows the trend per topic. Older calls get less
        # drift; today's calls get the full shift.
        rate = TOPIC_SENTIMENT_DRIFT_6M.get(topic, 0.0)
        if rate != 0.0:
            months_ago = (today - dt).days / 30.0
            drift_progress = max(0.0, 1.0 - months_ago / 6.0)
            sentiment += drift_progress * rate
        # Per-(topic, month) noise — a deterministic offset shared by every
        # call in that bucket, so the chart's monthly aggregate moves with
        # it. ~σ=0.07 gives lines that wobble ±0.10-0.15 around their trend
        # — feels like real-world month-to-month variation rather than a
        # textbook-clean slope.
        month_idx = dt.year * 12 + dt.month
        noise_rng = random.Random(hash((topic, month_idx)) & 0xFFFFFFFF ^ seed)
        sentiment += noise_rng.gauss(0.0, 0.07)
        sentiment = max(-1.0, min(1.0, sentiment))

        # Per-day stress (existing — random ~5% of days). Independent of the
        # weekly noise so an event can land inside any week.
        day_offset = (dt.date() - start_date.date()).days
        day_rng = random.Random(day_offset * 1000 + seed)
        if day_rng.random() < 0.05:
            day_stress = day_rng.uniform(3.0, 5.0)
        else:
            day_stress = day_rng.uniform(0.95, 1.05)

        # Wait time — base × day_stress × site_factor × load_factor.
        # Email distribution shifts so ~25% breach the 24h SLA threshold —
        # PATTERN #3 (worst SLA on the channel customers actually prefer).
        wait_roll = rng.random()
        wait_factor = site_profile["wait_factor"]
        if channel == "voice":
            # Tightened tails — 90% baseline well under the 20s SLA so site
            # wait_factor + load coupling can degrade SLA without nuking it
            if wait_roll < 0.90:   base_wait = rng.randint(1, 12)
            elif wait_roll < 0.97: base_wait = rng.randint(15, 40)
            else:                  base_wait = rng.randint(45, 180)
            wait_time_sec = min(int(base_wait * day_stress * wait_factor * load_factor), 600)
        elif channel == "chat":
            if wait_roll < 0.85:   base_wait = rng.randint(3, 35)
            elif wait_roll < 0.97: base_wait = rng.randint(40, 120)
            else:                  base_wait = rng.randint(120, 600)
            wait_time_sec = min(int(base_wait * day_stress * wait_factor * load_factor), 1800)
        else:  # email — async, no per-call day_stress, but load_factor applies
            # PATTERN #3 — email is the worst-SLA channel. The 24h threshold
            # is generous in absolute time but harder to hit reliably than
            # voice's 20s when email volume spikes (humans queue up). Shift
            # the distribution so ~30% of emails breach 24h baseline; load
            # coupling pushes that further on busy weeks.
            if wait_roll < 0.30:   base_wait = rng.randint(60, 14400)        # ≤4h (30%)
            elif wait_roll < 0.55: base_wait = rng.randint(14400, 43200)     # 4-12h (25%)
            elif wait_roll < 0.70: base_wait = rng.randint(43200, 86400)     # 12-24h (15%)
            else:                  base_wait = rng.randint(86400, 172800)   # 24-48h (30%, misses SLA)
            wait_time_sec = min(int(base_wait * wait_factor * load_factor), 172800)

        # Abandon — base 3% × load_factor, capped at 25%. Day stress on top.
        abandon_prob = min(0.25,
                           0.03 * load_factor +
                           (0.04 if day_stress > 2.5 else 0.0))
        # Email rarely abandons — async, customer just waits
        if channel == "email":
            abandon_prob *= 0.15
        abandoned = rng.random() < abandon_prob

        is_q2 = dt >= Q2_START
        csat = (_pick_csat(rng, site_profile["csat_weights"], wait_time_sec,
                           channel, day_stress, topic_profile["csat_bias"], is_q2)
                if rng.random() > 0.3 else None)

        calls.append({
            "call_id": f"C-{i:08d}",
            "started_at": dt.isoformat(),
            "ended_at": (dt + timedelta(seconds=base_aht)).isoformat(),
            "channel": channel,
            "queue": f"queue_{rng.randint(1, 8):02d}",
            "topic": topic,
            "agent_id": agent["agent_id"],
            "site_id": agent["site_id"],
            "handle_time_sec": base_aht,
            "wait_time_sec": wait_time_sec,
            "abandoned": abandoned,
            "csat_score": csat,
            "sentiment_score": round(sentiment, 3),
            "text": _make_transcript(topic, sentiment, rng),
        })
    return calls

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and write parquet

# COMMAND ----------

import pandas as pd
from pathlib import Path

sites = generate_sites(n=6, seed=SEED)
agents = generate_agents(
    n=N_AGENTS, sites=sites,
    new_hire_count=CFG.new_hire_cluster.count,
    hire_window_days=CFG.new_hire_cluster.hire_window_days,
    seed=SEED,
)
calls = generate_calls(n=N_CALLS, agents=agents, anomalies=CFG, days=DAYS, seed=SEED)

out_dir = Path(VOLUME_PATH)
out_dir.mkdir(parents=True, exist_ok=True)
written = {}
for name, rows in [("sites", sites), ("agents", agents), ("calls", calls)]:
    path = out_dir / f"bronze_{name}.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    written[name] = str(path)
    print(f"  {name}: {len(rows):>8,} rows -> {path}")

print("\nParquet written. Running 02-tables-and-metric-views.sql next.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialise bronze → silver → gold + metric views
# MAGIC
# MAGIC Reads the SQL file uploaded alongside this notebook, substitutes the
# MAGIC `${var.catalog}` / `${var.schema}` placeholders, and runs each statement
# MAGIC via `spark.sql()`. The SQL file is the source of truth; this cell is
# MAGIC just an executor.

# COMMAND ----------

import re

if not SQL_PATH:
    raise RuntimeError(
        "sql_path widget is empty. When run via the DAB, the bundle sets this "
        "to ${workspace.file_path}/aibi/aibi-contact-centre-golden/02-tables-and-metric-views.sql. "
        "When running interactively, fill it in manually."
    )

sql_text = open(SQL_PATH).read()
sql_text = sql_text.replace("${var.catalog}", CATALOG).replace("${var.schema}", SCHEMA)

# Split on ; while protecting Metric View $$...$$ YAML blocks (which can
# contain colons and newlines but no raw semicolons), and ignoring
# semicolons inside SQL line comments.
blocks = {}
def _stash(match):
    key = f"__MV_BLOCK_{len(blocks)}__"
    blocks[key] = match.group(0)
    return key
stashed = re.sub(r"\$\$.*?\$\$", _stash, sql_text, flags=re.DOTALL)
# Strip `--` line comments BEFORE stashing strings — English apostrophes
# in comments (e.g. "-- it's now …") are unbalanced quotes that would
# otherwise hijack the string-stash regex and swallow whole statements.
stashed = re.sub(r"--[^\n]*", "", stashed)
# Now stash single-quoted SQL string literals so semicolons inside
# COMMENT '...' (or any other string) don't trip up the splitter.
# Handles SQL's doubled-quote escape (`''` for a literal `'` inside).
stashed = re.sub(r"'(?:[^']|'')*'", _stash, stashed)

statements = []
for raw in stashed.split(";"):
    stmt = raw.strip()
    for key, original in blocks.items():
        stmt = stmt.replace(key, original)
    if stmt:
        statements.append(stmt)

print(f"Executing {len(statements)} SQL statements")
for i, stmt in enumerate(statements, 1):
    head = stmt.splitlines()[0][:80].replace("\n", " ")
    print(f"  [{i:>2}/{len(statements)}] {head}...")
    spark.sql(stmt)

print("\nDone. Tables + metric views materialised.")
