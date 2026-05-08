# Meridian Customer Operations — AI/BI + Genie golden demo

A self-contained Databricks asset showing the AI/BI Dashboard and Genie
working as one connected experience over a single Unity Catalog metric
view. The scenario is a fictitious contact centre — Meridian Customer
Operations — running across six sites and three channels (voice, chat,
email). Eighteen months of engineered call-level data drive every chart
and Genie answer.

## What's in this folder

| File | What it does |
| --- | --- |
| `01-data-generation.py` | Faker-based synthetic data generation. Writes parquet for sites, agents, calls (~1M rows). 6 deliberate patterns (Q-over-Q CSAT decline, Athens vs Toledo, email paradox, wait↔CSAT correlation, week-level volume noise, load coupling) plus seeded operational anomalies (Mar 6 stress, Birmingham AHT drift, new-hire cohort, Billing sentiment slide). |
| `02-tables-and-metric-views.sql` | Materialises bronze→silver→gold + the canonical `mv_calls` Unity Catalog metric view. Declares PK/FK constraints (Genie infers joins from these). Tags the surface tables Certified. |
| `03-dashboard.lvdash.json` | Lakeview dashboard JSON. Six pages: Overview, Operations, Agents, Voice of Customer, Forecast & Plan, Changelog. |
| `04-genie-space.yaml` | Genie space spec — 53 example queries, sample questions, room instructions. Joins are auto-discovered from UC FK constraints; SQL expressions are added manually post-deploy (see header comment). |
| `05-benchmark.yaml` | 16 benchmark cases for the Genie space. Surfaces in the Genie UI's Benchmarks tab. |
| `_resources/` | Setup helpers placeholder. |

## Deploying

From the repo root:

```bash
databricks bundle deploy --target default
databricks bundle run data_gen --target default
```

Then import `03-dashboard.lvdash.json` and `04-genie-space.yaml` via the
workspace UI (see top-level README).

Post-deploy manual UI steps (header comment on `04-genie-space.yaml` has
the full list): logo upload, certified-asset flag, SQL expression.
