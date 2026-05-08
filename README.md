# Meridian Customer Operations — AI/BI + Genie Demo

Industry-agnostic Databricks AI/BI Dashboard + Genie demo over a single
Unity Catalog metric view. 1M synthetic contact-centre call records,
six deliberately-engineered demo patterns, a six-page Lakeview
dashboard, a 53-example Genie space, and 16 benchmarks.

## What's in here

| File | Purpose |
| --- | --- |
| `aibi/aibi-contact-centre-golden/01-data-generation.py` | Faker-based synthetic data generator (sites, agents, calls — ~1M rows) |
| `aibi/aibi-contact-centre-golden/02-tables-and-metric-views.sql` | Materialises bronze → silver → gold + the `mv_calls` Unity Catalog metric view |
| `aibi/aibi-contact-centre-golden/03-dashboard.lvdash.json` | Lakeview dashboard JSON (six pages: Overview, Operations, Agents, Voice of Customer, Forecast & Plan, Changelog) |
| `aibi/aibi-contact-centre-golden/04-genie-space.yaml` | Genie space spec (53 example queries, sample questions, room instructions) |
| `aibi/aibi-contact-centre-golden/05-benchmark.yaml` | 16 benchmark cases for the Genie space |
| `databricks.yml` | Databricks Asset Bundle config |

## Quick start

```bash
# 1. Authenticate
databricks auth login --profile <your-profile>

# 2. Edit databricks.yml — set your workspace host, catalog, schema, and
#    warehouse_id under the `default` target.

# 3. Deploy the bundle (creates the schema, uploads notebooks)
databricks bundle deploy --target default

# 4. Generate data + materialise tables (~6 min for 1M calls)
databricks bundle run data_gen --target default
```

Then import the dashboard and Genie space via the workspace UI:

- **Dashboard** — Workspace UI → Import →
  `aibi/aibi-contact-centre-golden/03-dashboard.lvdash.json`. Bind it to
  the SQL warehouse you set in `databricks.yml`.
- **Genie space** — Genie UI → New space → Import from YAML →
  `aibi/aibi-contact-centre-golden/04-genie-space.yaml`. Joins are
  auto-discovered from the UC FK constraints declared by the SQL.

Post-deploy manual UI steps (header comment on `04-genie-space.yaml` has
the full list): logo upload, certified-asset flag, "Agents Needed Per
Shift" SQL expression.

## License

Apache 2.0 — see [LICENSE](LICENSE).
