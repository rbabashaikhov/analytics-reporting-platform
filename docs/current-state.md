# Current State — Analytics Reporting Platform

**Edition:** Portfolio Edition  
**Scope:** Monthly pipeline only  
**Document type:** Current-state freeze (packaging / documentation)

This document separates legacy MVP history from the confirmed recovered operational state and Portfolio Edition improvements. It does not claim roadmap items as complete. Client names, credentials, identifiers, URLs, and KPI values are intentionally omitted.

For the historical recovery freeze (dumps, sandbox ports, June/July reproduction notes), see `docs/technical-baseline.md`.

---

## 1. Legacy MVP

Historical delivery used:

- Adobe Analytics API as the upstream traffic source
- Jupyter notebooks for Monthly RAW → CALC → CDM transformations
- PostgreSQL databases for RAW / CALC / CDM layers
- An Excel dashboard as the MVP reporting surface

Weekly Organic and related tables may exist as historical database artifacts. They were never part of Portfolio Edition product scope and are not validated as an operational track here.

Private recovery materials (notebooks, dumps, token caches, real env files) remain outside this Git repository.

---

## 2. Confirmed recovered state

Isolated local PostgreSQL restores and controlled sandbox validation confirmed that the historical **Monthly** chain can run end-to-end:

```text
Adobe Analytics API → RAW → CALC → CDM
```

Confirmed operational facts for Portfolio Edition packaging:

| Capability | Status |
|---|---|
| Monthly-only pipeline | Confirmed |
| Adobe → RAW → CALC → CDM | Live E2E executed successfully |
| Airflow orchestration (`monthly_private_pipeline`) | Confirmed |
| Incremental mode | Confirmed |
| Rebuild mode | Confirmed |
| Validate mode (read-only DQ) | Confirmed |
| Automatic DQ after ETL | Confirmed |
| Persistent validation artifacts | Confirmed |
| Runtime manual validation | PASS |
| Airflow `data_quality` | SUCCESS |
| Read-only validation | Confirmed |

Data model inventory (Monthly):

| Layer | Monthly marts |
|---|---:|
| RAW | 4 (3 Adobe + Wordstat brand queries) |
| CALC | 6 (5 Adobe + brand_demand) |
| CDM | 16 (15 Adobe + organic_brand_demand) |

Pipeline graph:

```text
determine_periods → [load_raw | load_wordstat_raw] → build_calc → build_cdm
  → data_quality → final_status
```

Execution modes:

| Mode | Behavior |
|---|---|
| `incremental` | Missing completed months → ETL → automatic DQ |
| `rebuild` | Manual selected period → ETL → automatic DQ |
| `validate` | No Adobe / API / DWH writes → read-only DQ |

Critical DQ **FAIL** fails the Airflow `data_quality` task. Artifact root is configurable via `ARP_VALIDATION_ARTIFACT_ROOT`.

---

## 3. Portfolio Edition improvements

Relative to the notebook MVP, this repository packages:

- Client-neutral Python package under `src/analytics_reporting_platform/`
- Layer modules: RAW, CALC, CDM
- Execution planning and period helpers
- Orchestration stage functions called from Airflow
- Validation / DQ suite with contracts, reconciliation, coverage, and Monthly-only guard
- Persistent MD/JSON validation artifacts
- CLI entrypoint (`analytics-reporting`)
- Adobe client with sequential extraction, bounded retries, `429` Retry-After, and transient `5xx` handling
- Public-safe templates: `.env.example`, `config/client-config.example.yaml`
- Unit test coverage for package behavior (see README for latest counts)

Notification abstraction exists; default is **NoOp**. Real Telegram / email / Slack backends are not claimed as implemented.

Business metrics and mart semantics follow the recovered Monthly model — Portfolio Edition did not invent a new KPI taxonomy.

---

## 4. Remaining roadmap

Not completed in the current repository state:

- BI presentation layer / operational BI cutover
- Anonymized public demo dataset / export
- CI/CD workflows (not added as part of this packaging step)
- Optional notification integrations (Telegram / email / Slack)
- Weekly Organic as a separate product track (explicitly out of scope unless reopened)

Do not treat roadmap items as delivered.

---

## Security notes

- Secrets and client identifiers live in private env / YAML outside Git
- Dumps, real CSV/Excel extracts, validation runtime artifacts, and Airflow logs are excluded from Git
- This private repository is the engineering source of truth for the Monthly Portfolio Edition codebase; it must not contain client datasets or credential material
