# Technical Baseline

**Project:** Analytics Reporting Platform — Portfolio Edition  
**Document type:** Technical baseline freeze (historical recovery)  
**Phase:** CURRENT STATE / TECHNICAL BASELINE  
**Validated scope:** Monthly report only  

This baseline records recovered infrastructure, Monthly inventory, mutation semantics, and accepted validation outcomes from the recovery / sandbox phase. For the current Portfolio Edition operational freeze (Airflow, DQ, execution modes), see `docs/current-state.md`.

---

## Baseline Environment

| Component | Status |
|---|---|
| Frozen historical PostgreSQL restore | Accepted / read-only |
| Monthly execution sandbox | Accepted for controlled runs |
| Historical Monthly notebooks | Recovered / immutable archive |
| Adobe API access for Monthly RAW | Usable (credentials private) |
| Weekly Organic | Present as historical artifacts only; **not validated** in this baseline |
| Airflow / new BI / formal DQ | **Not implemented** for this platform |

---

## PostgreSQL

| Property | Frozen baseline | Execution sandbox |
|---|---|---|
| Compose project | `arp-dwh-restore` | `arp-monthly-sandbox` |
| Host bind | `127.0.0.1:15432` | `127.0.0.1:15433` |
| Engine | PostgreSQL 14.23 | PostgreSQL 14.23 |
| Isolation | Separate container / volume / network | Separate container / volume / network |
| Role | Immutable reference | Controlled mutation target |

Notes:

- Airflow metadata PostgreSQL is **not** this DWH.
- Existing Airflow / Superset / DataLens infrastructure remains separate and is **not yet integrated**.

---

## Database Inventory

Both environments contain:

| Database | Tables |
|---|---:|
| `raw_data` | 6 |
| `calc_data` | 8 |
| `cdm_data` | 20 |

Historical dump restore into empty databases: **PASS**.

`cdm_data` includes:

- 15 Monthly CDM marts (validated for Monthly baseline)
- 5 Weekly CDM tables (historical presence only; out of current validation scope)

---

## Monthly RAW

### Tables

- `raw_aa_monthly_metrics`
- `raw_aa_monthly_by_section`
- `raw_aa_monthly_by_url`

### Accepted historical coverage (frozen)

- 2025-01 → 2026-06
- 18 distinct months

### Sandbox after July incremental

- 2025-01 → 2026-07
- 19 distinct months

### Behavior

- Extracted from Adobe Analytics API
- Append new month when month does not exist
- Skip existing month when replacement disabled
- Optional delete-and-reload for selected months when replacement enabled

---

## Monthly CALC

### Tables

- `calc_monthly_kpi`
- `calc_monthly_channel_mix`
- `calc_monthly_search_engines`
- `calc_monthly_section_share`
- `calc_explore_buying_guides_kpi`

### Accepted historical coverage (frozen)

- 2026-01 → 2026-06
- 6 distinct months

### Sandbox after July incremental

- 2026-01 → 2026-07
- 7 distinct months

### Behavior

- Period-scoped delete + rebuild + append
- Uses RAW lookback for MoM / YoY
- Historical notebook hardcodes canonical DB names `raw_data` / `calc_data`

---

## Monthly CDM

### Monthly tables (15)

- `cdm_dim_month`
- `cdm_traffic_kpi`
- `cdm_overall_traffic_trend`
- `cdm_traffic_source_mix`
- `cdm_geo_traffic_kpi`
- `cdm_geo_traffic_trend`
- `cdm_geo_traffic_by_section`
- `cdm_organic_search_kpi`
- `cdm_organic_search_engine_mix`
- `cdm_organic_search_engine_trend`
- `cdm_explore_buying_guides_kpi`
- `cdm_explore_buying_guides_trend`
- `cdm_sections_mix`
- `cdm_top_pages`
- `cdm_device_section_mix`

### Coverage patterns

| Mart class | Historical frozen | Sandbox after July |
|---|---|---|
| Trend / calendar | 2025-01 → 2026-06 (18) | 2025-01 → 2026-07 (19) |
| Report / interactive | 2026-01 → 2026-06 (6) | 2026-01 → 2026-07 (7) |

Historical Monthly Excel-required CDM objects: **15 / 15 present**.

### Behavior

- Full Monthly rebuild semantics
- Deletes / repopulates Monthly CDM tables listed by the historical notebook
- Dynamic end month from `MAX(calc_monthly_kpi.month)` when end month is unset
- `cdm_dt` refreshed on rebuild
- Relative `month_type` labels shift when latest month advances

---

## Execution Sandbox

Purpose: allow controlled Monthly pipeline execution without writing to the frozen baseline.

Constraints used during validation:

- target host port must be sandbox (`15433`), never frozen (`15432`)
- private environment file for sandbox credentials (not historical `.env`)
- disposable working copies of notebooks for execution-only edits
- historical notebook archive remains immutable

Allowed working-copy-only corrections used during validation:

- explicit sandbox environment loading
- runtime safety assertions for host/port/DB names/period
- private token-cache path for Adobe auth cache
- `import os` for the historical CDM notebook defect

---

## June Reproduction Results

Controlled June 2026 reproduction in sandbox:

| Stage | Result |
|---|---|
| Adobe → RAW | PASS WITH SOURCE DRIFT |
| RAW → CALC | STRUCTURALLY REPRODUCED WITH EXPECTED UPSTREAM DRIFT |
| CALC → CDM | STRUCTURALLY REPRODUCED WITH EXPECTED UPSTREAM DRIFT |

Structural confirmation:

- schemas matched
- business key sets matched
- period scope matched
- row counts matched
- frozen baseline unchanged

Source drift note:

- Adobe re-pull of June produced small numeric differences vs historical snapshot
- observed primary numeric drift in that test was &lt;0.1%
- this value is **test evidence only**, not a platform DQ threshold or SLA

---

## July End-to-End Results

Incremental Monthly run for **2026-07-01** in sandbox:

| Stage | Result |
|---|---|
| Adobe → RAW July | Success |
| RAW → CALC July | Success |
| CALC → CDM full Monthly rebuild through July | Success |

Overall classification: **END-TO-END PASS WITH WARNINGS**

Validated checks:

- July present in Monthly RAW / CALC / expected CDM marts
- no duplicate business keys in validated Monthly July tables
- RAW earlier months unchanged (append-only)
- CALC Jan–Jun unchanged
- Weekly table fingerprints/row counts unchanged
- frozen baseline unchanged
- final Monthly CDM latest period = **2026-07-01**

Warnings:

1. Expected `month_type` relabeling on selected CDM mix tables when end month advanced
2. Known June upstream metric drift vs frozen remained visible and was not introduced by July

---

## Mutation Semantics

| Layer | Mutation model | Period safety expectation |
|---|---|---|
| RAW | Append new month; optional replace selected months | Non-selected months remain |
| CALC | Delete requested period then rebuild | Other CALC months remain |
| CDM Monthly | Full rebuild of Monthly marts | Coverage expands with CALC max; technical timestamps refresh; relative labels may shift |

Forbidden during baseline validation unless separately approved:

- writes to frozen baseline
- Weekly notebook execution as part of Monthly acceptance
- July/Adobe expansion beyond approved period
- Git publication of private data

---

## Data Quality Smoke Results

Minimal structural acceptance (not a formal DQ framework):

| Check | July result |
|---|---|
| Required July records present | Pass |
| Duplicate business keys | 0 |
| Obvious negatives in expected non-negative metrics | None observed |
| Share-like values within logical [0, 1] bounds where checked | Pass |

No permanent DQ product, thresholds, or SLA were introduced by this baseline.

---

## Security Constraints

- Secrets remain private and outside Git
- Historical `.env` not reused for sandbox runs
- Tokens / Adobe identifiers / page-level content must not appear in public docs or Git
- Real dumps and private notebooks remain outside the public project tree
- Public documentation must stay free of credentials and client-identifying values

---

## Baseline Acceptance

Accepted as the Monthly technical baseline for Portfolio Edition recovery:

1. Frozen historical restore is a valid immutable reference
2. Monthly historical inventory and coverage are confirmed
3. Sandbox can execute historical Monthly RAW → CALC → CDM logic
4. June reproduction is structurally accepted with known Adobe source drift
5. July incremental Monthly chain is accepted as end-to-end capable in sandbox
6. Weekly remains out of validated scope
7. Portfolio Edition packaging / orchestration / BI / formal DQ remain not implemented

---

## Known Limitations

1. Adobe upstream values are mutable over time; exact-value reproduction is not guaranteed
2. Historical CDM full-rebuild semantics are destructive to Monthly CDM contents in the target database
3. Historical CDM notebook requires `import os` (working-copy defect workaround only)
4. Canonical DB names are hardcoded in historical CALC/CDM notebooks
5. Weekly Organic is not part of this baseline acceptance
6. No Airflow DAG, formal DQ system, automated tests, or new BI surface is included in this baseline
7. Observed June numeric drift magnitude must not be generalized into a tolerance policy without a separate decision
