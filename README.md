# Analytics Reporting Platform

Monthly analytics ETL/DWH platform built with Python, PostgreSQL, and Apache Airflow.

## Overview

Portfolio Edition of a Monthly analytics reporting platform: a recovered and hardened rebuild of an existing analytics automation MVP, packaged as a reproducible operational pipeline.

```text
Adobe Analytics API
        ↓
Apache Airflow
        ↓
PostgreSQL RAW
        ↓
CALC
        ↓
CDM
        ↓
Data Quality / BI consumers
```

Scope is **Monthly only**. Client-specific identifiers, datasets, and KPI values are kept outside Git.

## Problem

Monthly analytics reporting was produced through a partly notebook-driven, manual workflow. The goal was to turn that into a reproducible, operational pipeline with clear layers, orchestration, and automated data quality — without inventing new business metrics.

## Solution

- Python package instead of notebook-only ETL
- PostgreSQL layered model: **RAW → CALC → CDM**
- Apache Airflow orchestration (`monthly_private_pipeline`)
- Monthly **incremental** execution for missing completed months
- Manual **rebuild** for a selected period
- Automatic data quality after ETL
- Read-only **validate** mode (no Adobe / API / DWH writes)
- Idempotent stage execution with replace/skip semantics
- Adobe API throttling and bounded retry handling
- Private runtime configuration via env + client YAML (not in Git)

## Pipeline

```text
determine_periods
        →
load_raw
        →
build_calc
        →
build_cdm
        →
data_quality
        →
final_status
```

| Task | Role |
|---|---|
| `determine_periods` | Resolves closed months to process from mode and DWH state |
| `load_raw` | Sequential Adobe extraction into Monthly RAW marts |
| `build_calc` | Builds Monthly CALC marts from RAW |
| `build_cdm` | Builds Monthly CDM marts from CALC |
| `data_quality` | Runs the Monthly DQ suite and persists validation artifacts |
| `final_status` | Summarizes run outcome; critical DQ FAIL fails the DAG |

## Execution modes

| Mode | Behavior |
|---|---|
| **incremental** | Missing completed months → ETL → automatic DQ |
| **rebuild** | Manual selected period → ETL → automatic DQ |
| **validate** | No Adobe / API / DWH writes → read-only DQ only |

The current incomplete calendar month is never processed. Concurrent DAG runs are limited (`max_active_runs=1`).

## Data model

| Layer | Confirmed Monthly marts |
|---|---:|
| **RAW** | 3 |
| **CALC** | 5 |
| **CDM** | 15 |

Layers are client-neutral in public docs: RAW holds Adobe extracts, CALC holds derived KPI / mix / share tables, CDM holds report-ready marts for BI consumers.

## Data Quality

The Monthly suite covers:

- period coverage
- contracts
- uniqueness
- numeric ranges
- RAW → CALC reconciliation
- CALC → CDM reconciliation
- report contract
- latest completed month
- Monthly-only guard

Statuses: **PASS** / **WARN** / **FAIL**.

Critical **FAIL** fails the Airflow `data_quality` task (and therefore the DAG). WARN does not fail the run by itself.

## Validation artifacts

Persistent, human-readable and machine-readable reports:

```text
<validation-root>/
  monthly_private_pipeline/
    <run-id>/
      validation.md
      validation.json
```

Artifact root is configurable through `ARP_VALIDATION_ARTIFACT_ROOT` (private path outside Git).

## Observability

Airflow provides DAG/task state, including the `data_quality` task. Each DQ run produces a human-readable validation summary plus persistent MD/JSON artifacts.

A notification abstraction exists; the default notifier is **NoOp**. Telegram / email / Slack integrations can be added later without changing DQ outcomes.

## Reliability

- Sequential Adobe extraction (no parallel Adobe pulls)
- Bounded retries with `429` Retry-After and transient `5xx` handling
- Network timeout handling
- `max_active_runs=1`
- Closed-month processing only

## Security

- No secrets in Git
- Public templates only: `.env.example`, `config/client-config.example.yaml`
- Airflow runtime / env configuration stays private
- No client datasets, dumps, real page URLs, or KPI extracts in the repository

## Project structure

```text
.
├── dags/
│   └── monthly_private_pipeline.py
├── src/analytics_reporting_platform/
│   ├── raw/              # Monthly RAW extract + persistence
│   ├── calc/             # Monthly CALC builds
│   ├── cdm/              # Monthly CDM builds
│   ├── execution/        # Period planning / run models
│   ├── orchestration/    # Package-level pipeline stages
│   ├── validation/       # DQ suite + artifacts + notifier
│   ├── sources/adobe/    # Adobe client / reports / retries
│   ├── cli/              # CLI entrypoints
│   ├── config/           # Settings + client config loaders
│   └── db/               # Engine / health helpers
├── tests/
├── config/               # Public example client YAML
├── docs/
├── infra/                # Isolated local PostgreSQL restore/sandbox helpers
├── .env.example
└── pyproject.toml
```

## Local configuration

1. Copy `.env.example` to a private `.env` (never commit).
2. Copy `config/client-config.example.yaml` to a private client config path.
3. Point Airflow / CLI at those private paths via env or Airflow Variables (`ARP_ENV_FILE`, `ARP_CLIENT_CONFIG`, `ARP_VALIDATION_ARTIFACT_ROOT`).

Required categories in `.env.example`: PostgreSQL connection, Adobe OAuth placeholders, validation artifact root. Leave example values empty.

## Testing

Unit tests cover package foundations, Monthly RAW/CALC/CDM builders, execution planning, Airflow orchestration helpers, and validation/DQ behavior. Integration tests are reserved for private runtime environments.

Latest local run (this repository):

```text
212 passed, 7 skipped
```

## Current status

### Confirmed

- Monthly pipeline implemented
- Live Adobe → RAW → CALC → CDM E2E executed successfully
- Manual Airflow read-only validation executed successfully
- DQ final status **PASS**

### Portfolio Edition additions

- Package refactor (RAW / CALC / CDM / execution / orchestration / validation)
- CLI
- Airflow DAG
- Automatic DQ after ETL
- Persistent validation artifacts
- Notifier extension point (default NoOp)

### Roadmap

- BI layer
- Anonymized demo dataset
- CI/CD (not yet in this repository)
- Optional notification integrations

See `docs/current-state.md` for a fuller freeze of legacy vs recovered vs Portfolio Edition state.
