# Isolated PostgreSQL DWH Restore Environment

## Purpose

Historical recovery / baseline environment for **Analytics Reporting Platform — Portfolio Edition**.

This stack restores the historical DWH dumps into an isolated PostgreSQL 14 instance.

**THIS ENVIRONMENT IS FOR RECOVERY / BASELINE.**  
**DO NOT USE AIRFLOW METADATA POSTGRESQL AS DWH.**

Also do not reuse host PostgreSQL, Superset, or DataLens PostgreSQL instances as the DWH.

## Status

No PostgreSQL container has been started yet.  
No restore has been executed yet.

## Target databases (to be created later)

- `raw_data`
- `calc_data`
- `cdm_data`

## PostgreSQL

- Major version: **14**
- Skeleton image: `postgres:14`
- After a successful baseline restore, pin the exact image version/digest for reproducibility.

## Access

- Host bind: `127.0.0.1:15432` → container `5432`
- Localhost only (no `0.0.0.0` publish)

## Compose

- Project name: `arp-dwh-restore`
- Service: `postgres`
- Named volume: `arp_dwh_pg14_data`
- Network: `arp_dwh_restore_net` (dedicated; not attached to Airflow/Superset/DataLens networks)

## Private locations

- Dumps (read-only mount source):  
  `~/Documents/private-data/analytics-reporting-platform/backups/`
- Secrets file (not in Git):  
  `~/Documents/private-data/analytics-reporting-platform/.env.dwh-restore`

Repository `.env.example` contains variable names only.

## Planned start (NOT executed yet)

From the project root, after explicit approval:

```bash
docker compose -p arp-dwh-restore \
  --env-file "$HOME/Documents/private-data/analytics-reporting-platform/.env.dwh-restore" \
  -f infra/dwh-restore/docker-compose.yml \
  up -d
```

Do not run this until approved. Do not restore dumps until a separate restore task is approved.
