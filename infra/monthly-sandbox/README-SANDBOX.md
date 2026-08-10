# Monthly Execution Sandbox

## Purpose

Controlled, disposable PostgreSQL environment for legacy Monthly pipeline reproduction tests.

This is **not** the frozen historical baseline.

## Isolation

| Role | Compose project | Host port | Volume |
|---|---|---|---|
| Frozen baseline (read-only for execution work) | `arp-dwh-restore` | `127.0.0.1:15432` | `arp_dwh_pg14_data` |
| Monthly execution sandbox | `arp-monthly-sandbox` | `127.0.0.1:15433` | `arp_monthly_sandbox_pg14_data` |

Do **not** write to the frozen baseline during sandbox experiments.

## PostgreSQL

- Major: 14
- Skeleton image: `postgres:14`
- Prefer the same image digest/version as the frozen baseline for reproducibility

## Databases

Canonical names (required by historical CALC/CDM notebooks):

- `raw_data`
- `calc_data`
- `cdm_data`

## Private locations

- Secrets / `DB_*` mapping: private env file outside the repository
- Historical dumps: private backups directory, mounted read-only into the container as `/backups`

Repository `.env.example` contains placeholders only.

## Safety rules

- Sandbox is disposable
- Credentials are private and must not enter Git
- No Adobe secrets in repository files
- Notebooks are **not** executed by sandbox creation itself
- Historical notebooks and dumps remain untouched
- Do not open or copy historical `.env` files

## Planned start (after approval)

From the project root:

```bash
docker compose -p arp-monthly-sandbox \
  --env-file <PRIVATE_ENV_FILE> \
  -f infra/monthly-sandbox/docker-compose.yml \
  up -d
```

Do not execute legacy notebooks until a separate execution task is approved.
