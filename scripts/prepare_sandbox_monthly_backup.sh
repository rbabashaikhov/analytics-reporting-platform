#!/usr/bin/env bash
# Prepare a private logical backup of the Monthly execution sandbox.
# NON-DESTRUCTIVE. Does not reset. Does not touch frozen :15432.
# Uses pg_dump inside arp-monthly-sandbox-postgres-1 (matches server major).
# Does not start/stop/recreate Docker services.
set -euo pipefail

PRIVATE_ROOT="${ARP_PRIVATE_ROOT:-$HOME/Documents/private-data/analytics-reporting-platform}"
ENV_FILE="${ARP_SANDBOX_ENV:-$PRIVATE_ROOT/.env.monthly-sandbox}"
CONTAINER="${ARP_SANDBOX_PG_CONTAINER:-arp-monthly-sandbox-postgres-1}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$PRIVATE_ROOT/backups/sandbox-pre-backfill-$STAMP"

python3 - "$ENV_FILE" "$OUT_DIR" "$CONTAINER" <<'PY'
import os, subprocess, sys
from pathlib import Path

env_file, out_dir, container = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
if not env_file.is_file():
    raise SystemExit(f"error=missing_env_file path={env_file}")
env = {}
for line in env_file.read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    k, v = line.split("=", 1)
    env[k] = v.strip().strip('"').strip("'")

out_dir.mkdir(parents=True, exist_ok=True)
os.chmod(out_dir, 0o700)
user = env["DB_USER"]
password = env["DB_PASSWORD"]
for db in ("raw_data", "calc_data", "cdm_data"):
    target = out_dir / f"{db}.dump"
    print(f"dumping db={db}")
    with open(target, "wb") as fh:
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-e",
                f"PGPASSWORD={password}",
                container,
                "pg_dump",
                "-U",
                user,
                "-d",
                db,
                "--format=custom",
            ],
            stdout=fh,
            stderr=subprocess.PIPE,
            check=False,
        )
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.decode(errors="replace")[:800])
    os.chmod(target, 0o600)

manifest = out_dir / "MANIFEST.txt"
manifest.write_text(
    "\n".join(
        [
            f"backup_dir={out_dir}",
            "purpose=pre-backfill-sandbox-backup",
            f"container={container}",
            "method=docker_exec_pg_dump_custom",
            "databases=raw_data,calc_data,cdm_data",
            "frozen_port=15432_NOT_INCLUDED",
            "note=logical dumps only; Docker volume not copied; non-destructive",
            "",
        ]
    )
)
os.chmod(manifest, 0o600)
print(f"backup_dir={out_dir}")
print("status=ok")
PY
