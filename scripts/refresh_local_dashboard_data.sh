#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/home/robertsory/Desktop/Projects/Luna_Export"
cd "$REPO_ROOT"

mkdir -p artifacts/logs/mlb

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] local refresh start"
  PYTHONPATH=src python3 scripts/receive_mlb_inbox.py --db luna_mlb.sqlite
  PYTHONPATH=src python3 scripts/run_derivations.py --db luna_mlb.sqlite
  python3 scripts/build_dashboard_warehouse.py
  echo "[$(date '+%Y-%m-%d %H:%M:%S %Z')] local refresh done"
} >> artifacts/logs/mlb/local_dashboard_refresh.log 2>&1
