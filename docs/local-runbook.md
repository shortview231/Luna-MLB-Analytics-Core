# Local Runbook

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run pipeline

```bash
make smoke-ingest
make smoke-derive
make smoke-dashboard
```

## Receive daily inbox bundles

```bash
make receive-inbox DB=luna_mlb.sqlite
```

Dry-run validation:

```bash
PYTHONPATH=src python3 scripts/receive_mlb_inbox.py --db luna_mlb.sqlite --dry-run
```

Force reprocess for an already-imported bundle ID:

```bash
PYTHONPATH=src python3 scripts/receive_mlb_inbox.py --db luna_mlb.sqlite --force-reprocess
```

## Refresh public visuals from a Luna export

This repo does not fetch live data directly. It consumes bundles pushed from luna_ingestion.

```bash
make refresh-public-assets BUNDLE=/path/to/luna_exported_bundle.json
```

This regenerates:
- `docs/proof/standings_latest.png`
- `docs/proof/player_stats_latest.png`
- `docs/proof/latest.json`

## Launch dashboard

```bash
streamlit run src/luna_mlb_analytics/dashboard/app.py
```

## Quality checks

```bash
make lint
make test
```

## Inspect inbox/archive/quarantine quickly

```bash
find artifacts/inbox/mlb -mindepth 1 -maxdepth 1 -type d | sort
find artifacts/archive/mlb -mindepth 1 -maxdepth 1 -type d | sort | tail -n 20
find artifacts/quarantine/mlb -mindepth 1 -maxdepth 1 -type d | sort
tail -n 50 artifacts/logs/mlb/receiver_runs.jsonl
```

## Daily scheduler recommendation

Cron (local, lightweight):

```cron
15 6 * * * cd <repo_root> && PYTHONPATH=src /usr/bin/python3 scripts/receive_mlb_inbox.py --db luna_mlb.sqlite >> artifacts/logs/mlb/receiver_cron.log 2>&1
```

Systemd timer (preferred for long-term local ops):
- Service executes `scripts/receive_mlb_inbox.py`
- Timer runs once daily after upstream bundle push window
