# Local Runbook

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the reproducibility checks

```bash
make smoke-ingest
make smoke-derive
make smoke-dashboard
```

## Process a structured inbox bundle

```bash
make receive-inbox DB=mlb_analytics.sqlite
```

Dry-run validation:

```bash
PYTHONPATH=src python3 scripts/receive_mlb_inbox.py --db mlb_analytics.sqlite --dry-run
```

Force reprocessing for an already-imported fixture or bundle ID when testing:

```bash
PYTHONPATH=src python3 scripts/receive_mlb_inbox.py --db mlb_analytics.sqlite --force-reprocess
```

## Refresh demonstration visuals

The analytics project accepts structured input bundles that satisfy the documented repository contract.

```bash
make refresh-public-assets BUNDLE=/path/to/mlb_bundle.json
```

This regenerates demonstration artifacts under `docs/proof/`.

## Launch the dashboard

```bash
streamlit run src/luna_mlb_analytics/dashboard/app.py
```

The module path retains the project's original package name for compatibility.

## Quality checks

```bash
make lint
make test
```

## Inspect local processing state

```bash
find artifacts/inbox/mlb -mindepth 1 -maxdepth 1 -type d | sort
find artifacts/archive/mlb -mindepth 1 -maxdepth 1 -type d | sort
find artifacts/quarantine/mlb -mindepth 1 -maxdepth 1 -type d | sort
tail -n 50 artifacts/logs/mlb/receiver_runs.jsonl
```

This runbook is limited to reproducing the behavior contained in this repository.
