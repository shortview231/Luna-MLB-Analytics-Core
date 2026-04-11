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

## Refresh public visuals from a Luna export

This repo does not fetch live data directly. It only consumes exported bundles.

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
