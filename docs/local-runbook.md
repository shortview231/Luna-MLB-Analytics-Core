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

## Launch dashboard

```bash
streamlit run src/luna_mlb_analytics/dashboard/app.py
```

## Quality checks

```bash
make lint
make test
```
