# luna-mlb-analytics

Offline-first MLB analytics pipeline that imports external bundle drops, computes reproducible boxscore-derived metrics, and serves a local dashboard for team/player insights.

## Why this repo exists

This project demonstrates a practical analytics architecture where data collection can happen outside the runtime environment and analysis remains reproducible locally.

Pipeline:

`external collector -> bundle drop -> local import -> derivations -> dashboard`

## MVP scope

Included:
- Boxscore-first offline ingestion from JSON bundle drops
- Derived standings plus team/player aggregate stats
- Local SQLite-backed analytics storage
- Local Streamlit dashboard
- Reproducible fixtures and tests

Out of scope for MVP:
- Live collector services
- Cloud deployment and multi-user auth
- Forecasting/predictive modeling

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

make smoke-ingest
make smoke-derive
make smoke-dashboard
streamlit run src/luna_mlb_analytics/dashboard/app.py
```

## Reproducibility

- Sample bundle: `data/fixtures/bundles/sample_boxscore_bundle.json`
- Expected smoke output baseline: `data/fixtures/expected/expected_summary.json`
- Unit/integration tests validate schema, ingestion, and derivation outputs.

## Documentation

- `docs/architecture.md`
- `docs/data-model.md`
- `docs/offline-ingestion-flow.md`
- `docs/local-runbook.md`
- `docs/mvp-vs-production.md`
- `docs/roadmap.md`

## Resume/portfolio positioning

- Highlights offline-first ingestion design and deterministic local derivations.
- Demonstrates schema contract discipline and analytics reproducibility.
- Shows practical end-to-end ownership: ingest, model, transform, and surface insights.

## License

MIT
