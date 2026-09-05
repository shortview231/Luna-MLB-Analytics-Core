# MLB Analytics Pipeline

An offline-first baseball analytics project that imports structured data bundles, validates them, computes reproducible metrics, stores analytical state locally, and serves team and player insights through a dashboard.

## Project goal

The project demonstrates an end-to-end data workflow where collection and analysis are cleanly separated. The analytics side can be rerun locally from structured inputs without depending on a live external service.

```text
structured data input
  -> validation
  -> import
  -> deterministic derivations
  -> local analytical storage
  -> dashboard
```

## What it demonstrates

- structured JSON ingestion
- schema and checksum validation
- idempotent processing
- SQLite-backed analytical storage
- reproducible team and player aggregations
- archive and quarantine handling for processed inputs
- deterministic fixtures and tests
- local Streamlit dashboarding
- documented failure and recovery behavior

## MVP scope

Included:

- boxscore-first ingestion from structured bundle drops
- derived standings and team/player aggregate statistics
- local SQLite-backed storage
- dashboard views for baseball analysis
- reproducible fixtures and validation tests

Not included:

- live data-collection services
- cloud deployment
- multi-user authentication
- production forecasting services

## Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

make smoke-ingest
make smoke-derive
make smoke-dashboard
make receive-inbox
streamlit run src/luna_mlb_analytics/dashboard/app.py
```

## Reproducibility

The repository includes sample bundle data and expected outputs so ingestion and derivation behavior can be tested consistently.

Key examples include:

- `data/fixtures/bundles/sample_boxscore_bundle.json`
- `data/fixtures/expected/expected_summary.json`
- unit and integration tests for ingestion, schema validation, and derivation output

## Reliability practices

The ingestion workflow is designed to make failures visible and repeated runs safe.

- already-imported bundle IDs are recognized instead of silently duplicated
- malformed inputs can be separated from accepted inputs
- run events can be logged for later inspection
- dry-run behavior supports validation without writing data
- deterministic processing makes failures easier to reproduce

## Documentation

The repository includes notes covering:

- architecture
- data model
- ingestion behavior
- receiver workflow
- local operation
- MVP boundaries
- proof artifacts

## Skills demonstrated

- Python data engineering
- structured-data validation
- SQLite and analytical modeling
- ETL-style ingestion
- idempotent workflow design
- testing and reproducibility
- failure handling and recovery
- analytical dashboard development
- technical documentation

## Portfolio positioning

This project is intended as a standalone example of reliable analytics engineering: accepting structured data, validating it, transforming it deterministically, preserving inspectable state, and presenting useful analytical outputs without hiding operational failure modes.

## License

MIT