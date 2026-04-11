# Architecture

## System components

- External collector: fetches MLB source data in a separate environment.
- Bundle drop: writes normalized JSON bundles to a shared/imported location.
- Ingestion layer: validates bundle contract and writes canonical game/player lines to SQLite.
- Transform layer: computes team standings and player aggregate metrics.
- Dashboard layer: reads derived tables and exposes local analytics views.

## Data flow

1. Collector exports a bundle file.
2. `scripts/run_ingest.py` validates and imports bundle into local DB.
3. `scripts/run_derivations.py` refreshes derived stats.
4. Dashboard reads `team_stats` and `player_stats`.

## Design choices

- Offline-first guarantees operability without live network dependencies.
- SQLite keeps environment setup simple for local reproducibility.
- Idempotent import ledger prevents accidental duplicate bundle application.
