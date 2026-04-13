# Architecture

## System components

- External collector: fetches MLB source data in a separate environment.
- Bundle drop: writes normalized JSON bundles to a shared/imported location.
- Ingestion layer: validates bundle contract and writes canonical game/player lines to SQLite.
- Transform layer: computes team standings and player aggregate metrics.
- Dashboard layer: reads derived tables and exposes local analytics views.

## Data flow

1. Collector exports a bundle and pushes directly to this repo inbox.
2. `scripts/receive_mlb_inbox.py` scans inbox, validates bundle checksums, and imports new bundle IDs into local DB.
3. Receiver runs derivations and atomically moves bundle to archive.
4. Failed bundles move to quarantine with reason metadata.
5. Dashboard reads `team_stats` and `player_stats`.

## Design choices

- Offline-first guarantees operability without live network dependencies.
- SQLite keeps environment setup simple for local reproducibility.
- Idempotent import ledger prevents accidental duplicate bundle application.
- Receiver lock file prevents overlapping local runs.
- JSONL receiver logs provide low-ceremony operational visibility.
