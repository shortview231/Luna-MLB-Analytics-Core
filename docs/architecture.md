# Architecture

## Repository components

- Structured input bundle: a documented JSON bundle satisfying the repository contract.
- Ingestion layer: validates the bundle contract and writes canonical game/player lines to SQLite.
- Transform layer: computes team standings and player aggregate metrics.
- Dashboard layer: reads derived tables and exposes local analytics views.

## Data flow

1. A valid structured bundle is placed in the local repository inbox.
2. `scripts/receive_mlb_inbox.py` scans the inbox, validates bundle checksums, and imports new bundle IDs into the local database.
3. The receiver runs derivations and atomically moves accepted bundles to the archive.
4. Failed bundles move to quarantine with reason metadata.
5. The dashboard reads `team_stats` and `player_stats`.

## Design choices

- Offline-first operation keeps the demonstrated analytics workflow reproducible without a live network dependency.
- SQLite keeps environment setup simple for local reproduction.
- An idempotent import ledger prevents accidental duplicate bundle application.
- A receiver lock file prevents overlapping local runs.
- JSONL receiver logs provide lightweight operational visibility.

The repository boundary starts with the documented structured bundle and contains everything required to reproduce the demonstrated ingestion, derivation, storage, and dashboard behavior.
