# Offline Ingestion Flow

Primary path:

`structured bundle -> inbox -> receiver -> SQLite -> derivations -> dashboard`

## Bundle contract

Folder bundle required files:

- `manifest.json`
- `schedule.json`
- `boxscores.json`

Reference:

- `ingestion/bundle_spec.md`

## Receiver workflow

1. Place a valid bundle folder in `artifacts/inbox/mlb/`.
2. Run the receiver:

```bash
python3 scripts/receive_mlb_inbox.py --db mlb_analytics.sqlite
```

3. The receiver validates checksums, imports new bundle IDs, computes derived statistics, and archives accepted inputs.
4. Failed bundles move to quarantine with reason metadata.
5. Run events are appended to a JSONL log for inspection.

## Legacy fixture workflow

A single-file JSON import remains available for compatibility and reproducible testing:

```bash
python3 scripts/run_ingest.py --bundle data/fixtures/bundles/sample_boxscore_bundle.json --db mlb_analytics.sqlite
python3 scripts/run_derivations.py --db mlb_analytics.sqlite
```

This document covers the complete input and processing flow needed to reproduce the repository's demonstrated analytics behavior.
