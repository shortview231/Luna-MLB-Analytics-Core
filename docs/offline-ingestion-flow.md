# Offline Ingestion Flow

Primary path:

`luna_ingestion -> artifacts/inbox/mlb/<bundle_id>/ -> receiver -> sqlite -> derivations -> dashboard`

## Bundle contract

Folder bundle required files:
- `manifest.json`
- `schedule.json`
- `boxscores.json`

Reference:
- `docs/ingestion/luna_bundle_spec.md`

## Receiver-first workflow

1. Upstream writes bundle folders into `artifacts/inbox/mlb/`.
2. Run receiver:

```bash
python3 scripts/receive_mlb_inbox.py --db luna_mlb.sqlite
```

3. Receiver validates checksums, imports new bundles, derives stats, and archives bundles.
4. Failed bundles move to quarantine with reason metadata.
5. Receiver appends per-bundle run events to JSONL log.

## Legacy compatibility workflow

Single-file JSON import remains supported temporarily:

```bash
python3 scripts/run_ingest.py --bundle data/fixtures/bundles/sample_boxscore_bundle.json --db luna_mlb.sqlite
python3 scripts/run_derivations.py --db luna_mlb.sqlite
```

Deprecation trigger:
- Remove legacy path after stable daily folder-bundle runs and no active consumers.
