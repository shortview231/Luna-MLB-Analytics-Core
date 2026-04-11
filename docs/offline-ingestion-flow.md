# Offline Ingestion Flow

## Contract

Bundle shape:
- `bundle_id`
- `generated_at`
- `games[]` with game metadata and `players[]` lines

## Workflow

1. External collector publishes bundle JSON.
2. Operator copies bundle into local environment.
3. Run import command:

```bash
python3 scripts/run_ingest.py --bundle data/fixtures/bundles/sample_boxscore_bundle.json --db luna_mlb.sqlite
```

4. Import validates schema and writes game/player rows.
5. Import ledger records `bundle_id` and blocks duplicate re-imports.

## Failure handling

- Schema violations fail fast with explicit key-level errors.
- Duplicate bundle IDs return `already_imported` and make no data mutations.
