# MLB Inbox Receiver Workflow

## State model

- Inbox: `artifacts/inbox/mlb/<bundle_id>/`
- Archive: `artifacts/archive/mlb/<bundle_id>/`
- Quarantine: `artifacts/quarantine/mlb/<bundle_id>/`
- Run log: `artifacts/logs/mlb/receiver_runs.jsonl`
- Lock file: `artifacts/state/mlb/receiver.lock`

Bundle-level metadata:
- Archive receipt: `_receipt.json`
- Quarantine reason: `_quarantine.json`

## Receiver flow

1. Acquire non-blocking lock (`receiver.lock`) to prevent concurrent runs.
2. Scan inbox bundle directories in deterministic name order.
3. Detect `bundle_id` from `manifest.json` (fallback: folder name).
4. Idempotency check against `import_ledger`.
5. For new bundles:
   - Validate required files and checksums via importer.
   - Import games/player lines into sqlite.
   - Run derivations (`team_stats`, `player_stats`).
   - Archive bundle folder atomically.
6. For already-imported bundles:
   - Mark as already imported.
   - Archive folder to clear inbox.
7. On failure:
   - Move bundle to quarantine atomically.
   - Write reason metadata.
8. Append JSONL run event with status and counts.

## Idempotency and force behavior

- Default: bundle IDs already in `import_ledger` are skipped and archived.
- `--force-reprocess`: purge prior rows for the same `bundle_id`, then re-import.
- `--dry-run`: scan/check idempotency only; keeps bundles in inbox, no DB writes, no moves.

## Operator output

Receiver emits a summary JSON payload containing:
- `processed`, `imported`, `already_imported`, `failed`
- per-bundle stage status
- inserted row counts
- archive/quarantine destination paths
