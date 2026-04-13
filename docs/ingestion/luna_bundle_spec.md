# Luna Bundle Spec (Folder-Based)

This repo receives MLB bundles from `luna_ingestion` in:

`artifacts/inbox/mlb/<bundle_id>/`

Required files per bundle folder:
- `manifest.json`
- `schedule.json`
- `boxscores.json`

## Manifest contract

`manifest.json` must include:
- `bundle_id` (string, stable unique ID)
- `generated_at_utc` (ISO UTC timestamp)
- `files.schedule.sha256`
- `files.boxscores.sha256`

Checksums are validated against file bytes before import.

## Compatibility

Current ingestion supports both:
- Folder bundles (`manifest + schedule + boxscores`) - preferred and active path
- Legacy single-file JSON bundle - temporary compatibility path

Deprecation guidance:
- Keep legacy path until 14 consecutive days of successful folder-bundle daily runs.
- Remove legacy path only after runbook and scheduler automation are stable.
