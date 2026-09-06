# MLB Bundle Specification

The ingestion workflow accepts folder-based MLB data bundles under:

`artifacts/inbox/mlb/<bundle_id>/`

Required files per bundle:

- `manifest.json`
- `schedule.json`
- `boxscores.json`

## Manifest contract

`manifest.json` must include:

- `bundle_id` as a stable unique string
- `generated_at_utc` as an ISO UTC timestamp
- SHA-256 checksums for the schedule and boxscore files

Checksums are validated against file bytes before import.

## Compatibility

The project supports:

- folder bundles containing a manifest, schedule, and boxscores
- a legacy single-file JSON fixture path for compatibility and testing

This specification defines the complete input contract needed to reproduce the ingestion behavior demonstrated by this repository.
