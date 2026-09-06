# 2026-04-14 Dashboard Action-Line Upgrade

## Summary

This update expanded the local MLB dashboard from stat-table-only box scores to richer game context derived from structured boxscore payloads.

The goal was to make the analytical view more useful by surfacing batting, pitching, and game-note context while preserving deterministic offline processing.

## What changed

- Added ingestion support for team action lines from structured boxscore fields.
- Added storage for team action lines, team notes, player summaries, and global game notes.
- Updated the local analytical warehouse builder to expose those records to dashboard queries.
- Updated the box-score modal to render away/home action columns, team notes, player summaries, and global notes.
- Added integration-test coverage for the expanded ingestion path.

## Verification

- Integration tests passed with the added fields.
- Existing standings, scores, and aggregate-stat paths remained intact.
- Action-line output was validated against the repository's structured fixture data.

## Public pipeline context

```text
structured bundle
  -> validation and import
  -> derivations
  -> local analytical storage
  -> dashboard
```

The dashboard remains offline-first and does not require live API calls during local analysis.

## Portfolio value

This change demonstrates schema extension, backward-compatible ingestion, relational storage design, integration testing, and presentation of richer analytical context from structured source data.