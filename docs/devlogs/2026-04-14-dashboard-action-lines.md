# 2026-04-14 Dashboard Action-Line Upgrade

## Summary

Tonight we upgraded the MLB dashboard box score experience from stat-table-only to real game-action context sourced directly from bundle payloads.

Goal: make the modal feel like a real sports app using actual game text (HR/RBI/TB/RISP/LOB lines, notes, and player summaries), while keeping the offline-first architecture intact.

## What Changed

- Added ingestion support for team action lines from boxscore payload:
  - `teams.home/away.info[].fieldList[]`
  - `teams.home/away.note[]`
  - player `stats.batting.summary` and `stats.pitching.summary`
  - global `boxscore.info[]` and `boxscore.pitchingNotes[]`
- Added new sqlite storage tables:
  - `game_team_action_lines`
  - `game_team_notes`
  - `game_player_summaries`
  - `game_global_notes`
- Updated warehouse builder to load those tables into DuckDB for dashboard queries.
- Updated box score modal UI to render:
  - Away/Home action columns
  - Team notes
  - Player summaries
  - Global game notes
- Added integration test coverage for the new action-line ingest path.

## Verification

- Integration tests passed (`pytest`).
- Existing standings/scores/stats paths remained intact.
- Action lines now display real payload text with season totals where provided in the source string.

## Operational Notes

- Pipeline remains: `Luna_Ingestion -> folder bundle drop -> Luna_Export receiver/import -> derive -> warehouse -> dashboard`.
- No live/API calls added to dashboard runtime.
- Push remains manual by design; local auto-refresh is still available for private daily updates.

## Next Focus

- Improve visual layout density for action blocks on desktop and mobile.
- Add optional filtering for action sections (Batting/Pitching/Notes).
- Continue validating data freshness from latest archived/inbox bundles before publish pushes.
