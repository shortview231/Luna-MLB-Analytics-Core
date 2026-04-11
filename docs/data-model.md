# Data Model

## Core tables

- `games`: canonical per-game results from bundle imports.
- `game_players`: per-game player batting lines.
- `import_ledger`: ingested bundle IDs and timestamps.

## Derived tables

- `team_stats`: games played, W/L, runs scored/allowed, run diff, win pct.
- `player_stats`: aggregate AB/H/HR/RBI and batting average.

## Key relationships

- `game_players.game_id -> games.game_id`
- `games.source_bundle_id -> import_ledger.bundle_id`
