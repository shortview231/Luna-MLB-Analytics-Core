from __future__ import annotations

import sqlite3
from pathlib import Path


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Return a sqlite connection with rows addressable by column name."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS games (
            game_id TEXT PRIMARY KEY,
            game_date TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_runs INTEGER NOT NULL,
            away_runs INTEGER NOT NULL,
            source_bundle_id TEXT NOT NULL,
            imported_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS team_stats (
            team TEXT PRIMARY KEY,
            games_played INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            losses INTEGER NOT NULL,
            runs_scored INTEGER NOT NULL,
            runs_allowed INTEGER NOT NULL,
            run_diff INTEGER NOT NULL,
            win_pct REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS game_players (
            game_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            at_bats INTEGER NOT NULL,
            hits INTEGER NOT NULL,
            home_runs INTEGER NOT NULL,
            rbi INTEGER NOT NULL,
            PRIMARY KEY(game_id, player_id),
            FOREIGN KEY(game_id) REFERENCES games(game_id)
        );

        CREATE TABLE IF NOT EXISTS game_pitchers (
            game_id TEXT NOT NULL,
            player_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            ip_outs INTEGER NOT NULL,
            h_allowed INTEGER NOT NULL,
            er INTEGER NOT NULL,
            bb_allowed INTEGER NOT NULL,
            so_pitched INTEGER NOT NULL,
            hr_allowed INTEGER NOT NULL,
            pitches INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            era_game REAL,
            PRIMARY KEY(game_id, player_id),
            FOREIGN KEY(game_id) REFERENCES games(game_id)
        );

        CREATE TABLE IF NOT EXISTS player_stats (
            player_id TEXT PRIMARY KEY,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            at_bats INTEGER NOT NULL,
            hits INTEGER NOT NULL,
            home_runs INTEGER NOT NULL,
            rbi INTEGER NOT NULL,
            batting_avg REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS player_pitching_stats (
            player_id TEXT PRIMARY KEY,
            player_name TEXT NOT NULL,
            team TEXT NOT NULL,
            ip_outs INTEGER NOT NULL,
            h_allowed INTEGER NOT NULL,
            er INTEGER NOT NULL,
            bb_allowed INTEGER NOT NULL,
            so_pitched INTEGER NOT NULL,
            hr_allowed INTEGER NOT NULL,
            pitches INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            era REAL
        );

        CREATE TABLE IF NOT EXISTS import_ledger (
            bundle_id TEXT PRIMARY KEY,
            imported_at TEXT NOT NULL,
            game_count INTEGER NOT NULL
        );
        """
    )
    conn.commit()
