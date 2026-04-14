#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from datetime import datetime, UTC
from pathlib import Path

import duckdb

SQLITE_DB = Path("luna_mlb.sqlite")
DUCKDB_DB = Path("data/warehouse/mlb_core.duckdb")

TEAM_MAP = {
    "ARI": (109, "Arizona Diamondbacks"),
    "ATL": (144, "Atlanta Braves"),
    "BAL": (110, "Baltimore Orioles"),
    "BOS": (111, "Boston Red Sox"),
    "CHC": (112, "Chicago Cubs"),
    "CIN": (113, "Cincinnati Reds"),
    "CLE": (114, "Cleveland Guardians"),
    "COL": (115, "Colorado Rockies"),
    "DET": (116, "Detroit Tigers"),
    "HOU": (117, "Houston Astros"),
    "KC": (118, "Kansas City Royals"),
    "LAA": (108, "Los Angeles Angels"),
    "LAD": (119, "Los Angeles Dodgers"),
    "MIA": (146, "Miami Marlins"),
    "MIL": (158, "Milwaukee Brewers"),
    "MIN": (142, "Minnesota Twins"),
    "NYM": (121, "New York Mets"),
    "NYY": (147, "New York Yankees"),
    "ATH": (133, "Athletics"),
    "PHI": (143, "Philadelphia Phillies"),
    "PIT": (134, "Pittsburgh Pirates"),
    "SD": (135, "San Diego Padres"),
    "SEA": (136, "Seattle Mariners"),
    "SF": (137, "San Francisco Giants"),
    "STL": (138, "St. Louis Cardinals"),
    "TB": (139, "Tampa Bay Rays"),
    "TEX": (140, "Texas Rangers"),
    "TOR": (141, "Toronto Blue Jays"),
    "CWS": (145, "Chicago White Sox"),
    "WSH": (120, "Washington Nationals"),
    "AZ": (109, "Arizona Diamondbacks"),
}


def _team_info(code: str) -> tuple[int, str]:
    code = (code or "").strip().upper()
    if code in TEAM_MAP:
        return TEAM_MAP[code]
    return (0, code or "UNK")


def build() -> None:
    if not SQLITE_DB.exists():
        raise SystemExit(f"Missing sqlite db: {SQLITE_DB}")

    DUCKDB_DB.parent.mkdir(parents=True, exist_ok=True)
    sq = sqlite3.connect(str(SQLITE_DB))
    sq.row_factory = sqlite3.Row

    dd = duckdb.connect(str(DUCKDB_DB))
    dd.execute("DROP TABLE IF EXISTS games")
    dd.execute("DROP TABLE IF EXISTS team_game_results")
    dd.execute("DROP TABLE IF EXISTS player_game_batting")
    dd.execute("DROP TABLE IF EXISTS player_game_pitching")
    dd.execute("DROP TABLE IF EXISTS team_season_aggregates")
    dd.execute("DROP TABLE IF EXISTS player_season_aggregates")

    dd.execute(
        """
        CREATE TABLE games (
          game_pk BIGINT,
          game_date DATE,
          season INTEGER,
          status TEXT,
          home_team_id INTEGER,
          home_team_name TEXT,
          away_team_id INTEGER,
          away_team_name TEXT,
          home_score INTEGER,
          away_score INTEGER,
          venue_id INTEGER,
          venue_name TEXT,
          start_time_utc TIMESTAMP,
          source_payload_hash TEXT,
          last_ingested_at TIMESTAMP
        )
        """
    )

    dd.execute(
        """
        CREATE TABLE team_game_results (
          game_pk BIGINT,
          team_id INTEGER,
          team_name TEXT,
          opponent_team_id INTEGER,
          is_home BOOLEAN,
          win_flag INTEGER,
          loss_flag INTEGER,
          runs_scored INTEGER,
          runs_allowed INTEGER,
          hits INTEGER,
          errors INTEGER,
          left_on_base INTEGER,
          team_ops_game DOUBLE,
          team_era_game DOUBLE,
          last_ingested_at TIMESTAMP
        )
        """
    )

    dd.execute(
        """
        CREATE TABLE player_game_batting (
          game_pk BIGINT,
          team_id INTEGER,
          player_id BIGINT,
          player_name TEXT,
          batting_order TEXT,
          position TEXT,
          ab INTEGER,
          r INTEGER,
          h INTEGER,
          rbi INTEGER,
          bb INTEGER,
          so INTEGER,
          hr INTEGER,
          doubles INTEGER,
          triples INTEGER,
          sb INTEGER,
          cs INTEGER,
          hbp INTEGER,
          sf INTEGER,
          obp_game DOUBLE,
          slg_game DOUBLE,
          ops_game DOUBLE,
          last_ingested_at TIMESTAMP
        )
        """
    )

    dd.execute(
        """
        CREATE TABLE player_game_pitching (
          game_pk BIGINT,
          team_id INTEGER,
          player_id BIGINT,
          player_name TEXT,
          ip_outs INTEGER,
          h_allowed INTEGER,
          er INTEGER,
          bb_allowed INTEGER,
          so_pitched INTEGER,
          hr_allowed INTEGER,
          pitches INTEGER,
          strikes INTEGER,
          decision TEXT,
          era_game DOUBLE,
          last_ingested_at TIMESTAMP
        )
        """
    )

    dd.execute(
        """
        CREATE TABLE team_season_aggregates (
          season INTEGER,
          team_id INTEGER,
          team_name TEXT,
          games_played INTEGER,
          wins INTEGER,
          losses INTEGER,
          runs_scored INTEGER,
          runs_allowed INTEGER,
          run_differential INTEGER,
          team_ops DOUBLE,
          team_era DOUBLE,
          streak TEXT,
          last_10 TEXT,
          next_game BIGINT,
          next_game_time TIMESTAMP,
          as_of_date DATE
        )
        """
    )

    dd.execute(
        """
        CREATE TABLE player_season_aggregates (
          season INTEGER,
          player_id BIGINT,
          team_id INTEGER,
          player_name TEXT,
          games_played INTEGER,
          ab INTEGER,
          r INTEGER,
          h INTEGER,
          rbi INTEGER,
          bb INTEGER,
          so INTEGER,
          hr INTEGER,
          doubles INTEGER,
          triples INTEGER,
          sb INTEGER,
          cs INTEGER,
          hbp INTEGER,
          sf INTEGER,
          ops DOUBLE,
          ip_outs INTEGER,
          er INTEGER,
          h_allowed INTEGER,
          bb_allowed INTEGER,
          so_pitched INTEGER,
          hr_allowed INTEGER,
          era DOUBLE,
          as_of_date DATE
        )
        """
    )

    game_rows = sq.execute(
        """
        SELECT game_id, game_date, home_team, away_team, home_runs, away_runs, imported_at
        FROM games
        ORDER BY game_date, game_id
        """
    ).fetchall()

    team_rows = []
    for g in game_rows:
        home_id, home_name = _team_info(g["home_team"])
        away_id, away_name = _team_info(g["away_team"])
        season = int(str(g["game_date"])[:4])
        try:
            game_pk = int(str(g["game_id"]).replace("g-", ""))
        except ValueError:
            continue
        imported_at = g["imported_at"] or datetime.now(UTC).isoformat()
        dd.execute(
            """
            INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                game_pk,
                g["game_date"],
                season,
                "Final",
                home_id,
                home_name,
                away_id,
                away_name,
                int(g["home_runs"]),
                int(g["away_runs"]),
                None,
                None,
                None,
                None,
                imported_at,
            ],
        )

        away_hits = sq.execute(
            "SELECT COALESCE(SUM(hits),0) FROM game_players WHERE game_id=? AND team=?",
            (g["game_id"], g["away_team"]),
        ).fetchone()[0]
        home_hits = sq.execute(
            "SELECT COALESCE(SUM(hits),0) FROM game_players WHERE game_id=? AND team=?",
            (g["game_id"], g["home_team"]),
        ).fetchone()[0]

        home_win = 1 if int(g["home_runs"]) > int(g["away_runs"]) else 0
        away_win = 1 if int(g["away_runs"]) > int(g["home_runs"]) else 0

        team_rows.append(
            [
                game_pk,
                away_id,
                away_name,
                home_id,
                False,
                away_win,
                1 - away_win,
                int(g["away_runs"]),
                int(g["home_runs"]),
                int(away_hits),
                0,
                None,
                None,
                None,
                imported_at,
            ]
        )
        team_rows.append(
            [
                game_pk,
                home_id,
                home_name,
                away_id,
                True,
                home_win,
                1 - home_win,
                int(g["home_runs"]),
                int(g["away_runs"]),
                int(home_hits),
                0,
                None,
                None,
                None,
                imported_at,
            ]
        )

    if team_rows:
        dd.executemany("INSERT INTO team_game_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", team_rows)

    batting_rows = sq.execute(
        """
        SELECT gp.game_id, gp.player_id, gp.player_name, gp.team, gp.at_bats, gp.hits, gp.home_runs, gp.rbi
        FROM game_players gp
        JOIN games g ON g.game_id = gp.game_id
        ORDER BY g.game_date, gp.game_id, gp.player_name
        """
    ).fetchall()

    b_inserts = []
    for r in batting_rows:
        try:
            game_pk = int(str(r["game_id"]).replace("g-", ""))
            player_id = int(str(r["player_id"]))
        except ValueError:
            continue
        team_id, _team_name = _team_info(r["team"])
        ab = int(r["at_bats"] or 0)
        h = int(r["hits"] or 0)
        bb = 0
        hbp = 0
        sf = 0
        obp = (h + bb + hbp) / (ab + bb + hbp + sf) if (ab + bb + hbp + sf) else None
        slg = h / ab if ab else None
        ops = (obp + slg) if obp is not None and slg is not None else None
        b_inserts.append(
            [
                game_pk,
                team_id,
                player_id,
                r["player_name"],
                None,
                None,
                ab,
                0,
                h,
                int(r["rbi"] or 0),
                bb,
                0,
                int(r["home_runs"] or 0),
                0,
                0,
                0,
                0,
                hbp,
                sf,
                obp,
                slg,
                ops,
                datetime.now(UTC).isoformat(),
            ]
        )

    if b_inserts:
        dd.executemany(
            "INSERT INTO player_game_batting VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            b_inserts,
        )

    tstats = sq.execute(
        """
        SELECT team, games_played, wins, losses, runs_scored, runs_allowed, run_diff
        FROM team_stats
        """
    ).fetchall()

    for t in tstats:
        team_id, team_name = _team_info(t["team"])
        dd.execute(
            """
            INSERT INTO team_season_aggregates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                2026,
                team_id,
                team_name,
                int(t["games_played"] or 0),
                int(t["wins"] or 0),
                int(t["losses"] or 0),
                int(t["runs_scored"] or 0),
                int(t["runs_allowed"] or 0),
                int(t["run_diff"] or 0),
                None,
                None,
                None,
                None,
                None,
                None,
                datetime.now(UTC).date().isoformat(),
            ],
        )

    pstats = sq.execute(
        """
        SELECT player_id, player_name, team, at_bats, hits, home_runs, rbi, batting_avg
        FROM player_stats
        """
    ).fetchall()

    for p in pstats:
        try:
            pid = int(str(p["player_id"]))
        except ValueError:
            continue
        team_id, _team_name = _team_info(p["team"])
        dd.execute(
            """
            INSERT INTO player_season_aggregates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                2026,
                pid,
                team_id,
                p["player_name"],
                1,
                int(p["at_bats"] or 0),
                0,
                int(p["hits"] or 0),
                int(p["rbi"] or 0),
                0,
                0,
                int(p["home_runs"] or 0),
                0,
                0,
                0,
                0,
                0,
                0,
                float(p["batting_avg"] or 0.0),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                datetime.now(UTC).date().isoformat(),
            ],
        )

    dd.close()
    sq.close()
    print({"status": "ok", "duckdb": str(DUCKDB_DB)})


if __name__ == "__main__":
    build()
