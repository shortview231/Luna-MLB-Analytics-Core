#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
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
    "CHW": (145, "Chicago White Sox"),
    "KCR": (118, "Kansas City Royals"),
    "SFG": (137, "San Francisco Giants"),
    "SDP": (135, "San Diego Padres"),
    "TBR": (139, "Tampa Bay Rays"),
    "WSN": (120, "Washington Nationals"),
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
        dd.executemany(
            "INSERT INTO team_game_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            team_rows,
        )

    batting_rows = sq.execute(
        """
        SELECT gp.game_id, gp.player_id, gp.player_name, gp.team, gp.at_bats,
               gp.hits, gp.home_runs, gp.rbi
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
            (
                "INSERT INTO player_game_batting VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            b_inserts,
        )

    pitching_rows = sq.execute(
        """
        SELECT gp.game_id, gp.player_id, gp.player_name, gp.team, gp.ip_outs, gp.h_allowed, gp.er,
               gp.bb_allowed, gp.so_pitched, gp.hr_allowed, gp.pitches, gp.strikes, gp.era_game
        FROM game_pitchers gp
        JOIN games g ON g.game_id = gp.game_id
        ORDER BY g.game_date, gp.game_id, gp.player_name
        """
    ).fetchall()

    p_inserts = []
    for r in pitching_rows:
        try:
            game_pk = int(str(r["game_id"]).replace("g-", ""))
            player_id = int(str(r["player_id"]))
        except ValueError:
            continue
        team_id, _team_name = _team_info(r["team"])
        p_inserts.append(
            [
                game_pk,
                team_id,
                player_id,
                r["player_name"],
                int(r["ip_outs"] or 0),
                int(r["h_allowed"] or 0),
                int(r["er"] or 0),
                int(r["bb_allowed"] or 0),
                int(r["so_pitched"] or 0),
                int(r["hr_allowed"] or 0),
                int(r["pitches"] or 0),
                int(r["strikes"] or 0),
                None,
                float(r["era_game"] or 0.0),
                datetime.now(UTC).isoformat(),
            ]
        )

    if p_inserts:
        dd.executemany(
            "INSERT INTO player_game_pitching VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            p_inserts,
        )

    dd.execute(
        """
        INSERT INTO team_season_aggregates
        WITH team_rollup AS (
          SELECT
            g.season,
            t.team_id,
            MIN(t.team_name) AS team_name,
            COUNT(*) AS games_played,
            SUM(t.win_flag) AS wins,
            SUM(t.loss_flag) AS losses,
            SUM(t.runs_scored) AS runs_scored,
            SUM(t.runs_allowed) AS runs_allowed,
            SUM(t.runs_scored) - SUM(t.runs_allowed) AS run_differential,
            MAX(g.game_date) AS as_of_date
          FROM team_game_results t
          JOIN games g ON g.game_pk = t.game_pk
          WHERE lower(g.status) = 'final' AND t.team_id > 0
          GROUP BY g.season, t.team_id
        ),
        team_ops AS (
          SELECT
            g.season,
            b.team_id,
            CASE
              WHEN SUM(b.ab) > 0 AND (SUM(b.ab) + SUM(b.bb) + SUM(b.hbp) + SUM(b.sf)) > 0
              THEN
                (
                  (SUM(b.h) + SUM(b.bb) + SUM(b.hbp))::DOUBLE
                  / (SUM(b.ab) + SUM(b.bb) + SUM(b.hbp) + SUM(b.sf))
                ) +
                (
                  (SUM(b.h) + SUM(b.doubles) + (2 * SUM(b.triples)) + (3 * SUM(b.hr)))::DOUBLE
                  / SUM(b.ab)
                )
              ELSE NULL
            END AS team_ops
          FROM player_game_batting b
          JOIN games g ON g.game_pk = b.game_pk
          WHERE lower(g.status) = 'final' AND b.team_id > 0
          GROUP BY g.season, b.team_id
        ),
        team_era AS (
          SELECT
            g.season,
            p.team_id,
            CASE
              WHEN SUM(p.ip_outs) > 0
              THEN ROUND((SUM(p.er)::DOUBLE * 27.0) / SUM(p.ip_outs), 4)
              ELSE NULL
            END AS team_era
          FROM player_game_pitching p
          JOIN games g ON g.game_pk = p.game_pk
          WHERE lower(g.status) = 'final' AND p.team_id > 0
          GROUP BY g.season, p.team_id
        )
        SELECT
          tr.season,
          tr.team_id,
          tr.team_name,
          tr.games_played,
          tr.wins,
          tr.losses,
          tr.runs_scored,
          tr.runs_allowed,
          tr.run_differential,
          o.team_ops,
          e.team_era,
          NULL AS streak,
          NULL AS last_10,
          NULL AS next_game,
          NULL AS next_game_time,
          tr.as_of_date
        FROM team_rollup tr
        LEFT JOIN team_ops o ON o.season = tr.season AND o.team_id = tr.team_id
        LEFT JOIN team_era e ON e.season = tr.season AND e.team_id = tr.team_id
        """
    )

    dd.execute(
        """
        INSERT INTO player_season_aggregates
        WITH bat AS (
          SELECT
            g.season,
            b.player_id,
            b.team_id,
            MIN(b.player_name) AS player_name,
            COUNT(DISTINCT b.game_pk) AS games_played,
            SUM(b.ab) AS ab,
            SUM(b.r) AS r,
            SUM(b.h) AS h,
            SUM(b.rbi) AS rbi,
            SUM(b.bb) AS bb,
            SUM(b.so) AS so,
            SUM(b.hr) AS hr,
            SUM(b.doubles) AS doubles,
            SUM(b.triples) AS triples,
            SUM(b.sb) AS sb,
            SUM(b.cs) AS cs,
            SUM(b.hbp) AS hbp,
            SUM(b.sf) AS sf,
            CASE
              WHEN SUM(b.ab) > 0 AND (SUM(b.ab) + SUM(b.bb) + SUM(b.hbp) + SUM(b.sf)) > 0
              THEN
                (
                  (SUM(b.h) + SUM(b.bb) + SUM(b.hbp))::DOUBLE
                  / (SUM(b.ab) + SUM(b.bb) + SUM(b.hbp) + SUM(b.sf))
                ) +
                (
                  (SUM(b.h) + SUM(b.doubles) + (2 * SUM(b.triples)) + (3 * SUM(b.hr)))::DOUBLE
                  / SUM(b.ab)
                )
              ELSE NULL
            END AS ops,
            MAX(g.game_date) AS as_of_date
          FROM player_game_batting b
          JOIN games g ON g.game_pk = b.game_pk
          WHERE lower(g.status) = 'final' AND b.team_id > 0
          GROUP BY g.season, b.player_id, b.team_id
        ),
        pit AS (
          SELECT
            g.season,
            p.player_id,
            p.team_id,
            MIN(p.player_name) AS player_name,
            COUNT(DISTINCT p.game_pk) AS games_played,
            SUM(p.ip_outs) AS ip_outs,
            SUM(p.er) AS er,
            SUM(p.h_allowed) AS h_allowed,
            SUM(p.bb_allowed) AS bb_allowed,
            SUM(p.so_pitched) AS so_pitched,
            SUM(p.hr_allowed) AS hr_allowed,
            CASE
              WHEN SUM(p.ip_outs) > 0
              THEN ROUND((SUM(p.er)::DOUBLE * 27.0) / SUM(p.ip_outs), 4)
              ELSE NULL
            END AS era,
            MAX(g.game_date) AS as_of_date
          FROM player_game_pitching p
          JOIN games g ON g.game_pk = p.game_pk
          WHERE lower(g.status) = 'final' AND p.team_id > 0
          GROUP BY g.season, p.player_id, p.team_id
        )
        SELECT
          COALESCE(bat.season, pit.season) AS season,
          COALESCE(bat.player_id, pit.player_id) AS player_id,
          COALESCE(bat.team_id, pit.team_id) AS team_id,
          COALESCE(bat.player_name, pit.player_name) AS player_name,
          COALESCE(bat.games_played, 0) + COALESCE(pit.games_played, 0) AS games_played,
          COALESCE(bat.ab, 0) AS ab,
          COALESCE(bat.r, 0) AS r,
          COALESCE(bat.h, 0) AS h,
          COALESCE(bat.rbi, 0) AS rbi,
          COALESCE(bat.bb, 0) AS bb,
          COALESCE(bat.so, 0) AS so,
          COALESCE(bat.hr, 0) AS hr,
          COALESCE(bat.doubles, 0) AS doubles,
          COALESCE(bat.triples, 0) AS triples,
          COALESCE(bat.sb, 0) AS sb,
          COALESCE(bat.cs, 0) AS cs,
          COALESCE(bat.hbp, 0) AS hbp,
          COALESCE(bat.sf, 0) AS sf,
          bat.ops AS ops,
          pit.ip_outs,
          pit.er,
          pit.h_allowed,
          pit.bb_allowed,
          pit.so_pitched,
          pit.hr_allowed,
          pit.era,
          COALESCE(bat.as_of_date, pit.as_of_date) AS as_of_date
        FROM bat
        FULL OUTER JOIN pit
          ON bat.season = pit.season
          AND bat.player_id = pit.player_id
          AND bat.team_id = pit.team_id
        """
    )

    dd.close()
    sq.close()
    print({"status": "ok", "duckdb": str(DUCKDB_DB)})


if __name__ == "__main__":
    build()
