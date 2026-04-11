from __future__ import annotations

from collections import defaultdict

from luna_mlb_analytics.storage.db import connect


def derive_team_and_player_stats(db_path: str) -> dict:
    conn = connect(db_path)

    games = conn.execute("SELECT * FROM games ORDER BY game_date, game_id").fetchall()
    if not games:
        conn.close()
        return {"teams_upserted": 0, "players_upserted": 0}

    team_rollup = defaultdict(lambda: {"gp": 0, "w": 0, "l": 0, "rs": 0, "ra": 0})

    for g in games:
        home = g["home_team"]
        away = g["away_team"]
        hr = g["home_runs"]
        ar = g["away_runs"]

        team_rollup[home]["gp"] += 1
        team_rollup[away]["gp"] += 1
        team_rollup[home]["rs"] += hr
        team_rollup[home]["ra"] += ar
        team_rollup[away]["rs"] += ar
        team_rollup[away]["ra"] += hr

        if hr > ar:
            team_rollup[home]["w"] += 1
            team_rollup[away]["l"] += 1
        else:
            team_rollup[away]["w"] += 1
            team_rollup[home]["l"] += 1

    conn.execute("DELETE FROM team_stats")
    team_rows = []
    for team, stats in sorted(team_rollup.items()):
        gp = stats["gp"]
        win_pct = stats["w"] / gp if gp else 0.0
        team_rows.append(
            (
                team,
                gp,
                stats["w"],
                stats["l"],
                stats["rs"],
                stats["ra"],
                stats["rs"] - stats["ra"],
                round(win_pct, 3),
            )
        )

    conn.executemany(
        """
        INSERT INTO team_stats(
            team, games_played, wins, losses, runs_scored, runs_allowed, run_diff, win_pct
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        team_rows,
    )

    player_rollup = defaultdict(
        lambda: {"name": "", "team": "", "ab": 0, "h": 0, "hr": 0, "rbi": 0}
    )
    for row in conn.execute(
        "SELECT player_id, player_name, team, at_bats, hits, home_runs, rbi FROM game_players"
    ):
        pid = row["player_id"]
        player_rollup[pid]["name"] = row["player_name"]
        player_rollup[pid]["team"] = row["team"]
        player_rollup[pid]["ab"] += row["at_bats"]
        player_rollup[pid]["h"] += row["hits"]
        player_rollup[pid]["hr"] += row["home_runs"]
        player_rollup[pid]["rbi"] += row["rbi"]

    conn.execute("DELETE FROM player_stats")
    player_rows = []
    for pid, stats in sorted(player_rollup.items()):
        avg = (stats["h"] / stats["ab"]) if stats["ab"] else 0.0
        player_rows.append(
            (
                pid,
                stats["name"],
                stats["team"],
                stats["ab"],
                stats["h"],
                stats["hr"],
                stats["rbi"],
                round(avg, 3),
            )
        )

    conn.executemany(
        """
        INSERT INTO player_stats(
            player_id, player_name, team, at_bats, hits, home_runs, rbi, batting_avg
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        player_rows,
    )

    conn.commit()
    conn.close()

    return {"teams_upserted": len(team_rows), "players_upserted": len(player_rows)}
