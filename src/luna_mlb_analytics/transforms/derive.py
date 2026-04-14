from __future__ import annotations

from collections import defaultdict

from luna_mlb_analytics.storage.db import connect, initialize_schema


def derive_team_and_player_stats(db_path: str) -> dict:
    conn = connect(db_path)
    initialize_schema(conn)

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

    pitching_rollup = defaultdict(
        lambda: {
            "name": "",
            "team": "",
            "ip_outs": 0,
            "h_allowed": 0,
            "er": 0,
            "bb_allowed": 0,
            "so_pitched": 0,
            "hr_allowed": 0,
            "pitches": 0,
            "strikes": 0,
        }
    )
    for row in conn.execute(
        """
        SELECT player_id, player_name, team, ip_outs, h_allowed, er, bb_allowed,
               so_pitched, hr_allowed, pitches, strikes
        FROM game_pitchers
        """
    ):
        pid = row["player_id"]
        pitching_rollup[pid]["name"] = row["player_name"]
        pitching_rollup[pid]["team"] = row["team"]
        pitching_rollup[pid]["ip_outs"] += row["ip_outs"]
        pitching_rollup[pid]["h_allowed"] += row["h_allowed"]
        pitching_rollup[pid]["er"] += row["er"]
        pitching_rollup[pid]["bb_allowed"] += row["bb_allowed"]
        pitching_rollup[pid]["so_pitched"] += row["so_pitched"]
        pitching_rollup[pid]["hr_allowed"] += row["hr_allowed"]
        pitching_rollup[pid]["pitches"] += row["pitches"]
        pitching_rollup[pid]["strikes"] += row["strikes"]

    conn.execute("DELETE FROM player_pitching_stats")
    pitching_rows = []
    for pid, stats in sorted(pitching_rollup.items()):
        era = ((stats["er"] * 27.0) / stats["ip_outs"]) if stats["ip_outs"] else None
        pitching_rows.append(
            (
                pid,
                stats["name"],
                stats["team"],
                stats["ip_outs"],
                stats["h_allowed"],
                stats["er"],
                stats["bb_allowed"],
                stats["so_pitched"],
                stats["hr_allowed"],
                stats["pitches"],
                stats["strikes"],
                round(era, 3) if era is not None else None,
            )
        )
    if pitching_rows:
        conn.executemany(
            """
            INSERT INTO player_pitching_stats(
                player_id, player_name, team, ip_outs, h_allowed, er, bb_allowed,
                so_pitched, hr_allowed, pitches, strikes, era
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pitching_rows,
        )

    conn.commit()
    conn.close()

    return {
        "teams_upserted": len(team_rows),
        "players_upserted": len(player_rows),
        "pitchers_upserted": len(pitching_rows),
    }
