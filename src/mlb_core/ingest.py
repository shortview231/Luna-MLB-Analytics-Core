from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

from .normalize import games_from_schedule, normalize_boxscore
from .statsapi import fetch_boxscore, fetch_schedule


FINAL_STATES = {"final", "game over", "completed early"}


def iter_dates(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def pull_window(start_date: date, end_date: date, raw_dir: Path) -> tuple[list[dict], dict[int, dict]]:
    schedule = fetch_schedule(start_date.isoformat(), end_date.isoformat())
    raw_dir.mkdir(parents=True, exist_ok=True)
    out = raw_dir / f"schedule_{start_date.isoformat()}_{end_date.isoformat()}.json"
    out.write_text(json.dumps(schedule, indent=2), encoding="utf-8")

    games = games_from_schedule(schedule)
    finalized = [g for g in games if str(g.get("status", "")).strip().lower() in FINAL_STATES]

    boxscores: dict[int, dict] = {}
    for game in finalized:
        gpk = int(game["game_pk"])
        try:
            payload = fetch_boxscore(gpk)
        except Exception as exc:  # keep long pulls resilient
            print(f"[warn] boxscore fetch failed gamePk={gpk}: {exc}")
            continue
        boxscores[gpk] = payload
        (raw_dir / f"boxscore_{gpk}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return finalized, boxscores


def upsert_records(con, games: list[dict], boxscores: dict[int, dict]) -> dict:
    now_ts = datetime.utcnow().isoformat()
    upserted = 0
    skipped_no_boxscore = 0
    for g in games:
        if int(g["game_pk"]) not in boxscores:
            skipped_no_boxscore += 1
            continue
        g["last_ingested_at"] = now_ts
        con.execute(
            """
            INSERT OR REPLACE INTO games VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                g["game_pk"], g["game_date"], g["season"], g["status"],
                g["home_team_id"], g["home_team_name"], g["away_team_id"], g["away_team_name"],
                g["home_score"], g["away_score"], g["venue_id"], g["venue_name"],
                g["start_time_utc"], g["source_payload_hash"], g["last_ingested_at"],
            ],
        )

        team_rows, bat_rows, pit_rows = normalize_boxscore(g["game_pk"], boxscores[g["game_pk"]])
        for r in team_rows:
            con.execute(
                """
                INSERT OR REPLACE INTO team_game_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["game_pk"], r["team_id"], r["team_name"], r["opponent_team_id"], r["is_home"],
                    r["win_flag"], r["loss_flag"], r["runs_scored"], r["runs_allowed"], r["hits"],
                    r["errors"], r["left_on_base"], r["team_ops_game"], r["team_era_game"], r["last_ingested_at"],
                ],
            )
        for r in bat_rows:
            con.execute(
                """
                INSERT OR REPLACE INTO player_game_batting VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["game_pk"], r["team_id"], r["player_id"], r["player_name"], r["batting_order"], r["position"],
                    r["ab"], r["r"], r["h"], r["rbi"], r["bb"], r["so"], r["hr"], r["doubles"], r["triples"],
                    r["sb"], r["cs"], r["hbp"], r["sf"], r["obp_game"], r["slg_game"], r["ops_game"], r["last_ingested_at"],
                ],
            )
        for r in pit_rows:
            con.execute(
                """
                INSERT OR REPLACE INTO player_game_pitching VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["game_pk"], r["team_id"], r["player_id"], r["player_name"], r["ip_outs"], r["h_allowed"],
                    r["er"], r["bb_allowed"], r["so_pitched"], r["hr_allowed"], r["pitches"], r["strikes"],
                    r["decision"], r["era_game"], r["last_ingested_at"],
                ],
            )
        upserted += 1

    return {
        "games_upserted": upserted,
        "games_missing_boxscore": skipped_no_boxscore,
        "boxscores_loaded": len(boxscores),
    }
