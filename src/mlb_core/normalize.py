from __future__ import annotations

import hashlib
import json
from datetime import datetime


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def _ip_to_outs(ip_text: str | None) -> int:
    if not ip_text:
        return 0
    parts = str(ip_text).split(".")
    whole = _safe_int(parts[0], 0)
    frac = _safe_int(parts[1], 0) if len(parts) > 1 else 0
    return (whole * 3) + frac


def games_from_schedule(schedule_payload: dict) -> list[dict]:
    games = []
    for d in schedule_payload.get("dates", []):
      game_date = d.get("date")
      for g in d.get("games", []):
        status = (g.get("status") or {}).get("detailedState") or "Unknown"
        teams = g.get("teams") or {}
        home = (teams.get("home") or {}).get("team") or {}
        away = (teams.get("away") or {}).get("team") or {}
        row = {
            "game_pk": _safe_int(g.get("gamePk"), 0),
            "game_date": game_date,
            "season": _safe_int((g.get("season") or str(game_date)[:4]), 0),
            "status": status,
            "home_team_id": _safe_int(home.get("id"), 0),
            "home_team_name": home.get("name"),
            "away_team_id": _safe_int(away.get("id"), 0),
            "away_team_name": away.get("name"),
            "home_score": _safe_int((teams.get("home") or {}).get("score"), 0),
            "away_score": _safe_int((teams.get("away") or {}).get("score"), 0),
            "venue_id": _safe_int((g.get("venue") or {}).get("id"), 0),
            "venue_name": (g.get("venue") or {}).get("name"),
            "start_time_utc": g.get("gameDate"),
            "source_payload_hash": hashlib.sha256(json.dumps(g, sort_keys=True).encode()).hexdigest(),
            "last_ingested_at": datetime.utcnow().isoformat(),
        }
        if row["game_pk"]:
            games.append(row)
    return games


def normalize_boxscore(game_pk: int, boxscore_payload: dict) -> tuple[list[dict], list[dict], list[dict]]:
    team_rows = []
    batting_rows = []
    pitching_rows = []

    teams = (boxscore_payload.get("teams") or {})
    home = teams.get("home") or {}
    away = teams.get("away") or {}

    for side, team_obj, opp_obj, is_home in [
        ("home", home, away, True),
        ("away", away, home, False),
    ]:
        team = team_obj.get("team") or {}
        opp_team = opp_obj.get("team") or {}
        stats = team_obj.get("teamStats") or {}
        bat = stats.get("batting") or {}
        pit = stats.get("pitching") or {}
        runs_scored = _safe_int(team_obj.get("teamStats", {}).get("batting", {}).get("runs"), 0)
        runs_allowed = _safe_int(opp_obj.get("teamStats", {}).get("batting", {}).get("runs"), 0)

        obp = 0.0
        slg = 0.0
        ab = _safe_int(bat.get("atBats"), 0)
        h = _safe_int(bat.get("hits"), 0)
        bb = _safe_int(bat.get("baseOnBalls"), 0)
        hbp = _safe_int(bat.get("hitByPitch"), 0)
        sf = _safe_int(bat.get("sacFlies"), 0)
        doubles = _safe_int(bat.get("doubles"), 0)
        triples = _safe_int(bat.get("triples"), 0)
        hr = _safe_int(bat.get("homeRuns"), 0)
        denom_obp = ab + bb + hbp + sf
        if denom_obp:
            obp = (h + bb + hbp) / denom_obp
        if ab:
            slg = (h + doubles + (2 * triples) + (3 * hr)) / ab

        ip_outs = _ip_to_outs(pit.get("inningsPitched"))
        er = _safe_int(pit.get("earnedRuns"), 0)
        era = (er * 27.0 / ip_outs) if ip_outs else None

        team_rows.append({
            "game_pk": game_pk,
            "team_id": _safe_int(team.get("id"), 0),
            "team_name": team.get("name"),
            "opponent_team_id": _safe_int(opp_team.get("id"), 0),
            "is_home": is_home,
            "win_flag": 1 if runs_scored > runs_allowed else 0,
            "loss_flag": 1 if runs_scored < runs_allowed else 0,
            "runs_scored": runs_scored,
            "runs_allowed": runs_allowed,
            "hits": h,
            "errors": _safe_int((team_obj.get("teamStats", {}).get("fielding", {}) or {}).get("errors"), 0),
            "left_on_base": _safe_int(bat.get("leftOnBase"), 0),
            "team_ops_game": obp + slg,
            "team_era_game": era,
            "last_ingested_at": datetime.utcnow().isoformat(),
        })

        players = team_obj.get("players") or {}
        for pdata in players.values():
            person = pdata.get("person") or {}
            b = (pdata.get("stats") or {}).get("batting") or {}
            p = (pdata.get("stats") or {}).get("pitching") or {}
            pid = _safe_int(person.get("id"), 0)
            name = person.get("fullName")
            if pid == 0:
                continue

            if b:
                bab = _safe_int(b.get("atBats"), 0)
                bh = _safe_int(b.get("hits"), 0)
                bbb = _safe_int(b.get("baseOnBalls"), 0)
                bhbp = _safe_int(b.get("hitByPitch"), 0)
                bsf = _safe_int(b.get("sacFlies"), 0)
                bd = _safe_int(b.get("doubles"), 0)
                bt = _safe_int(b.get("triples"), 0)
                bhr = _safe_int(b.get("homeRuns"), 0)
                bobp = None
                bslg = None
                if bab + bbb + bhbp + bsf:
                    bobp = (bh + bbb + bhbp) / (bab + bbb + bhbp + bsf)
                if bab:
                    bslg = (bh + bd + 2 * bt + 3 * bhr) / bab
                batting_rows.append({
                    "game_pk": game_pk,
                    "team_id": _safe_int(team.get("id"), 0),
                    "player_id": pid,
                    "player_name": name,
                    "batting_order": pdata.get("battingOrder"),
                    "position": ((pdata.get("position") or {}).get("abbreviation")),
                    "ab": bab,
                    "r": _safe_int(b.get("runs"), 0),
                    "h": bh,
                    "rbi": _safe_int(b.get("rbi"), 0),
                    "bb": bbb,
                    "so": _safe_int(b.get("strikeOuts"), 0),
                    "hr": bhr,
                    "doubles": bd,
                    "triples": bt,
                    "sb": _safe_int(b.get("stolenBases"), 0),
                    "cs": _safe_int(b.get("caughtStealing"), 0),
                    "hbp": bhbp,
                    "sf": bsf,
                    "obp_game": bobp,
                    "slg_game": bslg,
                    "ops_game": (bobp + bslg) if (bobp is not None and bslg is not None) else None,
                    "last_ingested_at": datetime.utcnow().isoformat(),
                })

            if p and (_safe_int(p.get("outs"), 0) > 0 or p.get("inningsPitched")):
                ip_outs = _safe_int(p.get("outs"), 0)
                if ip_outs == 0:
                    ip_outs = _ip_to_outs(p.get("inningsPitched"))
                er = _safe_int(p.get("earnedRuns"), 0)
                pitching_rows.append({
                    "game_pk": game_pk,
                    "team_id": _safe_int(team.get("id"), 0),
                    "player_id": pid,
                    "player_name": name,
                    "ip_outs": ip_outs,
                    "h_allowed": _safe_int(p.get("hits"), 0),
                    "er": er,
                    "bb_allowed": _safe_int(p.get("baseOnBalls"), 0),
                    "so_pitched": _safe_int(p.get("strikeOuts"), 0),
                    "hr_allowed": _safe_int(p.get("homeRuns"), 0),
                    "pitches": _safe_int(p.get("numberOfPitches"), 0),
                    "strikes": _safe_int(p.get("strikes"), 0),
                    "decision": p.get("note"),
                    "era_game": (er * 27.0 / ip_outs) if ip_outs else None,
                    "last_ingested_at": datetime.utcnow().isoformat(),
                })

    return team_rows, batting_rows, pitching_rows
