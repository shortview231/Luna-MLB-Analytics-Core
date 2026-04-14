from __future__ import annotations

from .presentation import build_game_overlay, build_player_overlay, build_team_overlay, get_presentation_snapshot
from .team_identity import get_team_identity


def query_rows(con, sql: str, params: list | None = None) -> list[dict]:
    result = con.execute(sql, params or [])
    cols = [d[0] for d in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]


def format_ip(ip_outs: int | None) -> str:
    if ip_outs is None:
        return ""
    whole = int(ip_outs) // 3
    rem = int(ip_outs) % 3
    return f"{whole}.{rem}"


def build_standings_rows(con, season: int) -> list[dict]:
    snapshot = get_presentation_snapshot(season)
    base = query_rows(
        con,
        """
        SELECT team_id, team_name, wins, losses, runs_scored, runs_allowed, run_differential, team_ops, team_era, streak, last_10, next_game, next_game_time
        FROM team_season_aggregates
        WHERE season=?
        """,
        [season],
    )
    grouped: dict[str, list[dict]] = {}
    for row in base:
        ident = get_team_identity(int(row["team_id"]))
        wins = int(row.get("wins") or 0)
        losses = int(row.get("losses") or 0)
        out = dict(row)
        out["team_name"] = ident["canonical_name"]
        out["team_abbreviation"] = ident["canonical_abbreviation"]
        out["league"] = ident["league"]
        out["division"] = ident["division"]
        out["pct"] = (wins / (wins + losses)) if (wins + losses) else 0.0
        out.update(build_team_overlay(snapshot, int(row["team_id"])))
        grouped.setdefault(ident["division"], []).append(out)

    rows: list[dict] = []
    for division, teams in grouped.items():
        teams.sort(
            key=lambda r: (
                -int(r.get("wins") or 0),
                int(r.get("losses") or 0),
                -int(r.get("run_differential") or 0),
                str(r.get("team_name") or ""),
            )
        )
        leader_wins = int(teams[0].get("wins") or 0)
        leader_losses = int(teams[0].get("losses") or 0)
        for idx, team in enumerate(teams, start=1):
            wins = int(team.get("wins") or 0)
            losses = int(team.get("losses") or 0)
            team["division_rank"] = idx
            team["gb"] = ((leader_wins - wins) + (losses - leader_losses)) / 2.0
            rows.append(team)
    rows.sort(key=lambda r: (r["league"], r["division"], r["division_rank"]))
    return rows


def build_scoreboard_games(con, season: int, game_date, favorite_team_id: int | None = None) -> list[dict]:
    snapshot = get_presentation_snapshot(season)
    rows = query_rows(
        con,
        """
        SELECT
          g.game_pk, g.game_date, g.start_time_utc, g.status,
          g.away_team_id, g.away_team_name, g.away_score,
          g.home_team_id, g.home_team_name, g.home_score,
          COALESCE((SELECT t.hits FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.away_team_id LIMIT 1), 0) AS away_hits,
          COALESCE((SELECT t.errors FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.away_team_id LIMIT 1), 0) AS away_errors,
          COALESCE((SELECT t.hits FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.home_team_id LIMIT 1), 0) AS home_hits,
          COALESCE((SELECT t.errors FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.home_team_id LIMIT 1), 0) AS home_errors
        FROM games g
        WHERE g.season=? AND g.game_date=?
        ORDER BY g.start_time_utc ASC, g.game_pk ASC
        """,
        [season, game_date],
    )
    for row in rows:
        away = get_team_identity(int(row["away_team_id"]))
        home = get_team_identity(int(row["home_team_id"]))
        row["away_team_name"] = away["canonical_name"]
        row["away_team_abbreviation"] = away["canonical_abbreviation"]
        row["home_team_name"] = home["canonical_name"]
        row["home_team_abbreviation"] = home["canonical_abbreviation"]
        row.update({f"away_{k[5:]}": v for k, v in build_team_overlay(snapshot, int(row["away_team_id"])).items() if k.startswith("team_")})
        row.update({f"home_{k[5:]}": v for k, v in build_team_overlay(snapshot, int(row["home_team_id"])).items() if k.startswith("team_")})
        row.update(build_game_overlay(snapshot, int(row["game_pk"])))
        row["is_favorite_team_game"] = int(favorite_team_id or 0) in {int(row["away_team_id"]), int(row["home_team_id"])}
    rows.sort(key=lambda r: (0 if r["is_favorite_team_game"] else 1, str(r.get("start_time_utc") or ""), int(r["game_pk"])))
    return rows


def build_player_batting_rows(con, season: int, min_ab: int = 0) -> list[dict]:
    snapshot = get_presentation_snapshot(season)
    rows = query_rows(
        con,
        """
        SELECT
          p.player_id, p.player_name, p.team_id, p.games_played, p.ab, p.r, p.h, p.rbi, p.bb, p.so, p.hr, p.doubles, p.triples, p.sb, p.cs, p.hbp, p.sf, p.ops
        FROM player_season_aggregates p
        WHERE p.season=? AND p.ab >= ?
        """,
        [season, min_ab],
    )
    for row in rows:
        ident = get_team_identity(int(row["team_id"]))
        row["team_name"] = ident["canonical_name"]
        row["team_abbreviation"] = ident["canonical_abbreviation"]
        row.update(build_team_overlay(snapshot, int(row["team_id"])))
        row.update(build_player_overlay(snapshot, int(row["player_id"])))
    return rows


def build_player_pitching_rows(con, season: int, min_outs: int = 0) -> list[dict]:
    snapshot = get_presentation_snapshot(season)
    rows = query_rows(
        con,
        """
        SELECT
          p.player_id, p.player_name, p.team_id, p.games_played, p.ip_outs, p.er, p.h_allowed, p.bb_allowed, p.so_pitched, p.hr_allowed, p.era
        FROM player_season_aggregates p
        WHERE p.season=? AND p.ip_outs >= ?
        """,
        [season, min_outs],
    )
    for row in rows:
        ident = get_team_identity(int(row["team_id"]))
        row["team_name"] = ident["canonical_name"]
        row["team_abbreviation"] = ident["canonical_abbreviation"]
        row["ip_display"] = format_ip(row.get("ip_outs"))
        row.update(build_team_overlay(snapshot, int(row["team_id"])))
        row.update(build_player_overlay(snapshot, int(row["player_id"])))
    return rows


def build_player_profile(con, season: int, player_id: int) -> dict | None:
    batting = build_player_batting_rows(con, season, 0)
    pitching = build_player_pitching_rows(con, season, 0)
    hit = next((r for r in batting if int(r["player_id"]) == int(player_id)), None)
    pit = next((r for r in pitching if int(r["player_id"]) == int(player_id)), None)
    if not hit and not pit:
        return None
    base = dict(hit or pit)
    if pit:
        base["ip_outs"] = pit.get("ip_outs")
        base["ip_display"] = pit.get("ip_display")
        base["er"] = pit.get("er")
        base["h_allowed"] = pit.get("h_allowed")
        base["bb_allowed"] = pit.get("bb_allowed")
        base["so_pitched"] = pit.get("so_pitched")
        base["hr_allowed"] = pit.get("hr_allowed")
        base["era"] = pit.get("era")
    return base


def build_boxscore_view(con, season: int, game_pk: int) -> dict:
    snapshot = get_presentation_snapshot(season)
    games = query_rows(
        con,
        """
        SELECT game_pk, game_date, away_team_id, away_team_name, away_score, home_team_id, home_team_name, home_score, status, venue_name
        FROM games
        WHERE game_pk=?
        LIMIT 1
        """,
        [game_pk],
    )
    if not games:
        raise ValueError(f"Unknown game_pk: {game_pk}")
    game = games[0]
    away_ident = get_team_identity(int(game["away_team_id"]))
    home_ident = get_team_identity(int(game["home_team_id"]))
    away_overlay = build_team_overlay(snapshot, int(game["away_team_id"]))
    home_overlay = build_team_overlay(snapshot, int(game["home_team_id"]))
    game_overlay = build_game_overlay(snapshot, int(game_pk))
    teams = query_rows(
        con,
        """
        SELECT team_id, team_name, is_home, runs_scored, hits, errors, left_on_base, team_ops_game, team_era_game
        FROM team_game_results
        WHERE game_pk=?
        ORDER BY is_home ASC, team_id ASC
        """,
        [game_pk],
    )
    batting = query_rows(
        con,
        """
        SELECT player_id, team_id, player_name, position, batting_order, ab, r, h, rbi, bb, so, hr, doubles, triples, sb, cs, hbp, sf, obp_game, slg_game, ops_game
        FROM player_game_batting
        WHERE game_pk=?
        ORDER BY team_id, batting_order ASC NULLS LAST, player_name ASC
        """,
        [game_pk],
    )
    pitching = query_rows(
        con,
        """
        SELECT player_id, team_id, player_name, ip_outs, h_allowed, er, bb_allowed, so_pitched, hr_allowed, pitches, strikes, era_game
        FROM player_game_pitching
        WHERE game_pk=?
        ORDER BY team_id, ip_outs DESC, so_pitched DESC, player_name ASC
        """,
        [game_pk],
    )
    for row in teams:
        row.update(build_team_overlay(snapshot, int(row["team_id"])))
    for row in batting:
        row.update(build_player_overlay(snapshot, int(row["player_id"])))
        row.update(build_team_overlay(snapshot, int(row["team_id"])))
    for row in pitching:
        row.update(build_player_overlay(snapshot, int(row["player_id"])))
        row.update(build_team_overlay(snapshot, int(row["team_id"])))
    return {
        "game": {
            "game_pk": game["game_pk"],
            "game_date": game["game_date"],
            "status": game["status"],
            "away_team_id": game["away_team_id"],
            "away_team_name": away_ident["canonical_name"],
            "away_team_abbreviation": away_ident["canonical_abbreviation"],
            "away_score": game["away_score"],
            "home_team_id": game["home_team_id"],
            "home_team_name": home_ident["canonical_name"],
            "home_team_abbreviation": home_ident["canonical_abbreviation"],
            "home_score": game["home_score"],
            "venue_name": game.get("venue_name"),
            **{f"away_{k[5:]}": v for k, v in away_overlay.items() if k.startswith("team_")},
            **{f"home_{k[5:]}": v for k, v in home_overlay.items() if k.startswith("team_")},
            **game_overlay,
        },
        "team_totals": teams,
        "batting_rows": batting,
        "pitching_rows": pitching,
        "season": season,
    }
