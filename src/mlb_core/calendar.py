from __future__ import annotations


def build_cardinals_schedule_events(con, season: int, cardinals_team_id: int = 138) -> list[dict]:
    rows = con.execute(
        """
        SELECT game_pk, game_date, start_time_utc, home_team_id, away_team_id, home_team_name, away_team_name
        FROM games
        WHERE season = ? AND (home_team_id = ? OR away_team_id = ?)
        ORDER BY game_date, start_time_utc
        """,
        [season, cardinals_team_id, cardinals_team_id],
    ).fetchall()

    events = []
    for r in rows:
        game_pk, game_date, start_time, home_id, away_id, home_name, away_name = r
        opp = away_name if int(home_id) == cardinals_team_id else home_name
        home_away = "home" if int(home_id) == cardinals_team_id else "away"
        events.append(
            {
                "game_pk": int(game_pk),
                "date": str(game_date),
                "time": str(start_time) if start_time is not None else None,
                "opponent": opp,
                "home_away": home_away,
                "title": f"Cardinals vs {opp}" if home_away == "home" else f"Cardinals @ {opp}",
            }
        )
    return events
