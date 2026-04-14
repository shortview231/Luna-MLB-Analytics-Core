from __future__ import annotations

from .db import AGG_PLAYER_SQL, AGG_TEAM_SQL


def rebuild_aggregates(con, season: int) -> dict:
    con.execute("DELETE FROM team_season_aggregates WHERE season = ?", [season])
    con.execute("DELETE FROM player_season_aggregates WHERE season = ?", [season])
    con.execute(AGG_TEAM_SQL, [season, season, season])
    con.execute(AGG_PLAYER_SQL, [season, season])

    team_rows = con.execute("SELECT COUNT(*) FROM team_season_aggregates WHERE season=?", [season]).fetchone()[0]
    player_rows = con.execute("SELECT COUNT(*) FROM player_season_aggregates WHERE season=?", [season]).fetchone()[0]
    return {"team_rows": int(team_rows), "player_rows": int(player_rows)}
