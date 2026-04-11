#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from luna_mlb_analytics.storage.db import connect


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke check that derived tables are queryable.")
    parser.add_argument("--db", default="luna_mlb.sqlite", help="Path to sqlite DB")
    args = parser.parse_args()

    conn = connect(args.db)
    team_count = conn.execute("SELECT COUNT(*) AS c FROM team_stats").fetchone()["c"]
    player_count = conn.execute("SELECT COUNT(*) AS c FROM player_stats").fetchone()["c"]
    top_team = conn.execute(
        "SELECT team, wins, losses FROM team_stats ORDER BY win_pct DESC, run_diff DESC LIMIT 1"
    ).fetchone()
    conn.close()

    result = {
        "team_count": team_count,
        "player_count": player_count,
        "top_team": dict(top_team) if top_team else None,
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
