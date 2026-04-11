from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from luna_mlb_analytics.ingestion.bundle_schema import validate_bundle
from luna_mlb_analytics.storage.db import connect, initialize_schema


def import_bundle(bundle_path: str | Path, db_path: str | Path) -> dict:
    bundle_path = Path(bundle_path)
    with bundle_path.open("r", encoding="utf-8") as f:
        bundle = json.load(f)

    validate_bundle(bundle)

    conn = connect(db_path)
    initialize_schema(conn)

    existing = conn.execute(
        "SELECT bundle_id FROM import_ledger WHERE bundle_id = ?", (bundle["bundle_id"],)
    ).fetchone()
    if existing:
        conn.close()
        return {"bundle_id": bundle["bundle_id"], "inserted_games": 0, "status": "already_imported"}

    imported_at = datetime.now(UTC).isoformat()
    rows = [
        (
            g["game_id"],
            g["game_date"],
            g["home_team"],
            g["away_team"],
            int(g["home_runs"]),
            int(g["away_runs"]),
            bundle["bundle_id"],
            imported_at,
        )
        for g in bundle["games"]
    ]
    conn.executemany(
        """
        INSERT INTO games(
            game_id, game_date, home_team, away_team,
            home_runs, away_runs, source_bundle_id, imported_at
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    player_rows = []
    for g in bundle["games"]:
        for p in g["players"]:
            player_rows.append(
                (
                    g["game_id"],
                    p["player_id"],
                    p["player_name"],
                    p["team"],
                    int(p["at_bats"]),
                    int(p["hits"]),
                    int(p["home_runs"]),
                    int(p["rbi"]),
                )
            )
    conn.executemany(
        """
        INSERT INTO game_players(
            game_id, player_id, player_name, team, at_bats, hits, home_runs, rbi
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        player_rows,
    )

    conn.execute(
        "INSERT INTO import_ledger(bundle_id, imported_at, game_count) VALUES(?, ?, ?)",
        (bundle["bundle_id"], imported_at, len(rows)),
    )
    conn.commit()
    conn.close()

    return {
        "bundle_id": bundle["bundle_id"],
        "inserted_games": len(rows),
        "inserted_player_lines": len(player_rows),
        "status": "imported",
    }
