import json
from pathlib import Path

from luna_mlb_analytics.ingestion.importer import import_bundle
from luna_mlb_analytics.storage.db import connect
from luna_mlb_analytics.transforms.derive import derive_team_and_player_stats


def test_ingest_and_derive(tmp_path):
    bundle = Path("tests/fixtures/sample_boxscore_bundle.json")
    db = tmp_path / "test.sqlite"

    import_result = import_bundle(bundle, db)
    assert import_result["status"] == "imported"
    assert import_result["inserted_games"] == 2

    derive_result = derive_team_and_player_stats(str(db))
    assert derive_result["teams_upserted"] == 4
    assert derive_result["players_upserted"] == 4

    conn = connect(db)
    counts = {
        "team_count": conn.execute("SELECT COUNT(*) AS c FROM team_stats").fetchone()["c"],
        "player_count": conn.execute("SELECT COUNT(*) AS c FROM player_stats").fetchone()["c"],
    }
    top_team = conn.execute(
        "SELECT team, wins, losses FROM team_stats ORDER BY win_pct DESC, run_diff DESC LIMIT 1"
    ).fetchone()
    conn.close()

    expected = json.loads(Path("data/fixtures/expected/expected_summary.json").read_text())
    assert counts["team_count"] == expected["team_count"]
    assert counts["player_count"] == expected["player_count"]
    assert dict(top_team) == expected["top_team"]
