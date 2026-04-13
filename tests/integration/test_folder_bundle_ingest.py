import json
from hashlib import sha256
from pathlib import Path

from luna_mlb_analytics.ingestion.importer import import_bundle
from luna_mlb_analytics.storage.db import connect


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def test_import_folder_bundle(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "mlb_reg_2026_20260410_20260410"
    schedule = {
        "source": "mlb_statsapi",
        "season": 2026,
        "start_date": "2026-04-10",
        "end_date": "2026-04-10",
        "generated_at_utc": "2026-04-11T00:00:00Z",
        "games": [
            {
                "gamePk": 123,
                "gameDate": "2026-04-10T23:05:00Z",
                "teams": {
                    "home": {"team": {"name": "Chicago Cubs"}},
                    "away": {"team": {"name": "St. Louis Cardinals"}},
                },
            }
        ],
    }
    boxscores = {
        "source": "mlb_statsapi",
        "season": 2026,
        "start_date": "2026-04-10",
        "end_date": "2026-04-10",
        "generated_at_utc": "2026-04-11T00:00:00Z",
        "games": [
            {
                "gamePk": 123,
                "boxscore": {
                    "teams": {
                        "home": {
                            "team": {"abbreviation": "CHC"},
                            "teamStats": {"batting": {"runs": 5}},
                            "players": {
                                "ID1": {
                                    "person": {"id": 1, "fullName": "Alex Reed"},
                                    "stats": {
                                        "batting": {
                                            "atBats": 4,
                                            "hits": 2,
                                            "homeRuns": 1,
                                            "rbi": 3,
                                        }
                                    },
                                }
                            },
                        },
                        "away": {
                            "team": {"abbreviation": "STL"},
                            "teamStats": {"batting": {"runs": 3}},
                            "players": {
                                "ID2": {
                                    "person": {"id": 2, "fullName": "Nate Cole"},
                                    "stats": {
                                        "batting": {
                                            "atBats": 4,
                                            "hits": 1,
                                            "homeRuns": 0,
                                            "rbi": 1,
                                        }
                                    },
                                }
                            },
                        },
                    }
                },
            }
        ],
        "failures": [],
    }

    schedule_path = bundle_dir / "schedule.json"
    boxscores_path = bundle_dir / "boxscores.json"
    _write_json(schedule_path, schedule)
    _write_json(boxscores_path, boxscores)

    manifest = {
        "bundle_id": bundle_dir.name,
        "generated_at_utc": "2026-04-11T00:00:00Z",
        "start_date": "2026-04-10",
        "files": {
            "schedule": {"path": "schedule.json", "sha256": _sha(schedule_path)},
            "boxscores": {"path": "boxscores.json", "sha256": _sha(boxscores_path)},
        },
    }
    _write_json(bundle_dir / "manifest.json", manifest)

    db = tmp_path / "test.sqlite"
    result = import_bundle(bundle_dir, db)
    assert result["status"] == "imported"
    assert result["inserted_games"] == 1
    assert result["inserted_player_lines"] == 2

    conn = connect(db)
    row = conn.execute(
        "SELECT game_id, home_team, away_team, home_runs, away_runs FROM games"
    ).fetchone()
    conn.close()
    assert dict(row) == {
        "game_id": "123",
        "home_team": "CHC",
        "away_team": "STL",
        "home_runs": 5,
        "away_runs": 3,
    }


def test_import_folder_bundle_rejects_checksum_mismatch(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "mlb_reg_2026_20260411_20260411"
    _write_json(bundle_dir / "schedule.json", {"games": []})
    _write_json(bundle_dir / "boxscores.json", {"games": []})
    _write_json(
        bundle_dir / "manifest.json",
        {
            "bundle_id": bundle_dir.name,
            "generated_at_utc": "2026-04-11T00:00:00Z",
            "files": {
                "schedule": {"path": "schedule.json", "sha256": "bad"},
                "boxscores": {"path": "boxscores.json", "sha256": "bad"},
            },
        },
    )

    db = tmp_path / "test.sqlite"
    try:
        import_bundle(bundle_dir, db)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "checksum" in str(exc).lower()
