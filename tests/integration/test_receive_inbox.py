import json
from hashlib import sha256
from pathlib import Path

from luna_mlb_analytics.ingestion.receiver import receive_mlb_inbox
from luna_mlb_analytics.storage.db import connect


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _make_bundle(inbox: Path, bundle_id: str, corrupt_checksums: bool = False) -> Path:
    bundle_dir = inbox / bundle_id
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
                                        "batting": {"atBats": 4, "hits": 2, "homeRuns": 1, "rbi": 3}
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
                                        "batting": {"atBats": 4, "hits": 1, "homeRuns": 0, "rbi": 1}
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
        "bundle_id": bundle_id,
        "generated_at_utc": "2026-04-11T00:00:00Z",
        "start_date": "2026-04-10",
        "files": {
            "schedule": {"path": "schedule.json", "sha256": _sha(schedule_path)},
            "boxscores": {"path": "boxscores.json", "sha256": _sha(boxscores_path)},
        },
    }
    if corrupt_checksums:
        manifest["files"]["schedule"]["sha256"] = "bad"

    _write_json(bundle_dir / "manifest.json", manifest)
    return bundle_dir


def test_receive_inbox_imports_and_archives(tmp_path: Path) -> None:
    inbox = tmp_path / "artifacts" / "inbox" / "mlb"
    archive = tmp_path / "artifacts" / "archive" / "mlb"
    quarantine = tmp_path / "artifacts" / "quarantine" / "mlb"
    logs = tmp_path / "artifacts" / "logs" / "mlb" / "receiver_runs.jsonl"
    lock = tmp_path / "artifacts" / "state" / "mlb" / "receiver.lock"
    db = tmp_path / "test.sqlite"

    bundle_id = "mlb_reg_2026_20260410_20260410"
    _make_bundle(inbox, bundle_id)

    result = receive_mlb_inbox(
        db_path=db,
        inbox_root=inbox,
        archive_root=archive,
        quarantine_root=quarantine,
        log_file=logs,
        lock_file=lock,
    )

    assert result["imported"] == 1
    assert result["failed"] == 0
    assert not (inbox / bundle_id).exists()
    assert (archive / bundle_id).exists()
    assert (archive / bundle_id / "_receipt.json").exists()

    conn = connect(db)
    games_count = conn.execute("SELECT COUNT(*) AS c FROM games").fetchone()["c"]
    ledger_count = conn.execute("SELECT COUNT(*) AS c FROM import_ledger").fetchone()["c"]
    conn.close()
    assert games_count == 1
    assert ledger_count == 1
    assert logs.exists()


def test_receive_inbox_quarantines_invalid_bundle(tmp_path: Path) -> None:
    inbox = tmp_path / "artifacts" / "inbox" / "mlb"
    archive = tmp_path / "artifacts" / "archive" / "mlb"
    quarantine = tmp_path / "artifacts" / "quarantine" / "mlb"
    logs = tmp_path / "artifacts" / "logs" / "mlb" / "receiver_runs.jsonl"
    lock = tmp_path / "artifacts" / "state" / "mlb" / "receiver.lock"
    db = tmp_path / "test.sqlite"

    bundle_id = "mlb_reg_2026_20260411_20260411"
    _make_bundle(inbox, bundle_id, corrupt_checksums=True)

    result = receive_mlb_inbox(
        db_path=db,
        inbox_root=inbox,
        archive_root=archive,
        quarantine_root=quarantine,
        log_file=logs,
        lock_file=lock,
    )

    assert result["failed"] == 1
    assert not (inbox / bundle_id).exists()
    assert (quarantine / bundle_id).exists()
    assert (quarantine / bundle_id / "_quarantine.json").exists()


def test_receive_inbox_dry_run_keeps_inbox(tmp_path: Path) -> None:
    inbox = tmp_path / "artifacts" / "inbox" / "mlb"
    archive = tmp_path / "artifacts" / "archive" / "mlb"
    quarantine = tmp_path / "artifacts" / "quarantine" / "mlb"
    logs = tmp_path / "artifacts" / "logs" / "mlb" / "receiver_runs.jsonl"
    lock = tmp_path / "artifacts" / "state" / "mlb" / "receiver.lock"
    db = tmp_path / "test.sqlite"

    bundle_id = "mlb_reg_2026_20260412_20260412"
    _make_bundle(inbox, bundle_id)

    result = receive_mlb_inbox(
        db_path=db,
        inbox_root=inbox,
        archive_root=archive,
        quarantine_root=quarantine,
        log_file=logs,
        lock_file=lock,
        dry_run=True,
    )

    assert result["processed"] == 1
    assert result["imported"] == 0
    assert (inbox / bundle_id).exists()
    assert not (archive / bundle_id).exists()
    assert not (quarantine / bundle_id).exists()
