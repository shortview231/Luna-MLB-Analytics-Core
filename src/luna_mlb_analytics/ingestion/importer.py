from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from luna_mlb_analytics.ingestion.bundle_schema import validate_bundle
from luna_mlb_analytics.storage.db import connect, initialize_schema


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _team_abbrev(
    team_payload: dict[str, Any],
    schedule_game: dict[str, Any] | None,
    side: str,
) -> str:
    team = team_payload.get("team") if isinstance(team_payload, dict) else {}
    if isinstance(team, dict):
        abbrev = str(team.get("abbreviation") or "").strip()
        if abbrev:
            return abbrev
        name = str(team.get("name") or "").strip()
        if name:
            return name

    if isinstance(schedule_game, dict):
        teams = schedule_game.get("teams") if isinstance(schedule_game, dict) else {}
        side_payload = teams.get(side, {}) if isinstance(teams, dict) else {}
        team_obj = side_payload.get("team", {}) if isinstance(side_payload, dict) else {}
        name = str(team_obj.get("name") or "").strip()
        if name:
            return name

    return "UNK"


def _extract_players(
    game_id: str, team_block: dict[str, Any], team_code: str
) -> list[dict[str, Any]]:
    players = team_block.get("players", {}) if isinstance(team_block, dict) else {}
    if not isinstance(players, dict):
        return []

    rows: list[dict[str, Any]] = []
    for value in players.values():
        if not isinstance(value, dict):
            continue
        person = value.get("person", {}) if isinstance(value.get("person"), dict) else {}
        stats = value.get("stats", {}) if isinstance(value.get("stats"), dict) else {}
        batting = stats.get("batting", {}) if isinstance(stats.get("batting"), dict) else {}

        player_id = str(person.get("id") or "").strip()
        if not player_id:
            continue

        rows.append(
            {
                "game_id": game_id,
                "player_id": player_id,
                "player_name": str(person.get("fullName") or "Unknown").strip() or "Unknown",
                "team": team_code,
                "at_bats": _to_int(batting.get("atBats"), 0),
                "hits": _to_int(batting.get("hits"), 0),
                "home_runs": _to_int(batting.get("homeRuns"), 0),
                "rbi": _to_int(batting.get("rbi"), 0),
            }
        )
    return rows


def _verify_checksum(file_path: Path, expected_sha: str) -> None:
    actual = hashlib.sha256(file_path.read_bytes()).hexdigest()
    if actual != expected_sha:
        raise ValueError(
            f"Checksum mismatch for {file_path.name}: expected={expected_sha} actual={actual}"
        )


def _load_folder_bundle(bundle_dir: Path) -> dict[str, Any]:
    manifest_path = bundle_dir / "manifest.json"
    schedule_path = bundle_dir / "schedule.json"
    boxscores_path = bundle_dir / "boxscores.json"

    for required in (manifest_path, schedule_path, boxscores_path):
        if not required.exists():
            raise ValueError(f"Missing required bundle file: {required}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
    boxscores = json.loads(boxscores_path.read_text(encoding="utf-8"))

    files_obj = manifest.get("files", {}) if isinstance(manifest.get("files"), dict) else {}
    schedule_meta = (
        files_obj.get("schedule", {})
        if isinstance(files_obj.get("schedule"), dict)
        else {}
    )
    boxscores_meta = (
        files_obj.get("boxscores", {})
        if isinstance(files_obj.get("boxscores"), dict)
        else {}
    )

    expected_schedule_sha = str(schedule_meta.get("sha256") or "").strip()
    expected_boxscores_sha = str(boxscores_meta.get("sha256") or "").strip()
    if not expected_schedule_sha or not expected_boxscores_sha:
        raise ValueError("Manifest missing required checksums for schedule/boxscores")

    _verify_checksum(schedule_path, expected_schedule_sha)
    _verify_checksum(boxscores_path, expected_boxscores_sha)

    bundle_id = str(manifest.get("bundle_id") or "").strip()
    generated_at = str(manifest.get("generated_at_utc") or "").strip()
    if not bundle_id:
        raise ValueError("manifest.json missing bundle_id")
    if not generated_at:
        generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    schedule_games = schedule.get("games", []) if isinstance(schedule.get("games"), list) else []
    schedule_by_pk: dict[int, dict[str, Any]] = {}
    for game in schedule_games:
        if not isinstance(game, dict):
            continue
        game_pk = game.get("gamePk")
        if game_pk is None:
            continue
        schedule_by_pk[_to_int(game_pk, -1)] = game

    converted_games: list[dict[str, Any]] = []
    boxscore_games = boxscores.get("games", []) if isinstance(boxscores.get("games"), list) else []
    for item in boxscore_games:
        if not isinstance(item, dict):
            continue
        game_pk = _to_int(item.get("gamePk"), -1)
        if game_pk < 0:
            continue

        box = item.get("boxscore", {}) if isinstance(item.get("boxscore"), dict) else {}
        teams = box.get("teams", {}) if isinstance(box.get("teams"), dict) else {}
        home_block = teams.get("home", {}) if isinstance(teams.get("home"), dict) else {}
        away_block = teams.get("away", {}) if isinstance(teams.get("away"), dict) else {}

        schedule_game = schedule_by_pk.get(game_pk)
        game_date_raw = ""
        if isinstance(schedule_game, dict):
            game_date_raw = str(schedule_game.get("gameDate") or "")
        game_date = (
            game_date_raw.split("T", 1)[0]
            if game_date_raw
            else str(manifest.get("start_date") or "")
        )

        home_runs = _to_int(
            ((home_block.get("teamStats") or {}).get("batting") or {}).get("runs"), 0
        )
        away_runs = _to_int(
            ((away_block.get("teamStats") or {}).get("batting") or {}).get("runs"), 0
        )

        home_team = _team_abbrev(home_block, schedule_game, "home")
        away_team = _team_abbrev(away_block, schedule_game, "away")

        game_id = str(game_pk)
        players = _extract_players(game_id, home_block, home_team) + _extract_players(
            game_id, away_block, away_team
        )

        converted_games.append(
            {
                "game_id": game_id,
                "game_date": game_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_runs": home_runs,
                "away_runs": away_runs,
                "players": players,
            }
        )

    return {
        "bundle_id": bundle_id,
        "generated_at": generated_at,
        "games": converted_games,
    }


def _load_bundle(bundle_path: Path) -> dict[str, Any]:
    if bundle_path.is_dir():
        return _load_folder_bundle(bundle_path)
    with bundle_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def import_bundle(bundle_path: str | Path, db_path: str | Path) -> dict:
    bundle_path = Path(bundle_path)
    bundle = _load_bundle(bundle_path)

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
    if rows:
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
    if player_rows:
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
