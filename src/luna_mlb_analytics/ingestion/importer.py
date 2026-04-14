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
                "runs": _to_int(batting.get("runs"), 0),
                "at_bats": _to_int(batting.get("atBats"), 0),
                "hits": _to_int(batting.get("hits"), 0),
                "doubles": _to_int(batting.get("doubles"), 0),
                "triples": _to_int(batting.get("triples"), 0),
                "home_runs": _to_int(batting.get("homeRuns"), 0),
                "rbi": _to_int(batting.get("rbi"), 0),
                "base_on_balls": _to_int(batting.get("baseOnBalls"), 0),
                "strike_outs": _to_int(batting.get("strikeOuts"), 0),
                "stolen_bases": _to_int(batting.get("stolenBases"), 0),
                "caught_stealing": _to_int(batting.get("caughtStealing"), 0),
                "hit_by_pitch": _to_int(batting.get("hitByPitch"), 0),
                "sac_flies": _to_int(batting.get("sacFlies"), 0),
                "left_on_base": _to_int(batting.get("leftOnBase"), 0),
            }
        )
    return rows


def _ip_to_outs(value: Any) -> int:
    innings = str(value or "").strip()
    if not innings:
        return 0
    if "." not in innings:
        return _to_int(innings, 0) * 3
    whole_str, frac_str = innings.split(".", 1)
    whole = _to_int(whole_str, 0)
    frac = _to_int(frac_str, 0)
    frac = max(0, min(frac, 2))
    return (whole * 3) + frac


def _extract_pitchers(
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
        pitching = stats.get("pitching", {}) if isinstance(stats.get("pitching"), dict) else {}
        if not pitching:
            continue

        player_id = str(person.get("id") or "").strip()
        if not player_id:
            continue

        rows.append(
            {
                "game_id": game_id,
                "player_id": player_id,
                "player_name": str(person.get("fullName") or "Unknown").strip() or "Unknown",
                "team": team_code,
                "ip_outs": _ip_to_outs(pitching.get("inningsPitched")),
                "h_allowed": _to_int(pitching.get("hits"), 0),
                "er": _to_int(pitching.get("earnedRuns"), 0),
                "bb_allowed": _to_int(pitching.get("baseOnBalls"), 0),
                "so_pitched": _to_int(pitching.get("strikeOuts"), 0),
                "hr_allowed": _to_int(pitching.get("homeRuns"), 0),
                "pitches": _to_int(pitching.get("pitchesThrown"), 0),
                "strikes": _to_int(pitching.get("strikes"), 0),
                "era_game": float(pitching.get("era") or 0.0),
            }
        )
    return rows


def _extract_team_action_lines(
    game_id: str, team_block: dict[str, Any], team_code: str
) -> list[dict[str, Any]]:
    info_rows = team_block.get("info", []) if isinstance(team_block, dict) else []
    if not isinstance(info_rows, list):
        return []
    rows: list[dict[str, Any]] = []
    order = 0
    for entry in info_rows:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title") or "").strip() or "INFO"
        field_list = entry.get("fieldList", [])
        if not isinstance(field_list, list):
            continue
        for field in field_list:
            if not isinstance(field, dict):
                continue
            label = str(field.get("label") or "").strip()
            value = str(field.get("value") or "").strip()
            if not label or not value:
                continue
            rows.append(
                {
                    "game_id": game_id,
                    "team": team_code,
                    "section_title": title,
                    "label": label,
                    "value": value,
                    "sort_order": order,
                }
            )
            order += 1
    return rows


def _extract_team_notes(
    game_id: str, team_block: dict[str, Any], team_code: str
) -> list[dict[str, Any]]:
    notes = team_block.get("note", []) if isinstance(team_block, dict) else []
    if not isinstance(notes, list):
        return []
    rows: list[dict[str, Any]] = []
    order = 0
    for entry in notes:
        if not isinstance(entry, dict):
            continue
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        label = str(entry.get("label") or "").strip() or None
        rows.append(
            {
                "game_id": game_id,
                "team": team_code,
                "note_key": label,
                "note_value": value,
                "sort_order": order,
            }
        )
        order += 1
    return rows


def _extract_player_summaries(
    game_id: str, team_block: dict[str, Any], team_code: str
) -> list[dict[str, Any]]:
    players = team_block.get("players", {}) if isinstance(team_block, dict) else {}
    if not isinstance(players, dict):
        return []
    rows: list[dict[str, Any]] = []
    order = 0
    for value in players.values():
        if not isinstance(value, dict):
            continue
        person = value.get("person", {}) if isinstance(value.get("person"), dict) else {}
        stats = value.get("stats", {}) if isinstance(value.get("stats"), dict) else {}
        batting = stats.get("batting", {}) if isinstance(stats.get("batting"), dict) else {}
        pitching = stats.get("pitching", {}) if isinstance(stats.get("pitching"), dict) else {}
        batting_summary = str(batting.get("summary") or "").strip()
        pitching_summary = str(pitching.get("summary") or "").strip()
        if not batting_summary and not pitching_summary:
            continue
        player_id = str(person.get("id") or "").strip()
        if not player_id:
            continue
        rows.append(
            {
                "game_id": game_id,
                "team": team_code,
                "player_id": player_id,
                "player_name": str(person.get("fullName") or "Unknown").strip() or "Unknown",
                "batting_summary": batting_summary or None,
                "pitching_summary": pitching_summary or None,
                "summary_order": order,
            }
        )
        order += 1
    return rows


def _extract_global_notes(game_id: str, boxscore: dict[str, Any]) -> list[dict[str, Any]]:
    raw_info = boxscore.get("info", []) if isinstance(boxscore, dict) else []
    rows: list[dict[str, Any]] = []
    order = 0
    if isinstance(raw_info, list):
        for entry in raw_info:
            if not isinstance(entry, dict):
                continue
            label = str(entry.get("label") or "").strip()
            value = str(entry.get("value") or "").strip()
            if not label or not value:
                continue
            rows.append(
                {
                    "game_id": game_id,
                    "label": label,
                    "value": value,
                    "sort_order": order,
                }
            )
            order += 1

    pitch_notes = boxscore.get("pitchingNotes", [])
    if isinstance(pitch_notes, list):
        for note in pitch_notes:
            value = str(note or "").strip()
            if not value:
                continue
            rows.append(
                {
                    "game_id": game_id,
                    "label": "Pitching Note",
                    "value": value,
                    "sort_order": order,
                }
            )
            order += 1
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
        pitchers = _extract_pitchers(game_id, home_block, home_team) + _extract_pitchers(
            game_id, away_block, away_team
        )

        action_lines = _extract_team_action_lines(game_id, home_block, home_team)
        action_lines += _extract_team_action_lines(game_id, away_block, away_team)
        team_notes = _extract_team_notes(game_id, home_block, home_team)
        team_notes += _extract_team_notes(game_id, away_block, away_team)
        player_summaries = _extract_player_summaries(game_id, home_block, home_team)
        player_summaries += _extract_player_summaries(game_id, away_block, away_team)
        global_notes = _extract_global_notes(game_id, box)

        converted_games.append(
            {
                "game_id": game_id,
                "game_date": game_date,
                "home_team": home_team,
                "away_team": away_team,
                "home_runs": home_runs,
                "away_runs": away_runs,
                "players": players,
                "pitchers": pitchers,
                "action_lines": action_lines,
                "team_notes": team_notes,
                "player_summaries": player_summaries,
                "global_notes": global_notes,
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
            INSERT OR REPLACE INTO games(
                game_id, game_date, home_team, away_team,
                home_runs, away_runs, source_bundle_id, imported_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    player_rows = []
    pitcher_rows = []
    action_rows = []
    note_rows = []
    player_summary_rows = []
    global_note_rows = []

    def _to_int(player: dict[str, Any], key: str, default: int = 0) -> int:
        value = player.get(key, default)
        if value is None:
            return default
        return int(value)

    def _to_float(player: dict[str, Any], key: str, default: float = 0.0) -> float:
        value = player.get(key, default)
        if value is None:
            return default
        return float(value)

    for g in bundle["games"]:
        for p in g["players"]:
            player_rows.append(
                (
                    g["game_id"],
                    p["player_id"],
                    p["player_name"],
                    p["team"],
                    _to_int(p, "runs"),
                    _to_int(p, "at_bats"),
                    _to_int(p, "hits"),
                    _to_int(p, "doubles"),
                    _to_int(p, "triples"),
                    _to_int(p, "home_runs"),
                    _to_int(p, "rbi"),
                    _to_int(p, "base_on_balls"),
                    _to_int(p, "strike_outs"),
                    _to_int(p, "stolen_bases"),
                    _to_int(p, "caught_stealing"),
                    _to_int(p, "hit_by_pitch"),
                    _to_int(p, "sac_flies"),
                    _to_int(p, "left_on_base"),
                )
            )
        for p in g.get("pitchers", []):
            pitcher_rows.append(
                (
                    g["game_id"],
                    p["player_id"],
                    p["player_name"],
                    p["team"],
                    _to_int(p, "ip_outs"),
                    _to_int(p, "h_allowed"),
                    _to_int(p, "er"),
                    _to_int(p, "bb_allowed"),
                    _to_int(p, "so_pitched"),
                    _to_int(p, "hr_allowed"),
                    _to_int(p, "pitches"),
                    _to_int(p, "strikes"),
                    _to_float(p, "era_game"),
                )
            )
        for row in g.get("action_lines", []):
            action_rows.append(
                (
                    g["game_id"],
                    row.get("team"),
                    row.get("section_title"),
                    row.get("label"),
                    row.get("value"),
                    int(row.get("sort_order") or 0),
                )
            )
        for row in g.get("team_notes", []):
            note_rows.append(
                (
                    g["game_id"],
                    row.get("team"),
                    row.get("note_key"),
                    row.get("note_value"),
                    int(row.get("sort_order") or 0),
                )
            )
        for row in g.get("player_summaries", []):
            player_summary_rows.append(
                (
                    g["game_id"],
                    row.get("team"),
                    row.get("player_id"),
                    row.get("player_name"),
                    row.get("batting_summary"),
                    row.get("pitching_summary"),
                    int(row.get("summary_order") or 0),
                )
            )
        for row in g.get("global_notes", []):
            global_note_rows.append(
                (
                    g["game_id"],
                    row.get("label"),
                    row.get("value"),
                    int(row.get("sort_order") or 0),
                )
            )
    if player_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_players(
                game_id, player_id, player_name, team, runs, at_bats, hits, doubles, triples,
                home_runs, rbi, base_on_balls, strike_outs, stolen_bases, caught_stealing,
                hit_by_pitch, sac_flies, left_on_base
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            player_rows,
        )
    if pitcher_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_pitchers(
                game_id, player_id, player_name, team, ip_outs, h_allowed, er,
                bb_allowed, so_pitched, hr_allowed, pitches, strikes, era_game
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            pitcher_rows,
        )
    if action_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_team_action_lines(
                game_id, team, section_title, label, value, sort_order
            )
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            action_rows,
        )
    if note_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_team_notes(
                game_id, team, note_key, note_value, sort_order
            )
            VALUES(?, ?, ?, ?, ?)
            """,
            note_rows,
        )
    if player_summary_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_player_summaries(
                game_id, team, player_id, player_name, batting_summary, pitching_summary, summary_order
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            player_summary_rows,
        )
    if global_note_rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO game_global_notes(
                game_id, label, value, sort_order
            )
            VALUES(?, ?, ?, ?)
            """,
            global_note_rows,
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
        "inserted_pitching_lines": len(pitcher_rows),
        "inserted_action_lines": len(action_rows),
        "inserted_team_notes": len(note_rows),
        "inserted_player_summaries": len(player_summary_rows),
        "inserted_global_notes": len(global_note_rows),
        "status": "imported",
    }
