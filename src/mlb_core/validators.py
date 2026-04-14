from __future__ import annotations

from .config import discover_paths
from .contract import build_player_batting_rows, build_player_pitching_rows, build_scoreboard_games, build_standings_rows
from .producer import find_latest_core_snapshot, find_latest_presentation_snapshot
from .team_identity import TEAM_IDENTITIES, get_team_identity


def resolve_manifest_files(manifest: dict) -> tuple[str, str, str | None, str | None]:
    files = manifest.get("files") or {}
    schedule_file = files.get("schedule_json")
    boxscores_file = files.get("boxscores_json")
    schedule_sha = None
    boxscores_sha = None
    if not schedule_file and isinstance(files.get("schedule"), dict):
        schedule_file = files["schedule"].get("path")
        schedule_sha = files["schedule"].get("sha256")
    if not boxscores_file and isinstance(files.get("boxscores"), dict):
        boxscores_file = files["boxscores"].get("path")
        boxscores_sha = files["boxscores"].get("sha256")
    checksums = manifest.get("checksums_sha256") or {}
    schedule_sha = schedule_sha or checksums.get(schedule_file or "")
    boxscores_sha = boxscores_sha or checksums.get(boxscores_file or "")
    return schedule_file or "schedule.json", boxscores_file or "boxscores.json", schedule_sha, boxscores_sha


def validate_bundle_manifest(manifest: dict) -> None:
    required = ["bundle_id", "generated_at_utc", "season", "start_date", "end_date", "schema_version", "files"]
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"Manifest missing required fields: {missing}")
    schedule_file, boxscores_file, _, _ = resolve_manifest_files(manifest)
    if not schedule_file or not boxscores_file:
        raise ValueError("Manifest must resolve both schedule and boxscores file paths")


def validate_presentation_manifest(manifest: dict) -> None:
    required = ["teams", "players", "games", "assets"]
    files = manifest.get("files") or {}
    missing = [key for key in required if key not in files]
    if missing:
        raise ValueError(f"Presentation manifest missing required file entries: {missing}")


def validate_team_identity_table() -> None:
    if len(TEAM_IDENTITIES) != 30:
        raise ValueError(f"Expected 30 MLB team identities, found {len(TEAM_IDENTITIES)}")
    for row in TEAM_IDENTITIES:
        for key in ["team_id", "canonical_name", "canonical_abbreviation", "league", "division"]:
            if not row.get(key):
                raise ValueError(f"Incomplete team identity row: {row}")
        get_team_identity(int(row["team_id"]))


def validate_producer_bridge_state(season: int | None = None) -> dict:
    paths = discover_paths()
    core = find_latest_core_snapshot(season=season)
    presentation = find_latest_presentation_snapshot(season=season)
    return {
        "producer_core_root": str(paths.producer_core_root),
        "producer_presentation_root": str(paths.producer_presentation_root),
        "has_core_snapshot": core is not None,
        "core_snapshot_path": str(core.root) if core else None,
        "core_generated_at_utc": (core.manifest.get("generated_at_utc") if core else None),
        "has_presentation_snapshot": presentation is not None,
        "presentation_snapshot_path": str(presentation.root) if presentation else None,
        "presentation_generated_at_utc": (presentation.manifest.get("generated_at_utc") if presentation else None),
    }


def validate_season_contract(con, season: int) -> dict:
    validate_team_identity_table()
    bridge = validate_producer_bridge_state(season)
    standings = build_standings_rows(con, season)
    batting = build_player_batting_rows(con, season, 0)
    pitching = build_player_pitching_rows(con, season, 0)
    if standings:
        first_date = con.execute("SELECT MIN(game_date) FROM games WHERE season=?", [season]).fetchone()[0]
        scoreboard = build_scoreboard_games(con, season, first_date)
    else:
        scoreboard = []
    for row in standings:
        for key in ["team_abbreviation", "league", "division", "division_rank", "pct", "gb"]:
            if key not in row:
                raise ValueError(f"Standings row missing canonical field {key}: {row}")
    return {
        "standings_rows": len(standings),
        "batting_rows": len(batting),
        "pitching_rows": len(pitching),
        "scoreboard_rows": len(scoreboard),
        "producer_bridge": bridge,
    }
