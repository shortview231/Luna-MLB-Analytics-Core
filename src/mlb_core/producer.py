from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import discover_paths


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_core_manifest(manifest: dict[str, Any]) -> None:
    required = ["bundle_id", "generated_at_utc", "season", "start_date", "end_date", "schema_version", "files"]
    missing = [key for key in required if key not in manifest]
    if missing:
        raise ValueError(f"Core producer manifest missing required fields: {missing}")


def _validate_presentation_manifest(manifest: dict[str, Any]) -> None:
    files = manifest.get("files") or {}
    required = ["teams", "players", "games", "assets"]
    missing = [key for key in required if key not in files]
    if missing:
        raise ValueError(f"Presentation producer manifest missing required file entries: {missing}")


def _iter_snapshot_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    dirs = [path for path in root.iterdir() if path.is_dir() and (path / "manifest.json").exists()]
    return sorted(dirs, key=lambda path: path.name)


def _manifest_timestamp(manifest: dict[str, Any]) -> str:
    for key in ("generated_at_utc", "generated_at", "snapshot_generated_at_utc"):
        value = str(manifest.get(key) or "").strip()
        if value:
            return value
    return ""


@dataclass(frozen=True)
class CoreSnapshot:
    root: Path
    manifest: dict[str, Any]
    schedule: dict[str, Any]
    boxscores: dict[str, Any]


@dataclass(frozen=True)
class PresentationSnapshot:
    root: Path
    manifest: dict[str, Any]
    teams: dict[int, dict[str, Any]]
    players: dict[int, dict[str, Any]]
    games: dict[int, dict[str, Any]]
    assets: dict[str, dict[str, Any]]


def _resolve_required_file(snapshot_root: Path, manifest: dict[str, Any], logical_name: str, default_name: str) -> Path:
    files = manifest.get("files") or {}
    file_entry = files.get(logical_name)
    if isinstance(file_entry, dict):
        rel = str(file_entry.get("path") or default_name).strip()
    else:
        rel = str(file_entry or default_name).strip()
    path = snapshot_root / rel
    if not path.exists():
        raise FileNotFoundError(f"Missing producer file: {path}")
    return path


def _pick_latest_snapshot(candidates: list[Path], *, season: int | None = None) -> Path | None:
    chosen: tuple[str, Path] | None = None
    for root in candidates:
        manifest = _load_json(root / "manifest.json")
        if season is not None:
            manifest_season = manifest.get("season")
            if manifest_season is not None and int(manifest_season) != int(season):
                continue
        stamp = _manifest_timestamp(manifest) or root.name
        if chosen is None or stamp > chosen[0]:
            chosen = (stamp, root)
    return chosen[1] if chosen else None


def find_latest_core_snapshot(*, season: int | None = None) -> CoreSnapshot | None:
    paths = discover_paths()
    candidates = _iter_snapshot_dirs(paths.producer_core_root)
    if not candidates:
        candidates = _iter_snapshot_dirs(paths.legacy_inbox_root) + _iter_snapshot_dirs(paths.legacy_archive_root)
    root = _pick_latest_snapshot(candidates, season=season)
    if root is None:
        return None
    manifest = _load_json(root / "manifest.json")
    _validate_core_manifest(manifest)
    schedule_path = _resolve_required_file(root, manifest, "schedule", "schedule.json")
    boxscores_path = _resolve_required_file(root, manifest, "boxscores", "boxscores.json")
    return CoreSnapshot(
        root=root,
        manifest=manifest,
        schedule=_load_json(schedule_path),
        boxscores=_load_json(boxscores_path),
    )


def _rows_to_id_map(payload: Any, id_keys: tuple[str, ...]) -> dict[Any, dict[str, Any]]:
    if isinstance(payload, dict):
        if isinstance(payload.get("rows"), list):
            rows = payload["rows"]
        else:
            rows = payload.values()
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []
    mapped: dict[Any, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in id_keys:
            value = row.get(key)
            if value is None:
                continue
            try:
                mapped[int(value)] = row
            except Exception:
                mapped[str(value)] = row
            break
    return mapped


def find_latest_presentation_snapshot(*, season: int | None = None) -> PresentationSnapshot | None:
    paths = discover_paths()
    root = _pick_latest_snapshot(_iter_snapshot_dirs(paths.producer_presentation_root), season=season)
    if root is None:
        return None
    manifest = _load_json(root / "manifest.json")
    _validate_presentation_manifest(manifest)
    teams_path = _resolve_required_file(root, manifest, "teams", "teams.json")
    players_path = _resolve_required_file(root, manifest, "players", "players.json")
    games_path = _resolve_required_file(root, manifest, "games", "games.json")
    assets_path = _resolve_required_file(root, manifest, "assets", "assets.json")
    assets_payload = _load_json(assets_path)
    return PresentationSnapshot(
        root=root,
        manifest=manifest,
        teams=_rows_to_id_map(_load_json(teams_path), ("team_id", "id")),
        players=_rows_to_id_map(_load_json(players_path), ("player_id", "id")),
        games=_rows_to_id_map(_load_json(games_path), ("game_pk", "gamePk", "id")),
        assets=_rows_to_id_map(assets_payload, ("asset_id", "id", "slug")),
    )
