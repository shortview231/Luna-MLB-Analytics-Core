from __future__ import annotations

from functools import lru_cache
from typing import Any

from .producer import PresentationSnapshot, find_latest_presentation_snapshot


def _first_present(row: dict[str, Any] | None, keys: tuple[str, ...], default=None):
    if not isinstance(row, dict):
        return default
    for key in keys:
        value = row.get(key)
        if value not in (None, "", []):
            return value
    return default


@lru_cache(maxsize=8)
def get_presentation_snapshot(season: int | None = None) -> PresentationSnapshot | None:
    return find_latest_presentation_snapshot(season=season)


def resolve_asset(snapshot: PresentationSnapshot | None, ref: Any) -> str | None:
    if snapshot is None or ref in (None, ""):
        return None
    key = str(ref)
    row = snapshot.assets.get(key)
    if row is None:
        try:
            row = snapshot.assets.get(int(ref))
        except Exception:
            row = None
    if not isinstance(row, dict):
        return None
    return _first_present(row, ("path", "asset_path", "url", "uri"))


def build_team_overlay(snapshot: PresentationSnapshot | None, team_id: int) -> dict[str, Any]:
    row = snapshot.teams.get(int(team_id), {}) if snapshot else {}
    return {
        "team_display_name": _first_present(row, ("display_name", "name", "short_name")),
        "team_slug": _first_present(row, ("slug",)),
        "team_primary_color": _first_present(row, ("primary_color", "color_primary", "primaryColor")),
        "team_secondary_color": _first_present(row, ("secondary_color", "color_secondary", "secondaryColor")),
        "team_logo_path": resolve_asset(snapshot, _first_present(row, ("logo_asset_id", "logo_asset_ref", "logo"))),
        "team_wordmark_path": resolve_asset(snapshot, _first_present(row, ("wordmark_asset_id", "wordmark_asset_ref", "wordmark"))),
    }


def build_player_overlay(snapshot: PresentationSnapshot | None, player_id: int) -> dict[str, Any]:
    row = snapshot.players.get(int(player_id), {}) if snapshot else {}
    return {
        "player_display_name": _first_present(row, ("display_name", "name")),
        "player_slug": _first_present(row, ("slug",)),
        "player_headshot_path": resolve_asset(snapshot, _first_present(row, ("headshot_asset_id", "headshot_asset_ref", "headshot"))),
    }


def build_game_overlay(snapshot: PresentationSnapshot | None, game_pk: int) -> dict[str, Any]:
    row = snapshot.games.get(int(game_pk), {}) if snapshot else {}
    winning_pitcher = _first_present(row, ("winning_pitcher", "winningPitcher", "winning_pitcher_display"))
    losing_pitcher = _first_present(row, ("losing_pitcher", "losingPitcher", "losing_pitcher_display"))
    save_pitcher = _first_present(row, ("save_pitcher", "savePitcher", "save_pitcher_display"))
    return {
        "venue_display_name": _first_present(row, ("venue_display_name", "venue_name", "venue")),
        "linescore": _first_present(row, ("linescore", "linescore_display"), default=[]),
        "scoring_summary": _first_present(row, ("scoring_summary", "scoring_plays"), default=[]),
        "winning_pitcher_display": winning_pitcher,
        "losing_pitcher_display": losing_pitcher,
        "save_pitcher_display": save_pitcher,
        "top_performers": _first_present(row, ("top_performers", "top_performer_cards"), default=[]),
    }
