import json
from pathlib import Path

import duckdb

import mlb_core.presentation as presentation_module
import mlb_core.producer as producer_module
from mlb_core.config import ProjectPaths
from mlb_core.contract import build_boxscore_view, build_scoreboard_games, build_standings_rows
from mlb_core.db import DDL


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _paths(root: Path) -> ProjectPaths:
    return ProjectPaths(
        repo_root=root,
        data_raw=root / "data" / "raw",
        data_stage=root / "data" / "stage",
        data_wh=root / "data" / "warehouse",
        db_path=root / "data" / "warehouse" / "mlb_core.duckdb",
        artifacts=root / "artifacts",
        bridge_root=root / "bridge",
        producer_root=root / "bridge" / "luna_ingestion" / "mlb",
        producer_core_root=root / "bridge" / "luna_ingestion" / "mlb" / "core",
        producer_presentation_root=root / "bridge" / "luna_ingestion" / "mlb" / "presentation",
        legacy_inbox_root=root / "bridge" / "luna_inbox" / "mlb",
        legacy_archive_root=root / "bridge" / "luna_archive" / "mlb",
        legacy_quarantine_root=root / "bridge" / "luna_quarantine" / "mlb",
        export_repo_root=root / "bridge" / "luna_export" / "repos",
    )


def _seed_dashboard_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(DDL)
    con.execute(
        """
        INSERT INTO games VALUES
        (1, '2026-04-12', 2026, 'Final', 138, 'St. Louis Cardinals', 111, 'Boston Red Sox', 3, 1, 10, 'Busch Stadium', '2026-04-12T18:15:00Z', 'hash-1', '2026-04-12T23:00:00Z')
        """
    )
    con.execute(
        """
        INSERT INTO team_game_results VALUES
        (1, 111, 'Boston Red Sox', 138, FALSE, 0, 1, 1, 3, 6, 1, 7, 0.600, 4.50, '2026-04-12T23:00:00Z'),
        (1, 138, 'St. Louis Cardinals', 111, TRUE, 1, 0, 3, 1, 8, 0, 5, 0.800, 3.00, '2026-04-12T23:00:00Z')
        """
    )
    con.execute(
        """
        INSERT INTO player_game_batting VALUES
        (1, 138, 5001, 'Jordan Walker', '100', 'RF', 4, 1, 2, 2, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0.500, 1.250, 1.750, '2026-04-12T23:00:00Z'),
        (1, 111, 5002, 'Rafael Devers', '200', '3B', 4, 0, 1, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0.250, 0.250, 0.500, '2026-04-12T23:00:00Z')
        """
    )
    con.execute(
        """
        INSERT INTO player_game_pitching VALUES
        (1, 138, 6001, 'Sonny Gray', 18, 4, 1, 2, 7, 0, 92, 61, 'W, 1-0', 1.50, '2026-04-12T23:00:00Z'),
        (1, 111, 6002, 'Tanner Houck', 15, 7, 3, 1, 5, 1, 88, 58, 'L, 0-1', 5.40, '2026-04-12T23:00:00Z')
        """
    )
    con.execute(
        """
        INSERT INTO team_season_aggregates VALUES
        (2026, 138, 'St. Louis Cardinals', 3, 2, 1, 12, 8, 4, 0.801, 3.100, NULL, NULL, NULL, NULL, '2026-04-12'),
        (2026, 111, 'Boston Red Sox', 3, 1, 2, 8, 12, -4, 0.702, 4.200, NULL, NULL, NULL, NULL, '2026-04-12')
        """
    )
    con.execute(
        """
        INSERT INTO player_season_aggregates VALUES
        (2026, 5001, 138, 'Jordan Walker', 3, 12, 2, 5, 4, 1, 3, 2, 1, 0, 0, 0, 0, 0, 0.950, 0, 0, 0, 0, 0, 0, NULL, '2026-04-12'),
        (2026, 6001, 138, 'Sonny Gray', 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, 18, 1, 4, 2, 7, 0, 1.500, '2026-04-12')
        """
    )
    return con


def test_presentation_overlay_enriches_contract_without_changing_truth(tmp_path, monkeypatch):
    paths = _paths(tmp_path)
    for path in [
        paths.data_raw,
        paths.data_stage,
        paths.data_wh,
        paths.artifacts,
        paths.producer_core_root,
        paths.producer_presentation_root,
        paths.legacy_inbox_root,
        paths.legacy_archive_root,
        paths.legacy_quarantine_root,
        paths.export_repo_root,
    ]:
        path.mkdir(parents=True, exist_ok=True)

    presentation_root = paths.producer_presentation_root / "snapshot_20260412T230000Z"
    _write_json(
        presentation_root / "manifest.json",
        {
            "generated_at_utc": "2026-04-12T23:00:00Z",
            "season": 2026,
            "files": {"teams": "teams.json", "players": "players.json", "games": "games.json", "assets": "assets.json"},
        },
    )
    _write_json(
        presentation_root / "teams.json",
        [
            {"team_id": 138, "display_name": "Cardinals", "primary_color": "#C41E3A", "logo_asset_id": "team-stl-logo"},
            {"team_id": 111, "display_name": "Red Sox", "primary_color": "#BD3039"},
        ],
    )
    _write_json(
        presentation_root / "players.json",
        [
            {"player_id": 5001, "display_name": "J. Walker", "headshot_asset_id": "player-5001-headshot"},
            {"player_id": 6001, "display_name": "S. Gray"},
        ],
    )
    _write_json(
        presentation_root / "games.json",
        [
            {
                "game_pk": 1,
                "venue_display_name": "Busch Stadium, St. Louis",
                "winning_pitcher_display": "Sonny Gray",
                "losing_pitcher_display": "Tanner Houck",
                "save_pitcher_display": "Ryan Helsley",
                "linescore": [{"inning": 1, "away": 0, "home": 1}],
                "scoring_summary": [{"inning": "1st", "team": "STL", "description": "Walker homered to left."}],
            }
        ],
    )
    _write_json(
        presentation_root / "assets.json",
        [
            {"asset_id": "team-stl-logo", "path": "assets/team/stl/logo.svg"},
            {"asset_id": "player-5001-headshot", "path": "assets/player/5001.png"},
        ],
    )

    monkeypatch.setattr(producer_module, "discover_paths", lambda: paths)
    monkeypatch.setattr(presentation_module, "find_latest_presentation_snapshot", lambda season=None: producer_module.find_latest_presentation_snapshot(season=season))
    presentation_module.get_presentation_snapshot.cache_clear()

    con = _seed_dashboard_db()
    standings = build_standings_rows(con, 2026)
    scoreboard = build_scoreboard_games(con, 2026, "2026-04-12", favorite_team_id=138)
    boxscore = build_boxscore_view(con, 2026, 1)

    cardinals = next(row for row in standings if int(row["team_id"]) == 138)
    assert cardinals["wins"] == 2
    assert cardinals["team_display_name"] == "Cardinals"
    assert cardinals["team_logo_path"] == "assets/team/stl/logo.svg"

    assert scoreboard[0]["home_score"] == 3
    assert scoreboard[0]["home_display_name"] == "Cardinals"
    assert scoreboard[0]["venue_display_name"] == "Busch Stadium, St. Louis"
    assert scoreboard[0]["winning_pitcher_display"] == "Sonny Gray"

    assert boxscore["game"]["home_score"] == 3
    assert boxscore["game"]["home_display_name"] == "Cardinals"
    assert boxscore["game"]["save_pitcher_display"] == "Ryan Helsley"
    walker_row = next(row for row in boxscore["batting_rows"] if int(row["player_id"]) == 5001)
    assert walker_row["player_display_name"] == "J. Walker"


def test_contract_operates_without_presentation_snapshot(tmp_path, monkeypatch):
    paths = _paths(tmp_path)
    monkeypatch.setattr(producer_module, "discover_paths", lambda: paths)
    monkeypatch.setattr(presentation_module, "find_latest_presentation_snapshot", lambda season=None: None)
    presentation_module.get_presentation_snapshot.cache_clear()

    con = _seed_dashboard_db()
    scoreboard = build_scoreboard_games(con, 2026, "2026-04-12", favorite_team_id=138)
    boxscore = build_boxscore_view(con, 2026, 1)

    assert scoreboard[0]["home_team_name"] == "St. Louis Cardinals"
    assert scoreboard[0].get("home_display_name") is None
    assert boxscore["game"]["home_team_name"] == "St. Louis Cardinals"
    assert boxscore["game"].get("scoring_summary") == []
