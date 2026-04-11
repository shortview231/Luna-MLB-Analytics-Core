from __future__ import annotations


def validate_bundle(bundle: dict) -> None:
    required_top = {"bundle_id", "generated_at", "games"}
    missing = required_top.difference(bundle.keys())
    if missing:
        raise ValueError(f"Bundle missing keys: {sorted(missing)}")

    if not isinstance(bundle["games"], list) or not bundle["games"]:
        raise ValueError("Bundle 'games' must be a non-empty list")

    for idx, game in enumerate(bundle["games"]):
        required_game = {
            "game_id",
            "game_date",
            "home_team",
            "away_team",
            "home_runs",
            "away_runs",
            "players",
        }
        gmissing = required_game.difference(game.keys())
        if gmissing:
            raise ValueError(f"Game[{idx}] missing keys: {sorted(gmissing)}")

        if not isinstance(game["players"], list):
            raise ValueError(f"Game[{idx}] players must be a list")

        for pidx, player in enumerate(game["players"]):
            required_player = {
                "player_id",
                "player_name",
                "team",
                "at_bats",
                "hits",
                "home_runs",
                "rbi",
            }
            pmissing = required_player.difference(player.keys())
            if pmissing:
                raise ValueError(f"Game[{idx}] player[{pidx}] missing keys: {sorted(pmissing)}")
