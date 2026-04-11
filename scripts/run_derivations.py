#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from luna_mlb_analytics.transforms.derive import derive_team_and_player_stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Derive team and player stats from ingested games."
    )
    parser.add_argument("--db", default="luna_mlb.sqlite", help="Path to sqlite DB")
    args = parser.parse_args()

    result = derive_team_and_player_stats(args.db)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
