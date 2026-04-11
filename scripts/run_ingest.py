#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from luna_mlb_analytics.ingestion.importer import import_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Import an offline MLB bundle into local sqlite.")
    parser.add_argument("--bundle", required=True, help="Path to bundle JSON")
    parser.add_argument("--db", default="luna_mlb.sqlite", help="Path to sqlite DB")
    args = parser.parse_args()

    result = import_bundle(args.bundle, args.db)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
