#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from luna_mlb_analytics.ingestion.receiver import receive_mlb_inbox


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Receive MLB folder bundles from artifacts/inbox/mlb into sqlite, "
            "derive stats, and archive/quarantine."
        )
    )
    parser.add_argument("--db", default="luna_mlb.sqlite", help="Path to sqlite DB")
    parser.add_argument(
        "--inbox",
        default="artifacts/inbox/mlb",
        help="Inbox root for bundle folders",
    )
    parser.add_argument(
        "--archive",
        default="artifacts/archive/mlb",
        help="Archive root for processed bundles",
    )
    parser.add_argument(
        "--quarantine",
        default="artifacts/quarantine/mlb",
        help="Quarantine root for failed bundles",
    )
    parser.add_argument(
        "--log-file",
        default="artifacts/logs/mlb/receiver_runs.jsonl",
        help="JSONL run summary path",
    )
    parser.add_argument(
        "--lock-file",
        default="artifacts/state/mlb/receiver.lock",
        help="Lock file to prevent concurrent runs",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/scan only; do not import or move bundles",
    )
    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Reprocess bundle IDs already present in import_ledger by purging prior rows first",
    )
    args = parser.parse_args()

    result = receive_mlb_inbox(
        db_path=args.db,
        inbox_root=args.inbox,
        archive_root=args.archive,
        quarantine_root=args.quarantine,
        log_file=args.log_file,
        lock_file=args.lock_file,
        dry_run=args.dry_run,
        force_reprocess=args.force_reprocess,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
