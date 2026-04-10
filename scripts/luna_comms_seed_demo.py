#!/usr/bin/env python3

from __future__ import annotations

import argparse

from luna_comms_common import INBOX_DIR, canonical_hash, ensure_runtime_dirs, now_utc_iso, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed demo source events for Luna_Comms portfolio walkthrough.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def make_portfolio_push_event() -> dict:
    seed = {
        "artifact_id": "portfolio-demo-artifact",
        "bundle_id": "portfolio-demo-bundle",
        "commit_sha": "demo-commit-sha-1234",
    }
    event_id = f"demo_portfolio_push__{canonical_hash(seed)[:20]}"
    return {
        "source_event_id": event_id,
        "event_type": "portfolio_push_confirmed",
        "detected_at": now_utc_iso(),
        "source": {
            "artifact_id": seed["artifact_id"],
            "bundle_id": seed["bundle_id"],
            "artifact_type": "portfolio_post_bundle",
            "manifest_path": "exports/approved/portfolio/demo/export_manifest.json",
            "summary": "Demo portfolio update generated for outbound reporting showcase.",
            "push": {
                "required": True,
                "confirmed": True,
                "push_receipt_ref": "exports/staged/email/ledger/portfolio_push_receipts.jsonl",
                "commit_sha": seed["commit_sha"],
                "pushed_at": now_utc_iso(),
                "target_repo_reference": "repos/shortview231.github.io",
            },
        },
        "channel_hints": ["email", "notification_update"],
        "title": "Demo Daily Build Update",
        "summary": "Demo artifact was pushed and is ready for Gmail reporting.",
    }


def make_calendar_event() -> dict:
    seed = {
        "artifact_id": "calendar-demo-artifact",
        "local_event_id": "luna-demo-event-001",
    }
    event_id = f"demo_calendar_sync__{canonical_hash(seed)[:20]}"
    return {
        "source_event_id": event_id,
        "event_type": "calendar_sync_finalized",
        "detected_at": now_utc_iso(),
        "source": {
            "artifact_id": seed["artifact_id"],
            "bundle_id": "calendar-demo-bundle",
            "artifact_type": "calendar_sync_bundle",
            "manifest_path": "exports/approved/calendar_sync/demo/export_manifest.json",
            "summary": "Demo calendar publication is ready.",
            "push": {
                "required": False,
                "confirmed": True,
                "push_receipt_ref": "",
                "commit_sha": "",
            },
            "calendar_command": {
                "command": {
                    "action": "upsert",
                    "calendar_id": "primary",
                    "local_event_id": seed["local_event_id"],
                    "summary": "Luna Demo Schedule Update",
                    "description": "Demo calendar sync from Luna_Comms portfolio workflow.",
                    "location": "Remote",
                    "start_at": "2026-04-15T16:00:00Z",
                    "end_at": "2026-04-15T16:30:00Z",
                }
            },
        },
        "channel_hints": ["calendar_sync", "notification_update"],
        "title": "Demo Calendar Sync",
        "summary": "Demo schedule update prepared for Google Calendar outbound sync.",
    }


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()
    events = [make_portfolio_push_event(), make_calendar_event()]

    paths = []
    for event in events:
        path = INBOX_DIR / f"{event['source_event_id']}.json"
        write_json(path, event)
        paths.append(str(path))

    payload = {"status": "ok", "seeded": len(paths), "paths": paths}
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(f"Seeded {len(paths)} demo source event(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
