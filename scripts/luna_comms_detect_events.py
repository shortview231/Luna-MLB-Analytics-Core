#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from luna_comms_common import (
    ROOT,
    INBOX_DIR,
    SOURCE_EVENT_LEDGER,
    append_jsonl,
    canonical_hash,
    ensure_runtime_dirs,
    load_json,
    load_jsonl,
    now_utc_iso,
    write_json,
)

PORTFOLIO_PUSH_LEDGER = ROOT / "exports" / "staged" / "email" / "ledger" / "portfolio_push_receipts.jsonl"
APPROVED_CALENDAR_DIR = ROOT / "exports" / "approved" / "calendar_sync"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect Luna_Comms source events from finalized export artifacts.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def known_source_event_ids() -> set[str]:
    return {str(row.get("source_event_id") or "") for row in load_jsonl(SOURCE_EVENT_LEDGER) if row.get("source_event_id")}


def write_source_event(event: dict) -> None:
    source_event_id = str(event["source_event_id"])
    event_path = INBOX_DIR / f"{source_event_id}.json"
    write_json(event_path, event)
    append_jsonl(
        SOURCE_EVENT_LEDGER,
        {
            "at": now_utc_iso(),
            "source_event_id": source_event_id,
            "event_type": event.get("event_type", ""),
            "artifact_id": (event.get("source") or {}).get("artifact_id", ""),
            "bundle_id": (event.get("source") or {}).get("bundle_id", ""),
            "push_receipt_ref": (event.get("source") or {}).get("push_receipt_ref", ""),
            "source_event_path": str(event_path),
            "status": "detected",
        },
    )


def detect_portfolio_push_events(existing: set[str]) -> list[dict]:
    events: list[dict] = []
    for row in load_jsonl(PORTFOLIO_PUSH_LEDGER):
        if row.get("event_type") != "portfolio_push":
            continue
        bundle_id = str(row.get("bundle_id") or "").strip()
        commit_sha = str(row.get("commit_sha") or "").strip()
        if not bundle_id or not commit_sha:
            continue
        source_event_id = f"portfolio_push__{canonical_hash({'bundle_id': bundle_id, 'commit_sha': commit_sha})[:20]}"
        if source_event_id in existing:
            continue
        source = {
            "artifact_id": bundle_id,
            "bundle_id": bundle_id,
            "artifact_type": str(row.get("artifact_type") or "portfolio_post_bundle"),
            "manifest_path": str(Path(str(row.get("bundle_root") or "")) / "export_manifest.json"),
            "summary": str(row.get("summary") or ""),
            "push": {
                "required": True,
                "confirmed": True,
                "push_receipt_ref": "exports/staged/email/ledger/portfolio_push_receipts.jsonl",
                "commit_sha": commit_sha,
                "pushed_at": str(row.get("pushed_at") or ""),
                "target_repo_reference": str(row.get("target_repo_reference") or ""),
            },
        }
        event = {
            "source_event_id": source_event_id,
            "event_type": "portfolio_push_confirmed",
            "detected_at": now_utc_iso(),
            "source": source,
            "channel_hints": ["email", "notification_update"],
            "title": str(row.get("title") or ""),
            "summary": str(row.get("summary") or ""),
        }
        events.append(event)
        existing.add(source_event_id)
    return events


def detect_calendar_bundle_events(existing: set[str]) -> list[dict]:
    events: list[dict] = []
    if not APPROVED_CALENDAR_DIR.exists():
        return events

    for bundle_dir in sorted(path for path in APPROVED_CALENDAR_DIR.iterdir() if path.is_dir()):
        manifest_path = bundle_dir / "export_manifest.json"
        command_path = bundle_dir / "calendar_outbound_command.json"
        if not manifest_path.is_file() or not command_path.is_file():
            continue

        manifest = load_json(manifest_path)
        command_payload = load_json(command_path)
        if not isinstance(manifest, dict) or not isinstance(command_payload, dict):
            continue

        source_event_id = f"calendar_bundle__{canonical_hash({'bundle': bundle_dir.name, 'command': command_payload})[:20]}"
        if source_event_id in existing:
            continue

        source = {
            "artifact_id": str(manifest.get("id") or bundle_dir.name),
            "bundle_id": str(manifest.get("id") or bundle_dir.name),
            "artifact_type": str(manifest.get("artifact_type") or "calendar_sync_bundle"),
            "manifest_path": str(manifest_path),
            "summary": str(manifest.get("summary") or ""),
            "push": {
                "required": False,
                "confirmed": True,
                "push_receipt_ref": "",
                "commit_sha": "",
            },
            "calendar_command": command_payload,
        }
        event = {
            "source_event_id": source_event_id,
            "event_type": "calendar_sync_finalized",
            "detected_at": now_utc_iso(),
            "source": source,
            "channel_hints": ["calendar_sync", "notification_update"],
            "title": str(manifest.get("title") or "Calendar sync update"),
            "summary": str(manifest.get("summary") or "Calendar sync command prepared"),
        }
        events.append(event)
        existing.add(source_event_id)
    return events


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()
    existing = known_source_event_ids()
    events = detect_portfolio_push_events(existing)
    events.extend(detect_calendar_bundle_events(existing))

    for event in events:
        write_source_event(event)

    payload = {
        "status": "ok",
        "detected": len(events),
        "inbox": str(INBOX_DIR),
        "sourceEventLedger": str(SOURCE_EVENT_LEDGER),
    }
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(f"Detected {len(events)} new source event(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
