#!/usr/bin/env python3

from __future__ import annotations

import argparse

from luna_comms_common import STATE_DIRS, ensure_runtime_dirs, load_json, mark_dedupe, transition_job


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive acknowledged Luna_Comms jobs.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()

    moved = 0
    for path in sorted(STATE_DIRS["acknowledged"].glob("*.json")):
        job = load_json(path)
        if not isinstance(job, dict):
            continue
        transition_job(job, path, "archived", actor="luna_comms_archive", reason="manual_archive_pass")
        key = str(job.get("idempotency_key") or "")
        if key:
            mark_dedupe(key, str(job.get("job_id") or ""), "archived")
        moved += 1

    payload = {"status": "ok", "archived": moved, "archivedDir": str(STATE_DIRS["archived"])}
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(f"Archived {moved} acknowledged job(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
