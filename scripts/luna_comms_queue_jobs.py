#!/usr/bin/env python3

from __future__ import annotations

import argparse

from luna_comms_common import STATE_DIRS, ensure_runtime_dirs, load_json, queue_ready, retry_ready, transition_job


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue Luna_Comms prepared and retry-ready failed jobs.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def queue_from_prepared() -> int:
    moved = 0
    for path in sorted(STATE_DIRS["prepared"].glob("*.json")):
        job = load_json(path)
        if not isinstance(job, dict):
            continue
        if not queue_ready(job):
            continue
        transition_job(job, path, "queued", actor="luna_comms_queue", reason="ready_to_send")
        moved += 1
    return moved


def queue_retries_from_failed() -> int:
    moved = 0
    for path in sorted(STATE_DIRS["failed"].glob("*.json")):
        job = load_json(path)
        if not isinstance(job, dict):
            continue
        if not retry_ready(job):
            continue
        transition_job(job, path, "queued", actor="luna_comms_queue", reason="retry_ready")
        moved += 1
    return moved


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()
    prepared_moved = queue_from_prepared()
    retry_moved = queue_retries_from_failed()
    payload = {
        "status": "ok",
        "queuedFromPrepared": prepared_moved,
        "queuedFromFailed": retry_moved,
        "queuedDir": str(STATE_DIRS["queued"]),
    }
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(f"Queued {prepared_moved + retry_moved} job(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
