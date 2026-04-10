#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one full Luna_Comms outbound cycle.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--enable-calendar-live", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def run_script(script: str, *args: str) -> dict:
    cmd = ["python3", str(ROOT / script), *args, "--json"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    payload: dict = {"script": script, "exitCode": result.returncode}
    try:
        parsed = json.loads(result.stdout.strip() or "{}")
        if isinstance(parsed, dict):
            payload["result"] = parsed
    except json.JSONDecodeError:
        payload["stdout"] = result.stdout.strip()
    if result.stderr.strip():
        payload["stderr"] = result.stderr.strip()
    return payload


def main() -> int:
    args = parse_args()
    steps = [
        run_script("luna_comms_detect_events.py"),
        run_script("luna_comms_prepare_jobs.py"),
        run_script("luna_comms_queue_jobs.py"),
        run_script(
            "luna_comms_send_jobs.py",
            "--limit",
            str(max(args.limit, 0)),
            *( ["--dry-run"] if args.dry_run else [] ),
            *( ["--enable-calendar-live"] if args.enable_calendar_live else [] ),
        ),
        run_script("luna_comms_archive_jobs.py"),
    ]

    exit_code = 0 if all(int(step.get("exitCode", 1)) == 0 for step in steps) else 2
    payload = {"status": "ok" if exit_code == 0 else "partial_failure", "steps": steps}
    if args.as_json:
        print(json.dumps(payload))
    else:
        print(f"Luna_Comms cycle finished with status={payload['status']}")
        for step in steps:
            print(f"- {step['script']}: exit={step['exitCode']}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
