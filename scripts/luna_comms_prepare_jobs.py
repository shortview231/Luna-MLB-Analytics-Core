#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

from luna_comms_common import (
    INBOX_DIR,
    STATE_DIRS,
    compute_idempotency_key,
    dedupe_exists,
    ensure_runtime_dirs,
    job_filename,
    load_json,
    now_utc_iso,
    transition_job,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Luna_Comms jobs from source events in inbox.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def resolve_manifest(source: dict) -> dict:
    manifest_path = Path(str(source.get("manifest_path") or "")).expanduser()
    if manifest_path.is_file():
        payload = load_json(manifest_path)
        if isinstance(payload, dict):
            return payload
    return {}


def base_job(source_event: dict, job_type: str, payload: dict) -> dict:
    source = dict(source_event.get("source") or {})
    created_at = now_utc_iso()
    idempotency_key = compute_idempotency_key(job_type, source, payload)
    job_id = f"job_{created_at.replace(':','').replace('-','').replace('+00:00','Z')}_{job_type}_{idempotency_key[:8]}"
    return {
        "job_id": job_id,
        "job_type": job_type,
        "schema_version": "1.0",
        "state": "detected",
        "created_at": created_at,
        "updated_at": created_at,
        "idempotency_key": idempotency_key,
        "correlation_id": str(source_event.get("source_event_id") or ""),
        "source": source,
        "routing": {
            "priority": "normal",
            "not_before": created_at,
            "max_attempts": 5 if job_type != "notification_update" else 3,
        },
        "payload": payload,
        "status": {
            "attempt_count": 0,
            "last_attempt_at": None,
            "last_error_code": None,
            "last_error_message": None,
            "next_retry_at": None,
            "acked_at": None,
            "archived_at": None,
        },
        "metadata": {
            "source_event_type": str(source_event.get("event_type") or ""),
            "source_event_path": str(source_event.get("source_event_path") or ""),
        },
    }


def build_email_job(source_event: dict) -> dict:
    source = dict(source_event.get("source") or {})
    manifest = resolve_manifest(source)
    summary = str(source_event.get("summary") or source.get("summary") or manifest.get("summary") or "")
    title = str(source_event.get("title") or manifest.get("title") or "Daily Luna Report")
    push = source.get("push") or {}

    payload = {
        "to": ["$ENV:LUNA_DAILY_REPORT_EMAIL_TO"],
        "cc": [],
        "bcc": [],
        "subject": f"Luna Daily Report: {title}",
        "body_text": (
            f"{title}\n\n"
            f"Summary:\n{summary}\n\n"
            f"Artifact ID: {source.get('artifact_id', '')}\n"
            f"Bundle ID: {source.get('bundle_id', '')}\n"
            f"Commit SHA: {push.get('commit_sha', '')}\n"
            f"Manifest: {source.get('manifest_path', '')}\n"
        ),
        "embed_summary": True,
        "source_summary_excerpt": summary,
        "attachments": list(manifest.get("related_files") or []),
    }
    return base_job(source_event, "email", payload)


def build_calendar_job(source_event: dict) -> dict | None:
    source = dict(source_event.get("source") or {})
    command = source.get("calendar_command")
    if not isinstance(command, dict):
        return None
    command_payload = command.get("command") if isinstance(command.get("command"), dict) else command
    payload = {
        "action": str(command_payload.get("action") or "upsert"),
        "calendar_id": str(command_payload.get("calendar_id") or "primary"),
        "local_event_id": str(command_payload.get("local_event_id") or source.get("artifact_id") or ""),
        "google_event_id": str(command_payload.get("google_event_id") or ""),
        "summary": str(command_payload.get("summary") or source_event.get("title") or "Luna Schedule Update"),
        "description": str(command_payload.get("description") or source_event.get("summary") or ""),
        "location": str(command_payload.get("location") or ""),
        "start_at": str(command_payload.get("start_at") or ""),
        "end_at": str(command_payload.get("end_at") or ""),
        "timezone": "UTC",
    }
    return base_job(source_event, "calendar_sync", payload)


def build_notification_job(source_event: dict) -> dict:
    source = dict(source_event.get("source") or {})
    payload = {
        "target": "device_feed",
        "title": str(source_event.get("title") or "Luna outbound update"),
        "summary": str(source_event.get("summary") or source.get("summary") or ""),
        "deep_link_refs": [str(source.get("manifest_path") or "")],
        "visibility": "internal",
        "source_refs": [
            str(source.get("artifact_id") or ""),
            str(source.get("bundle_id") or ""),
            str((source.get("push") or {}).get("commit_sha") or ""),
        ],
    }
    return base_job(source_event, "notification_update", payload)


def materialize_job(job: dict) -> bool:
    if dedupe_exists(str(job.get("idempotency_key") or "")):
        return False
    slug = str((job.get("source") or {}).get("bundle_id") or (job.get("source") or {}).get("artifact_id") or job.get("job_id") or "job")
    filename = job_filename(str(job.get("created_at") or now_utc_iso()), str(job.get("job_type") or "job"), slug, str(job.get("idempotency_key") or ""))
    path = STATE_DIRS["detected"] / filename
    write_json(path, job)
    transition_job(job, path, "prepared", actor="luna_comms_prepare", reason="job_prepared")
    return True


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()

    prepared_count = 0
    source_count = 0

    for source_path in sorted(INBOX_DIR.glob("*.json")):
        source_event = load_json(source_path)
        if not isinstance(source_event, dict):
            continue
        source_count += 1

        hints = {str(x) for x in (source_event.get("channel_hints") or [])}
        jobs: list[dict] = []
        if "email" in hints:
            jobs.append(build_email_job(source_event))
        if "calendar_sync" in hints:
            calendar_job = build_calendar_job(source_event)
            if calendar_job:
                jobs.append(calendar_job)
        jobs.append(build_notification_job(source_event))

        for job in jobs:
            if materialize_job(job):
                prepared_count += 1

        source_path.unlink()

    payload = {
        "status": "ok",
        "sourceEventsProcessed": source_count,
        "jobsPrepared": prepared_count,
        "preparedDir": str(STATE_DIRS["prepared"]),
    }
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(f"Processed {source_count} source event(s); prepared {prepared_count} job(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
