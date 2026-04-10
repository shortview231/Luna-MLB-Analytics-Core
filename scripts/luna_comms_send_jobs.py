#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import os
from datetime import UTC, datetime
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build

from luna_comms_common import (
    RECEIPTS_ROOT,
    STATE_DIRS,
    append_attempt,
    dedupe_exists,
    ensure_runtime_dirs,
    load_json,
    mark_dedupe,
    next_retry_at,
    now_utc_iso,
    transition_job,
    write_json,
)

GMAIL_SEND_SCOPE = "https://www.googleapis.com/auth/gmail.send"
CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar.events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send queued Luna_Comms outbound jobs.")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--enable-calendar-live", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Simulate sends and write dry-run receipts without provider API calls.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser.parse_args()


def now_stamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def load_creds(required_scope: str) -> UserCredentials:
    token_path = os.getenv("GOOGLE_OAUTH_TOKEN_PATH", "").strip()
    if not token_path:
        raise RuntimeError("Missing GOOGLE_OAUTH_TOKEN_PATH")
    creds = UserCredentials.from_authorized_user_file(token_path, [required_scope])
    scopes = set(creds.scopes or [])
    if required_scope not in scopes:
        raise RuntimeError(f"OAuth token missing required scope: {required_scope}")
    return creds


def resolve_env_tokens(addresses: list[str]) -> list[str]:
    out: list[str] = []
    for value in addresses:
        if value.startswith("$ENV:"):
            env_name = value.split(":", 1)[1]
            env_value = os.getenv(env_name, "").strip()
            if env_value:
                out.append(env_value)
        elif value.strip():
            out.append(value.strip())
    return out


def send_email(job: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    recipients = resolve_env_tokens(list(payload.get("to") or []))
    if dry_run and not recipients:
        recipients = ["demo@example.com"]
    if not recipients:
        raise RuntimeError("Email job has no recipient after env resolution")

    subject = str(payload.get("subject") or "Luna report")
    body = str(payload.get("body_text") or "")
    from_header = os.getenv("LUNA_DAILY_REPORT_EMAIL_FROM", "").strip()

    msg = MIMEText(body)
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    if from_header:
        msg["From"] = from_header

    if dry_run:
        message_id = f"dryrun-{now_stamp().lower()}"
    else:
        creds = load_creds(GMAIL_SEND_SCOPE)
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
        message_id = str(result.get("id") or "")

    receipt = {
        "status": "dry_run" if dry_run else "sent",
        "sent_at": now_utc_iso(),
        "job_id": str(job.get("job_id") or ""),
        "job_type": "email",
        "message_id": message_id,
        "recipients": recipients,
        "subject": subject,
    }
    receipt_path = RECEIPTS_ROOT / "gmail" / f"{job.get('job_id','job')}__attempt{(job.get('status') or {}).get('attempt_count',0)}__{message_id or now_stamp()}.json"
    write_json(receipt_path, receipt)
    return {"receipt_path": str(receipt_path), "external_id": message_id}


def send_calendar(job: dict[str, Any], *, live: bool) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    if not live:
        raise RuntimeError("Calendar live send is disabled. Re-run with --enable-calendar-live")

    creds = load_creds(CALENDAR_SCOPE)
    service = build("calendar", "v3", credentials=creds, cache_discovery=False)

    action = str(payload.get("action") or "upsert")
    calendar_id = str(payload.get("calendar_id") or "primary")
    local_event_id = str(payload.get("local_event_id") or "")
    google_event_id = str(payload.get("google_event_id") or "").strip()
    external_id = google_event_id or local_event_id

    if action == "cancel":
        if not external_id:
            raise RuntimeError("Calendar cancel requires google_event_id or local_event_id")
        service.events().delete(calendarId=calendar_id, eventId=external_id).execute()
    else:
        body = {
            "summary": str(payload.get("summary") or "Luna Schedule Update"),
            "description": str(payload.get("description") or ""),
            "location": str(payload.get("location") or ""),
            "start": {"dateTime": str(payload.get("start_at") or ""), "timeZone": str(payload.get("timezone") or "UTC")},
            "end": {"dateTime": str(payload.get("end_at") or ""), "timeZone": str(payload.get("timezone") or "UTC")},
        }
        if external_id:
            result = service.events().update(calendarId=calendar_id, eventId=external_id, body=body).execute()
        else:
            result = service.events().insert(calendarId=calendar_id, body=body).execute()
            external_id = str(result.get("id") or "")

    receipt = {
        "status": "sent",
        "sent_at": now_utc_iso(),
        "job_id": str(job.get("job_id") or ""),
        "job_type": "calendar_sync",
        "calendar_id": calendar_id,
        "action": action,
        "event_id": external_id,
    }
    receipt_path = RECEIPTS_ROOT / "calendar" / f"{job.get('job_id','job')}__attempt{(job.get('status') or {}).get('attempt_count',0)}__{external_id or now_stamp()}.json"
    write_json(receipt_path, receipt)
    return {"receipt_path": str(receipt_path), "external_id": external_id}


def send_calendar_dry_run(job: dict[str, Any]) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    external_id = str(payload.get("google_event_id") or payload.get("local_event_id") or f"dryrun-{now_stamp().lower()}")
    receipt = {
        "status": "dry_run",
        "sent_at": now_utc_iso(),
        "job_id": str(job.get("job_id") or ""),
        "job_type": "calendar_sync",
        "calendar_id": str(payload.get("calendar_id") or "primary"),
        "action": str(payload.get("action") or "upsert"),
        "event_id": external_id,
    }
    receipt_path = RECEIPTS_ROOT / "calendar" / f"{job.get('job_id','job')}__attempt{(job.get('status') or {}).get('attempt_count',0)}__{external_id}.json"
    write_json(receipt_path, receipt)
    return {"receipt_path": str(receipt_path), "external_id": external_id}


def send_notification(job: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    receipt = {
        "status": "dry_run" if dry_run else "sent",
        "sent_at": now_utc_iso(),
        "job_id": str(job.get("job_id") or ""),
        "job_type": "notification_update",
        "target": str(payload.get("target") or "device_feed"),
        "title": str(payload.get("title") or ""),
    }
    receipt_path = RECEIPTS_ROOT / "notifications" / f"{job.get('job_id','job')}__attempt{(job.get('status') or {}).get('attempt_count',0)}__{now_stamp()}.json"
    write_json(receipt_path, receipt)
    return {"receipt_path": str(receipt_path), "external_id": str(payload.get("target") or "device_feed")}


def process_job(path: Path, *, enable_calendar_live: bool, dry_run: bool) -> str:
    job = load_json(path)
    if not isinstance(job, dict):
        return "skipped"

    idempotency_key = str(job.get("idempotency_key") or "")
    if idempotency_key and dedupe_exists(idempotency_key):
        new_path = transition_job(job, path, "acknowledged", actor="luna_comms_send", reason="duplicate_prevented")
        archived_job = load_json(new_path)
        transition_job(archived_job, new_path, "archived", actor="luna_comms_send", reason="duplicate_prevented_archive")
        mark_dedupe(idempotency_key, str(job.get("job_id") or ""), "archived")
        append_attempt(job, "deduped", {"reason": "idempotency_key_already_sent"})
        return "deduped"

    status = dict(job.get("status") or {})
    status["attempt_count"] = int(status.get("attempt_count") or 0) + 1
    status["last_attempt_at"] = now_utc_iso()
    status["last_error_code"] = None
    status["last_error_message"] = None
    status["next_retry_at"] = None
    job["status"] = status
    write_json(path, job)

    try:
        job_type = str(job.get("job_type") or "")
        if job_type == "email":
            result = send_email(job, dry_run=dry_run)
        elif job_type == "calendar_sync":
            result = send_calendar_dry_run(job) if dry_run else send_calendar(job, live=enable_calendar_live)
        elif job_type == "notification_update":
            result = send_notification(job, dry_run=dry_run)
        else:
            raise RuntimeError(f"Unsupported job_type: {job_type}")

        sent_path = transition_job(
            job,
            path,
            "sent",
            actor="luna_comms_send",
            reason="send_success_dry_run" if dry_run else "send_success",
            receipt_ref=str(result.get("receipt_path") or ""),
            attempt_no=int(status["attempt_count"]),
        )
        sent_job = load_json(sent_path)
        sent_job_status = dict(sent_job.get("status") or {})
        sent_job_status["acked_at"] = now_utc_iso()
        sent_job["status"] = sent_job_status
        ack_path = transition_job(
            sent_job,
            sent_path,
            "acknowledged",
            actor="luna_comms_send",
            reason="provider_acknowledged_dry_run" if dry_run else "provider_acknowledged",
            receipt_ref=str(result.get("receipt_path") or ""),
            attempt_no=int(status["attempt_count"]),
        )
        ack_job = load_json(ack_path)
        ack_status = dict(ack_job.get("status") or {})
        ack_status["archived_at"] = now_utc_iso()
        ack_job["status"] = ack_status
        transition_job(ack_job, ack_path, "archived", actor="luna_comms_send", reason="auto_archive")
        if idempotency_key:
            mark_dedupe(idempotency_key, str(job.get("job_id") or ""), "archived")
        append_attempt(job, "sent", result)
        return "sent"
    except Exception as exc:  # noqa: BLE001
        max_attempts = int((job.get("routing") or {}).get("max_attempts") or 5)
        status["last_error_code"] = "send_failed"
        status["last_error_message"] = str(exc)
        status["next_retry_at"] = next_retry_at(int(status["attempt_count"]))
        job["status"] = status
        write_json(path, job)

        if int(status["attempt_count"]) >= max_attempts:
            transition_job(
                job,
                path,
                "dead_letter",
                actor="luna_comms_send",
                reason="max_attempts_exhausted",
                attempt_no=int(status["attempt_count"]),
            )
            append_attempt(job, "dead_letter", {"error": str(exc), "max_attempts": max_attempts})
            return "dead_letter"

        transition_job(
            job,
            path,
            "failed",
            actor="luna_comms_send",
            reason="send_failed_retryable",
            attempt_no=int(status["attempt_count"]),
        )
        append_attempt(job, "failed", {"error": str(exc), "next_retry_at": status["next_retry_at"]})
        return "failed"


def main() -> int:
    args = parse_args()
    ensure_runtime_dirs()

    sent = 0
    failed = 0
    dead = 0
    deduped = 0

    queued_paths = sorted(STATE_DIRS["queued"].glob("*.json"))[: max(args.limit, 0)]
    for path in queued_paths:
        result = process_job(path, enable_calendar_live=args.enable_calendar_live, dry_run=args.dry_run)
        if result == "sent":
            sent += 1
        elif result == "failed":
            failed += 1
        elif result == "dead_letter":
            dead += 1
        elif result == "deduped":
            deduped += 1

    payload = {
        "status": "ok" if failed == 0 and dead == 0 else "partial_failure",
        "attempted": len(queued_paths),
        "sent": sent,
        "failed": failed,
        "deadLetter": dead,
        "deduped": deduped,
    }
    if args.as_json:
        import json

        print(json.dumps(payload))
    else:
        print(
            f"Send pass complete: attempted={payload['attempted']} sent={sent} "
            f"failed={failed} dead_letter={dead} deduped={deduped}"
        )
    return 0 if dead == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
