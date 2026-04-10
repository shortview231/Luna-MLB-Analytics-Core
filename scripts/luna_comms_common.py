#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
COMMS_ROOT = ROOT / "exports" / "staged" / "luna_comms"
INBOX_DIR = COMMS_ROOT / "inbox"
JOBS_ROOT = COMMS_ROOT / "jobs"
STATE_DIRS = {
    "detected": JOBS_ROOT / "detected",
    "prepared": JOBS_ROOT / "prepared",
    "queued": JOBS_ROOT / "queued",
    "sent": JOBS_ROOT / "sent",
    "acknowledged": JOBS_ROOT / "acknowledged",
    "archived": JOBS_ROOT / "archived",
    "failed": JOBS_ROOT / "failed",
    "dead_letter": JOBS_ROOT / "dead_letter",
}
RECEIPTS_ROOT = COMMS_ROOT / "receipts"
LEDGER_DIR = COMMS_ROOT / "ledger"
TRANSITIONS_LEDGER = LEDGER_DIR / "job_transitions.jsonl"
ATTEMPTS_LEDGER = LEDGER_DIR / "send_attempts.jsonl"
DEDUPE_LEDGER = LEDGER_DIR / "dedupe_index.jsonl"
SOURCE_EVENT_LEDGER = LEDGER_DIR / "source_event_index.jsonl"
DEVICE_FEED_DIR = COMMS_ROOT / "feeds" / "device_updates"
DEVICE_LATEST = DEVICE_FEED_DIR / "latest.json"
DEVICE_HISTORY = DEVICE_FEED_DIR / "history.jsonl"

RETRY_BACKOFF_MINUTES = [1, 5, 15, 60, 360]


def now_utc() -> datetime:
    return datetime.now(UTC)


def now_utc_iso() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_runtime_dirs() -> None:
    ensure_dir(INBOX_DIR)
    for state_dir in STATE_DIRS.values():
        ensure_dir(state_dir)
    ensure_dir(RECEIPTS_ROOT / "gmail")
    ensure_dir(RECEIPTS_ROOT / "calendar")
    ensure_dir(RECEIPTS_ROOT / "notifications")
    ensure_dir(LEDGER_DIR)
    ensure_dir(DEVICE_FEED_DIR)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                out.append(payload)
    return out


def canonical_hash(payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def slugify(value: str, max_len: int = 80) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-"
    text = value.lower().replace(" ", "-").replace("_", "-")
    text = "".join(ch if ch in allowed else "-" for ch in text)
    while "--" in text:
        text = text.replace("--", "-")
    text = text.strip("-")
    return (text or "item")[:max_len]


def job_filename(created_at: str, job_type: str, slug: str, idempotency_key: str) -> str:
    stamp = created_at.replace(":", "").replace("-", "").replace("+00:00", "Z")
    return f"{stamp}__{job_type}__{slugify(slug)}__{idempotency_key[:8]}.json"


def compute_idempotency_key(job_type: str, source: dict[str, Any], payload: dict[str, Any]) -> str:
    basis = {
        "job_type": job_type,
        "artifact_id": source.get("artifact_id", ""),
        "bundle_id": source.get("bundle_id", ""),
        "commit_sha": (source.get("push") or {}).get("commit_sha", ""),
        "payload": payload,
    }
    return canonical_hash(basis)


def find_job_path(job_id: str) -> tuple[str, Path] | None:
    for state, state_dir in STATE_DIRS.items():
        candidate = state_dir / f"{job_id}.json"
        if candidate.exists():
            return state, candidate
        matches = list(state_dir.glob(f"*{job_id}*.json"))
        if matches:
            return state, matches[0]
    return None


def transition_job(
    job: dict[str, Any],
    current_path: Path,
    to_state: str,
    *,
    actor: str,
    reason: str,
    receipt_ref: str = "",
    attempt_no: int | None = None,
) -> Path:
    if to_state not in STATE_DIRS:
        raise SystemExit(f"Unsupported state transition target: {to_state}")
    from_state = str(job.get("state") or "") or None
    now = now_utc_iso()
    job["state"] = to_state
    job["updated_at"] = now

    new_path = STATE_DIRS[to_state] / current_path.name
    if current_path.resolve() != new_path.resolve():
        ensure_dir(new_path.parent)
        write_json(current_path, job)
        shutil.move(str(current_path), str(new_path))
    else:
        write_json(new_path, job)

    transition = {
        "transition_id": f"{job.get('job_id','unknown')}:{to_state}:{now}",
        "job_id": job.get("job_id", ""),
        "from_state": from_state,
        "to_state": to_state,
        "at": now,
        "actor": actor,
        "reason": reason,
    }
    if attempt_no is not None:
        transition["attempt_no"] = attempt_no
    if receipt_ref:
        transition["receipt_ref"] = receipt_ref
    append_jsonl(TRANSITIONS_LEDGER, transition)
    emit_device_update(job, transition)
    return new_path


def emit_device_update(job: dict[str, Any], transition: dict[str, Any]) -> None:
    payload = {
        "at": transition["at"],
        "job_id": job.get("job_id", ""),
        "job_type": job.get("job_type", ""),
        "state": transition["to_state"],
        "artifact_id": (job.get("source") or {}).get("artifact_id", ""),
        "bundle_id": (job.get("source") or {}).get("bundle_id", ""),
        "title": (job.get("payload") or {}).get("title") or (job.get("payload") or {}).get("subject") or "",
        "summary": (job.get("payload") or {}).get("summary") or (job.get("source") or {}).get("summary") or "",
    }
    write_json(DEVICE_LATEST, payload)
    append_jsonl(DEVICE_HISTORY, payload)


def append_attempt(job: dict[str, Any], status: str, detail: dict[str, Any]) -> None:
    payload = {
        "at": now_utc_iso(),
        "job_id": job.get("job_id", ""),
        "job_type": job.get("job_type", ""),
        "status": status,
        "attempt": (job.get("status") or {}).get("attempt_count", 0),
        "detail": detail,
    }
    append_jsonl(ATTEMPTS_LEDGER, payload)


def dedupe_exists(idempotency_key: str) -> bool:
    for row in load_jsonl(DEDUPE_LEDGER):
        if row.get("idempotency_key") == idempotency_key and row.get("status") in {"sent", "acknowledged", "archived"}:
            return True
    return False


def mark_dedupe(idempotency_key: str, job_id: str, status: str) -> None:
    append_jsonl(
        DEDUPE_LEDGER,
        {
            "at": now_utc_iso(),
            "idempotency_key": idempotency_key,
            "job_id": job_id,
            "status": status,
        },
    )


def next_retry_at(attempt_count: int) -> str:
    index = min(max(attempt_count, 1), len(RETRY_BACKOFF_MINUTES)) - 1
    dt = now_utc() + timedelta(minutes=RETRY_BACKOFF_MINUTES[index])
    return dt.isoformat().replace("+00:00", "Z")


def retry_ready(job: dict[str, Any]) -> bool:
    next_at = str((job.get("status") or {}).get("next_retry_at") or "").strip()
    if not next_at:
        return True
    try:
        gate = datetime.fromisoformat(next_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    return now_utc() >= gate


def queue_ready(job: dict[str, Any]) -> bool:
    not_before = str((job.get("routing") or {}).get("not_before") or "").strip()
    if not not_before:
        return True
    try:
        gate = datetime.fromisoformat(not_before.replace("Z", "+00:00"))
    except ValueError:
        return True
    return now_utc() >= gate
