from __future__ import annotations

import fcntl
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from luna_mlb_analytics.ingestion.importer import import_bundle
from luna_mlb_analytics.storage.db import connect, initialize_schema
from luna_mlb_analytics.transforms.derive import derive_team_and_player_stats


@dataclass(slots=True)
class ReceiverPaths:
    inbox_root: Path
    archive_root: Path
    quarantine_root: Path
    log_file: Path
    lock_file: Path


class ReceiverLock:
    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._handle: Any | None = None

    def __enter__(self) -> "ReceiverLock":
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self._lock_path.open("w", encoding="utf-8")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Receiver is already running (lock: {self._lock_path})") from exc

        self._handle.write(f"pid_lock_acquired_at={datetime.now(UTC).isoformat()}\n")
        self._handle.flush()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._handle is None:
            return
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _list_bundle_dirs(inbox_root: Path) -> list[Path]:
    if not inbox_root.exists():
        return []
    return sorted([p for p in inbox_root.iterdir() if p.is_dir()], key=lambda p: p.name)


def _bundle_id(bundle_dir: Path) -> str:
    manifest_path = bundle_dir / "manifest.json"
    if manifest_path.exists():
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            bundle_id = str(payload.get("bundle_id") or "").strip()
            if bundle_id:
                return bundle_id
        except (ValueError, OSError):
            pass
    return bundle_dir.name


def _bundle_already_imported(db_path: Path, bundle_id: str) -> bool:
    conn = connect(db_path)
    initialize_schema(conn)
    row = conn.execute("SELECT 1 FROM import_ledger WHERE bundle_id = ?", (bundle_id,)).fetchone()
    conn.close()
    return row is not None


def _purge_bundle(db_path: Path, bundle_id: str) -> dict[str, int]:
    conn = connect(db_path)
    initialize_schema(conn)

    game_ids = [
        row["game_id"]
        for row in conn.execute(
            "SELECT game_id FROM games WHERE source_bundle_id = ?",
            (bundle_id,),
        ).fetchall()
    ]

    deleted_player_rows = 0
    if game_ids:
        placeholders = ",".join("?" * len(game_ids))
        deleted_player_rows = conn.execute(
            f"DELETE FROM game_players WHERE game_id IN ({placeholders})", game_ids
        ).rowcount

    deleted_games = conn.execute(
        "DELETE FROM games WHERE source_bundle_id = ?",
        (bundle_id,),
    ).rowcount
    deleted_ledger = conn.execute(
        "DELETE FROM import_ledger WHERE bundle_id = ?",
        (bundle_id,),
    ).rowcount
    conn.commit()
    conn.close()

    return {
        "deleted_games": deleted_games,
        "deleted_player_lines": deleted_player_rows,
        "deleted_ledger_rows": deleted_ledger,
    }


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _safe_reason_text(errors: list[str]) -> str:
    if not errors:
        return "unknown"
    return "; ".join(errors)[:500]


def _write_bundle_receipt(
    receipt_path: Path,
    *,
    bundle_id: str,
    status: str,
    source_path: str,
    errors: list[str],
    result: dict[str, Any] | None,
) -> None:
    payload = {
        "bundle_id": bundle_id,
        "status": status,
        "source_path": source_path,
        "processed_at_utc": _utc_now_iso(),
        "errors": errors,
        "result": result or {},
    }
    receipt_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _move_to_dir(bundle_dir: Path, destination_root: Path) -> Path:
    destination_root.mkdir(parents=True, exist_ok=True)
    target = destination_root / bundle_dir.name

    if target.exists():
        suffix = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        target = destination_root / f"{bundle_dir.name}_{suffix}"

    return bundle_dir.rename(target)


def _process_bundle(
    bundle_dir: Path,
    *,
    db_path: Path,
    paths: ReceiverPaths,
    dry_run: bool,
    force_reprocess: bool,
) -> dict[str, Any]:
    bundle_id = _bundle_id(bundle_dir)
    started = _utc_now_iso()
    result: dict[str, Any] = {
        "timestamp_utc": started,
        "bundle_id": bundle_id,
        "bundle_dir": str(bundle_dir),
        "status": "started",
        "inserted_games": 0,
        "inserted_player_lines": 0,
        "stages": {
            "scan": "ok",
            "idempotency": "pending",
            "import": "pending",
            "derive": "pending",
            "finalize": "pending",
        },
        "errors": [],
    }

    try:
        already = _bundle_already_imported(db_path, bundle_id)
        if already and not force_reprocess:
            result["stages"]["idempotency"] = "already_imported"
            if dry_run:
                result["status"] = "dry_run"
                result["stages"]["import"] = "skipped_dry_run"
                result["stages"]["derive"] = "skipped_dry_run"
                result["stages"]["finalize"] = "kept_inbox"
                return result

            result["status"] = "already_imported"
            result["stages"]["import"] = "skipped"
            result["stages"]["derive"] = "skipped"
            if not dry_run:
                archived_dir = _move_to_dir(bundle_dir, paths.archive_root)
                _write_bundle_receipt(
                    archived_dir / "_receipt.json",
                    bundle_id=bundle_id,
                    status=result["status"],
                    source_path=str(bundle_dir),
                    errors=[],
                    result=result,
                )
                result["archive_path"] = str(archived_dir)
            result["stages"]["finalize"] = "archived"
            return result

        result["stages"]["idempotency"] = "force_reprocess" if force_reprocess else "new"

        if dry_run:
            result["status"] = "dry_run"
            result["stages"]["import"] = "skipped_dry_run"
            result["stages"]["derive"] = "skipped_dry_run"
            result["stages"]["finalize"] = "kept_inbox"
            return result

        if already and force_reprocess:
            result["purged"] = _purge_bundle(db_path, bundle_id)

        import_result = import_bundle(bundle_dir, db_path)
        result["import_result"] = import_result
        result["inserted_games"] = int(import_result.get("inserted_games", 0))
        result["inserted_player_lines"] = int(import_result.get("inserted_player_lines", 0))
        result["stages"]["import"] = import_result.get("status", "imported")

        derive_result = derive_team_and_player_stats(str(db_path))
        result["derive_result"] = derive_result
        result["stages"]["derive"] = "ok"

        archived_dir = _move_to_dir(bundle_dir, paths.archive_root)
        _write_bundle_receipt(
            archived_dir / "_receipt.json",
            bundle_id=bundle_id,
            status="imported",
            source_path=str(bundle_dir),
            errors=[],
            result=result,
        )
        result["archive_path"] = str(archived_dir)
        result["status"] = "imported"
        result["stages"]["finalize"] = "archived"
        return result

    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["errors"].append(str(exc))

        if dry_run:
            result["stages"]["finalize"] = "kept_inbox"
            return result

        quarantine_dir = _move_to_dir(bundle_dir, paths.quarantine_root)
        reason_payload = {
            "bundle_id": bundle_id,
            "quarantined_at_utc": _utc_now_iso(),
            "reason": _safe_reason_text(result["errors"]),
            "errors": result["errors"],
            "source_path": str(bundle_dir),
        }
        (quarantine_dir / "_quarantine.json").write_text(
            json.dumps(reason_payload, indent=2) + "\n", encoding="utf-8"
        )
        result["quarantine_path"] = str(quarantine_dir)
        result["stages"]["finalize"] = "quarantined"
        return result


def receive_mlb_inbox(
    *,
    db_path: str | Path = "luna_mlb.sqlite",
    inbox_root: str | Path = "artifacts/inbox/mlb",
    archive_root: str | Path = "artifacts/archive/mlb",
    quarantine_root: str | Path = "artifacts/quarantine/mlb",
    log_file: str | Path = "artifacts/logs/mlb/receiver_runs.jsonl",
    lock_file: str | Path = "artifacts/state/mlb/receiver.lock",
    dry_run: bool = False,
    force_reprocess: bool = False,
) -> dict[str, Any]:
    db_path = Path(db_path)
    paths = ReceiverPaths(
        inbox_root=Path(inbox_root),
        archive_root=Path(archive_root),
        quarantine_root=Path(quarantine_root),
        log_file=Path(log_file),
        lock_file=Path(lock_file),
    )

    summary: dict[str, Any] = {
        "started_at_utc": _utc_now_iso(),
        "dry_run": dry_run,
        "force_reprocess": force_reprocess,
        "inbox_root": str(paths.inbox_root),
        "archive_root": str(paths.archive_root),
        "quarantine_root": str(paths.quarantine_root),
        "processed": 0,
        "imported": 0,
        "already_imported": 0,
        "failed": 0,
        "results": [],
    }

    with ReceiverLock(paths.lock_file):
        bundle_dirs = _list_bundle_dirs(paths.inbox_root)
        summary["pending_bundles"] = [p.name for p in bundle_dirs]

        for bundle_dir in bundle_dirs:
            result = _process_bundle(
                bundle_dir,
                db_path=db_path,
                paths=paths,
                dry_run=dry_run,
                force_reprocess=force_reprocess,
            )
            summary["results"].append(result)
            summary["processed"] += 1
            status = result.get("status")
            if status == "imported":
                summary["imported"] += 1
            elif status == "already_imported":
                summary["already_imported"] += 1
            elif status == "failed":
                summary["failed"] += 1

            log_event = {
                "timestamp_utc": _utc_now_iso(),
                "bundle_id": result.get("bundle_id"),
                "status": status,
                "inserted_games": result.get("inserted_games", 0),
                "inserted_player_lines": result.get("inserted_player_lines", 0),
                "errors": result.get("errors", []),
            }
            _append_jsonl(paths.log_file, log_event)

    summary["finished_at_utc"] = _utc_now_iso()
    return summary
