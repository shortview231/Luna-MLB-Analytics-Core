#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
PENDING_RECORDS = ROOT / "metadata" / "records" / "pending"
APPROVED_RECORDS = ROOT / "metadata" / "records" / "approved"


def load_pending_records() -> list[tuple[Path, dict]]:
    records: list[tuple[Path, dict]] = []
    for path in sorted(PENDING_RECORDS.glob("*.json")):
        with path.open("r", encoding="utf-8") as handle:
            records.append((path, json.load(handle)))
    return records


def find_record(selector: str) -> tuple[Path, dict]:
    matches: list[tuple[Path, dict]] = []
    for record_path, record in load_pending_records():
        if record.get("id") == selector:
            matches.append((record_path, record))
            continue
        for related_file in record.get("related_files", []):
            if related_file == selector:
                matches.append((record_path, record))
                break

    if not matches:
        raise SystemExit(f"No pending metadata record matches: {selector}")
    if len(matches) > 1:
        raise SystemExit(f"Multiple pending metadata records match: {selector}")
    return matches[0]


def ensure_pending_export(path_text: str) -> Path:
    artifact_path = (ROOT / path_text).resolve()
    try:
        artifact_path.relative_to(ROOT.resolve())
    except ValueError as exc:
        raise SystemExit("Artifact path must stay inside the repository.") from exc
    if not artifact_path.exists():
        raise SystemExit(f"Artifact file not found: {path_text}")
    if "/exports/pending/" not in artifact_path.as_posix():
        raise SystemExit("Artifact path must be inside exports/pending/.")
    return artifact_path


def move_artifact(artifact_relpath: str) -> str:
    artifact_path = ensure_pending_export(artifact_relpath)
    approved_relpath = artifact_relpath.replace("exports/pending/", "exports/approved/", 1)
    approved_path = ROOT / approved_relpath
    approved_path.parent.mkdir(parents=True, exist_ok=True)
    if approved_path.exists():
        raise SystemExit(f"Approved artifact already exists: {approved_relpath}")
    shutil.move(str(artifact_path), str(approved_path))
    return approved_relpath


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python3 scripts/approve_artifact.py <artifact-id-or-path>")
        return 1

    selector = sys.argv[1]
    record_path, record = find_record(selector)

    if record.get("status") != "pending":
        raise SystemExit(f"Record is not pending: {record.get('id')}")
    if record.get("approved") is True:
        raise SystemExit(f"Record is already approved: {record.get('id')}")

    related_files = record.get("related_files", [])
    if not related_files:
        raise SystemExit(f"Record has no related files: {record.get('id')}")

    updated_related_files = [move_artifact(path) for path in related_files]

    screenshot_files = record.get("screenshot_files", [])
    updated_screenshot_files = []
    for path in screenshot_files:
        if path in related_files:
            updated_screenshot_files.append(
                path.replace("exports/pending/", "exports/approved/", 1)
            )
        else:
            updated_screenshot_files.append(path)

    record["status"] = "approved"
    record["approved"] = True
    record["related_files"] = updated_related_files
    record["screenshot_files"] = updated_screenshot_files

    approved_record_path = APPROVED_RECORDS / record_path.name
    if approved_record_path.exists():
        raise SystemExit(f"Approved metadata record already exists: {approved_record_path.name}")

    APPROVED_RECORDS.mkdir(parents=True, exist_ok=True)
    with approved_record_path.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, indent=2)
        handle.write("\n")

    record_path.unlink()
    print(f"Approved {record['id']}")
    print(f"Artifact moved to {updated_related_files[0]}")
    print(f"Metadata moved to {approved_record_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
