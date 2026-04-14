from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    repo_root: Path
    data_raw: Path
    data_stage: Path
    data_wh: Path
    db_path: Path
    artifacts: Path
    bridge_root: Path
    producer_root: Path
    producer_core_root: Path
    producer_presentation_root: Path
    legacy_inbox_root: Path
    legacy_archive_root: Path
    legacy_quarantine_root: Path
    export_repo_root: Path


def discover_paths() -> ProjectPaths:
    repo_root = Path(__file__).resolve().parents[2]
    data_raw = repo_root / "data" / "raw"
    data_stage = repo_root / "data" / "stage"
    data_wh = repo_root / "data" / "warehouse"
    artifacts = repo_root / "artifacts"
    bridge_root = repo_root / "bridge"
    producer_root = bridge_root / "luna_ingestion" / "mlb"
    producer_core_root = producer_root / "core"
    producer_presentation_root = producer_root / "presentation"
    legacy_inbox_root = bridge_root / "luna_inbox" / "mlb"
    legacy_archive_root = bridge_root / "luna_archive" / "mlb"
    legacy_quarantine_root = bridge_root / "luna_quarantine" / "mlb"
    export_repo_root = bridge_root / "luna_export" / "repos"
    for p in (
        data_stage,
        data_wh,
        artifacts,
        data_raw,
        producer_core_root,
        producer_presentation_root,
        legacy_inbox_root,
        legacy_archive_root,
        legacy_quarantine_root,
        export_repo_root,
    ):
        p.mkdir(parents=True, exist_ok=True)
    return ProjectPaths(
        repo_root=repo_root,
        data_raw=data_raw,
        data_stage=data_stage,
        data_wh=data_wh,
        db_path=data_wh / "mlb_core.duckdb",
        artifacts=artifacts,
        bridge_root=bridge_root,
        producer_root=producer_root,
        producer_core_root=producer_core_root,
        producer_presentation_root=producer_presentation_root,
        legacy_inbox_root=legacy_inbox_root,
        legacy_archive_root=legacy_archive_root,
        legacy_quarantine_root=legacy_quarantine_root,
        export_repo_root=export_repo_root,
    )
