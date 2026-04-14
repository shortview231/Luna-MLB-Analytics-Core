#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import time

from mlb_core.aggregate import rebuild_aggregates
from mlb_core.config import discover_paths
from mlb_core.db import connect
from import_bundle import import_bundle
from export_outputs import main as export_main


def _manifest(path: Path) -> dict:
    return json.loads((path / "manifest.json").read_text(encoding="utf-8"))


def _run_export_for_season(season: int) -> None:
    # Reuse export script behavior without shelling out.
    import sys
    old_argv = sys.argv[:]
    try:
        sys.argv = ["export_outputs.py", "--season", str(season)]
        export_main()
    finally:
        sys.argv = old_argv


def _sync_public_export_repo(repo_root: Path, export_repo_path: Path, touched_seasons: set[int]) -> dict:
    export_repo_path.mkdir(parents=True, exist_ok=True)
    mirror_root = export_repo_path / "mlb_analytics_export"
    if mirror_root.exists():
        shutil.rmtree(mirror_root)
    mirror_root.mkdir(parents=True, exist_ok=True)

    include_dirs = ["src", "scripts", "schemas", "docs", "tests", "dashboard"]
    include_files = ["README.md", "pyproject.toml"]
    for name in include_dirs:
        src = repo_root / name
        if src.exists():
            shutil.copytree(src, mirror_root / name)
    for name in include_files:
        src = repo_root / name
        if src.exists():
            shutil.copy2(src, mirror_root / name)

    # Export only generated outputs relevant to touched seasons.
    generated_src = repo_root / "artifacts" / "generated"
    generated_dst = mirror_root / "artifacts" / "generated"
    generated_dst.mkdir(parents=True, exist_ok=True)
    for season in sorted(touched_seasons):
        for filename in [f"standings_{season}.csv", f"player_summary_{season}.csv", f"scoreboard_{season}.json"]:
            src = generated_src / filename
            if src.exists():
                shutil.copy2(src, generated_dst / filename)

    cards_src = repo_root / "artifacts" / "cardinals_calendar"
    cards_dst = mirror_root / "artifacts" / "cardinals_calendar"
    cards_dst.mkdir(parents=True, exist_ok=True)
    for season in sorted(touched_seasons):
        src = cards_src / f"cardinals_schedule_{season}.json"
        if src.exists():
            shutil.copy2(src, cards_dst / src.name)

    return {"export_repo_path": str(export_repo_path), "mirror_path": str(mirror_root)}


def _list_bundle_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted([p for p in root.iterdir() if p.is_dir() and (p / "manifest.json").exists()])


def _is_read_only_producer_bundle(bundle: Path, producer_root: Path) -> bool:
    try:
        bundle.resolve().relative_to(producer_root.resolve())
        return True
    except Exception:
        return False


def main() -> None:
    ap = argparse.ArgumentParser(description="Offline daily ingestion runner (no internet in Luna)")
    ap.add_argument(
        "--core-root",
        default="bridge/luna_ingestion/mlb/core",
        help="Read-only producer core staging root containing bundle subfolders",
    )
    ap.add_argument("--legacy-inbox", default="bridge/luna_inbox/mlb", help="Legacy inbox root for mutable local bundle flow")
    ap.add_argument("--archive", default="bridge/luna_archive/mlb", help="Archive root for processed legacy bundles")
    ap.add_argument("--quarantine", default="bridge/luna_quarantine/mlb", help="Quarantine root for failed legacy bundles")
    ap.add_argument(
        "--export-repo-path",
        default="bridge/luna_export/repos",
        help="Path where a public-safe export snapshot should be mirrored",
    )
    ap.add_argument("--no-export-sync", action="store_true", help="Disable export repo mirror step")
    ap.add_argument("--allow-reimport", action="store_true")
    ap.add_argument("--db-lock-retries", type=int, default=12, help="Number of retries when DB lock is held")
    ap.add_argument("--db-lock-retry-seconds", type=int, default=5, help="Seconds between DB lock retries")
    args = ap.parse_args()

    paths = discover_paths()
    repo_root = paths.repo_root
    core_root = (repo_root / args.core_root).resolve()
    legacy_inbox = (repo_root / args.legacy_inbox).resolve()
    archive = (repo_root / args.archive).resolve()
    quarantine = (repo_root / args.quarantine).resolve()
    archive.mkdir(parents=True, exist_ok=True)
    quarantine.mkdir(parents=True, exist_ok=True)
    core_root.mkdir(parents=True, exist_ok=True)
    legacy_inbox.mkdir(parents=True, exist_ok=True)

    bundle_dirs = _list_bundle_dirs(core_root) + _list_bundle_dirs(legacy_inbox)
    if not bundle_dirs:
        print({"status": "ok", "processed": 0, "message": "No bundles in inbox"})
        return

    touched_seasons: set[int] = set()
    processed = 0
    failed = 0

    for bundle in bundle_dirs:
        try:
            result = None
            last_err = None
            for _ in range(max(args.db_lock_retries, 0) + 1):
                try:
                    result = import_bundle(bundle, allow_reimport=args.allow_reimport)
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    msg = str(exc).lower()
                    if "could not set lock on file" not in msg:
                        raise
                    time.sleep(max(args.db_lock_retry_seconds, 1))
            if result is None and last_err is not None:
                raise RuntimeError(
                    f"Bundle import failed due to persistent DB lock after retries: {last_err}"
                )
            if result.get("status") == "skipped":
                continue
            manifest = _manifest(bundle)
            season = int(manifest.get("season"))
            touched_seasons.add(season)
            if not _is_read_only_producer_bundle(bundle, core_root):
                target = archive / bundle.name
                if target.exists():
                    target = archive / f"{bundle.name}__dup"
                bundle.rename(target)
            processed += 1
        except Exception as exc:
            failed += 1
            try:
                if not _is_read_only_producer_bundle(bundle, core_root):
                    target = quarantine / bundle.name
                    if target.exists():
                        target = quarantine / f"{bundle.name}__dup"
                    bundle.rename(target)
            except Exception:
                pass
            print({"bundle": bundle.name, "status": "error", "error": str(exc)})

    if not touched_seasons:
        print(
            {
                "status": "ok",
                "processed": processed,
                "failed": failed,
                "touched_seasons": [],
                "export_sync": None,
            }
        )
        return

    con = None
    last_err = None
    for _ in range(max(args.db_lock_retries, 0) + 1):
        try:
            con = connect(paths.db_path)
            last_err = None
            break
        except Exception as exc:
            last_err = exc
            msg = str(exc).lower()
            if "could not set lock on file" not in msg:
                raise
            time.sleep(max(args.db_lock_retry_seconds, 1))
    if con is None:
        raise RuntimeError(
            f"DB remained locked after retries ({args.db_lock_retries} attempts): {last_err}"
        )
    for season in sorted(touched_seasons):
        agg = rebuild_aggregates(con, season)
        print({"season": season, "aggregate": agg})
        _run_export_for_season(season)
    con.close()

    export_summary = None
    if touched_seasons and not args.no_export_sync:
        export_target = (repo_root / args.export_repo_path).resolve()
        export_summary = _sync_public_export_repo(repo_root, export_target, touched_seasons)

    print(
        {
            "status": "ok",
            "processed": processed,
            "failed": failed,
            "touched_seasons": sorted(touched_seasons),
            "export_sync": export_summary,
        }
    )


if __name__ == "__main__":
    main()
