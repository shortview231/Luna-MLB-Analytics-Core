#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "luna_comms" / "config" / "repo_alignment.json"
DEFAULT_INBOX = ROOT / "exports" / "staged" / "luna_comms" / "inbox"


def run_git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(args)} failed in {cwd}")
    return (result.stdout or "").strip()


def load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected object JSON in config: {path}")
    return payload


def iter_inbox_target_refs(inbox_dir: Path) -> list[str]:
    refs: list[str] = []
    if not inbox_dir.exists():
        return refs
    for item in sorted(inbox_dir.glob("*.json")):
        try:
            payload = json.loads(item.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        source = payload.get("source") or {}
        if not isinstance(source, dict):
            continue
        push = source.get("push") or {}
        if not isinstance(push, dict):
            continue
        ref = str(push.get("target_repo_reference") or "").strip()
        if ref:
            refs.append(ref)
    return sorted(set(refs))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Luna_Export root/target repo alignment before commit+push automation."
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--inbox-dir", default=str(DEFAULT_INBOX))
    parser.add_argument("--strict", action="store_true", help="Treat missing local target repos as errors.")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    inbox_dir = Path(args.inbox_dir).expanduser()
    cfg = load_json(config_path)

    expected_root = str(cfg.get("root_repo_origin") or "").strip()
    target_map = cfg.get("target_repo_origins") or {}
    if not isinstance(target_map, dict):
        raise SystemExit("config.target_repo_origins must be an object")

    errors: list[str] = []
    warnings: list[str] = []
    checked_targets: list[dict] = []

    try:
        root_origin = run_git(ROOT, "remote", "get-url", "origin")
    except RuntimeError as exc:
        errors.append(str(exc))
        root_origin = ""

    if expected_root and root_origin and root_origin != expected_root:
        errors.append(f"root origin mismatch: expected '{expected_root}', got '{root_origin}'")

    refs_from_inbox = iter_inbox_target_refs(inbox_dir)
    unknown_refs = [ref for ref in refs_from_inbox if ref not in target_map]
    for ref in unknown_refs:
        warnings.append(f"inbox target_repo_reference not in config: {ref}")

    refs_to_check = sorted(set([*target_map.keys(), *refs_from_inbox]))
    for ref in refs_to_check:
        repo_path = (ROOT / ref).resolve()
        expected_origin = str(target_map.get(ref) or "").strip()
        if not repo_path.exists():
            msg = f"missing local target repo directory: {ref}"
            if args.strict:
                errors.append(msg)
            else:
                warnings.append(msg)
            checked_targets.append({"ref": ref, "exists": False, "expected_origin": expected_origin})
            continue
        if not (repo_path / ".git").exists():
            errors.append(f"target path is not a git repo: {ref}")
            checked_targets.append({"ref": ref, "exists": True, "is_git_repo": False, "expected_origin": expected_origin})
            continue
        try:
            actual_origin = run_git(repo_path, "remote", "get-url", "origin")
        except RuntimeError as exc:
            errors.append(str(exc))
            checked_targets.append({"ref": ref, "exists": True, "is_git_repo": True, "expected_origin": expected_origin})
            continue
        if expected_origin and actual_origin != expected_origin:
            errors.append(f"target origin mismatch for {ref}: expected '{expected_origin}', got '{actual_origin}'")
        checked_targets.append(
            {
                "ref": ref,
                "exists": True,
                "is_git_repo": True,
                "expected_origin": expected_origin,
                "actual_origin": actual_origin,
            }
        )

    status = "ok" if not errors else "error"
    payload = {
        "status": status,
        "root_repo": str(ROOT),
        "expected_root_origin": expected_root,
        "actual_root_origin": root_origin,
        "checked_targets": checked_targets,
        "inbox_target_repo_references": refs_from_inbox,
        "warnings": warnings,
        "errors": errors,
    }

    if args.as_json:
        print(json.dumps(payload))
    else:
        print(f"status={status}")
        print(f"root: {ROOT}")
        print(f"root origin: {root_origin}")
        if warnings:
            print("warnings:")
            for row in warnings:
                print(f"- {row}")
        if errors:
            print("errors:")
            for row in errors:
                print(f"- {row}")
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
