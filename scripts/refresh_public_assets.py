#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
from collections import defaultdict
from datetime import datetime
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib.pyplot as plt


def load_bundle(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def derive_standings(bundle: dict) -> list[dict]:
    standings = defaultdict(lambda: {"gp": 0, "w": 0, "l": 0, "rs": 0, "ra": 0})
    for game in bundle.get("games", []):
        home = game["home_team"]
        away = game["away_team"]
        home_runs = int(game["home_runs"])
        away_runs = int(game["away_runs"])

        standings[home]["gp"] += 1
        standings[away]["gp"] += 1
        standings[home]["rs"] += home_runs
        standings[home]["ra"] += away_runs
        standings[away]["rs"] += away_runs
        standings[away]["ra"] += home_runs

        if home_runs > away_runs:
            standings[home]["w"] += 1
            standings[away]["l"] += 1
        else:
            standings[away]["w"] += 1
            standings[home]["l"] += 1

    rows = []
    for team, s in standings.items():
        gp = s["gp"]
        rows.append(
            {
                "Team": team,
                "GP": gp,
                "W": s["w"],
                "L": s["l"],
                "RS": s["rs"],
                "RA": s["ra"],
                "RD": s["rs"] - s["ra"],
                "Win%": f"{(s['w'] / gp) if gp else 0.0:.3f}",
            }
        )

    rows.sort(key=lambda r: (-float(r["Win%"]), -r["RD"], r["Team"]))
    return rows


def derive_player_table(bundle: dict, limit: int) -> list[dict]:
    players = defaultdict(lambda: {"name": "", "team": "", "ab": 0, "h": 0, "hr": 0, "rbi": 0})

    for game in bundle.get("games", []):
        for p in game.get("players", []):
            pid = p["player_id"]
            players[pid]["name"] = p["player_name"]
            players[pid]["team"] = p["team"]
            players[pid]["ab"] += int(p["at_bats"])
            players[pid]["h"] += int(p["hits"])
            players[pid]["hr"] += int(p["home_runs"])
            players[pid]["rbi"] += int(p["rbi"])

    rows = []
    for stats in players.values():
        ab = stats["ab"]
        avg = (stats["h"] / ab) if ab else 0.0
        rows.append(
            {
                "Player": stats["name"],
                "Team": stats["team"],
                "AB": ab,
                "H": stats["h"],
                "HR": stats["hr"],
                "RBI": stats["rbi"],
                "AVG": f"{avg:.3f}",
            }
        )

    rows.sort(key=lambda r: (-float(r["AVG"]), -r["H"], r["Player"]))
    return rows[:limit]


def render_table_image(rows: list[dict], title: str, subtitle: str, output_path: Path) -> None:
    if not rows:
        rows = [{"Info": "No data"}]

    columns = list(rows[0].keys())
    table_data = [[str(r[c]) for c in columns] for r in rows]

    row_count = len(rows)
    fig_height = max(4.2, min(12.0, 1.8 + row_count * 0.42))
    fig, ax = plt.subplots(figsize=(12, fig_height))
    fig.patch.set_facecolor("#0b0d10")
    ax.set_facecolor("#0b0d10")
    ax.axis("off")

    fig.text(0.02, 0.965, title, color="#e6edf3", fontsize=18, fontweight="bold")
    fig.text(0.02, 0.935, subtitle, color="#8b98a5", fontsize=11)

    table = ax.table(
        cellText=table_data,
        colLabels=columns,
        cellLoc="center",
        colLoc="center",
        loc="center",
        bbox=[0.02, 0.02, 0.96, 0.87],
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)

    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_facecolor("#1f2937")
            cell.set_text_props(color="#f8fafc", weight="bold")
            cell.set_edgecolor("#374151")
        else:
            cell.set_facecolor("#111827" if row % 2 == 0 else "#0f172a")
            cell.set_text_props(color="#e5e7eb")
            cell.set_edgecolor("#1f2937")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def render_fun_graph(
    standings_rows: list[dict], player_rows: list[dict], stamp_date: str, output_path: Path
) -> str:
    seed = int(hashlib.sha256(stamp_date.encode("utf-8")).hexdigest()[:8], 16)
    rng = random.Random(seed)
    mode = rng.choice(["team_win_pct", "team_run_diff", "player_avg", "player_hr_rbi"])

    fig, ax = plt.subplots(figsize=(11.5, 5.2))
    fig.patch.set_facecolor("#0b0d10")
    ax.set_facecolor("#0b0d10")
    ax.tick_params(colors="#d1d5db")
    for spine in ax.spines.values():
        spine.set_color("#374151")

    title = "MLB Fun Graph of the Day"
    subtitle = f"Auto-selected daily view for {stamp_date}"

    if mode == "team_win_pct":
        teams = standings_rows[:10]
        labels = [t["Team"] for t in teams]
        vals = [float(t["Win%"]) for t in teams]
        ax.bar(labels, vals, color="#38bdf8")
        ax.set_ylim(0, 1)
        ax.set_ylabel("Win %", color="#e5e7eb")
        subtitle += " • Top teams by win percentage"
    elif mode == "team_run_diff":
        teams = sorted(standings_rows, key=lambda r: -r["RD"])[:10]
        labels = [t["Team"] for t in teams]
        vals = [t["RD"] for t in teams]
        colors = ["#22c55e" if v >= 0 else "#ef4444" for v in vals]
        ax.bar(labels, vals, color=colors)
        ax.axhline(0, color="#9ca3af", linewidth=0.9)
        ax.set_ylabel("Run Differential", color="#e5e7eb")
        subtitle += " • Top run differential snapshot"
    elif mode == "player_avg":
        players = player_rows[:10]
        labels = [p["Player"] for p in players]
        vals = [float(p["AVG"]) for p in players]
        ax.barh(labels, vals, color="#34d399")
        ax.set_xlim(0, max(0.35, max(vals, default=0.0) + 0.05))
        ax.set_xlabel("Batting Average", color="#e5e7eb")
        subtitle += " • Player batting average leaders"
    else:
        players = player_rows[:10]
        labels = [p["Player"] for p in players]
        vals = [int(p["HR"]) + int(p["RBI"]) for p in players]
        ax.barh(labels, vals, color="#f59e0b")
        ax.set_xlabel("HR + RBI", color="#e5e7eb")
        subtitle += " • Player power-production mix"

    ax.set_title(title, color="#f8fafc", fontsize=17, fontweight="bold", pad=14)
    fig.text(0.125, 0.92, subtitle, color="#9ca3af", fontsize=10)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return mode


def resolve_stamp_date(bundle: dict) -> str:
    raw = str(bundle.get("generated_at", "")).strip()
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")

    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        return dt.date().isoformat()
    except ValueError:
        return datetime.now().strftime("%Y-%m-%d")


def write_metadata(
    output_dir: Path,
    bundle_path: Path,
    bundle: dict,
    stamp_date: str,
    standings_rows: list[dict],
    players_rows: list[dict],
    fun_graph_mode: str,
) -> None:
    payload = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "bundle_source": str(bundle_path),
        "bundle_id": bundle.get("bundle_id"),
        "bundle_generated_at": bundle.get("generated_at"),
        "stamp_date": stamp_date,
        "games": len(bundle.get("games", [])),
        "standings_rows": len(standings_rows),
        "players_rows": len(players_rows),
        "files": {
            "standings_latest": "standings_latest.png",
            "players_latest": "player_stats_latest.png",
            "fun_graph_latest": "fun_graph_latest.png",
            "standings_dated": f"standings_{stamp_date}.png",
            "players_dated": f"player_stats_{stamp_date}.png",
            "fun_graph_dated": f"fun_graph_{stamp_date}.png",
        },
        "fun_graph_mode": fun_graph_mode,
    }
    (output_dir / "latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate landing-page-ready standings/player visuals "
            "from a Luna-exported bundle."
        )
    )
    parser.add_argument("--bundle", required=True, help="Path to Luna-exported JSON bundle")
    parser.add_argument(
        "--output-dir",
        default="docs/proof",
        help="Output directory for generated visuals and latest metadata",
    )
    parser.add_argument(
        "--player-limit",
        type=int,
        default=15,
        help="Max player rows in player stats window",
    )
    args = parser.parse_args()

    bundle_path = Path(args.bundle)
    output_dir = Path(args.output_dir)
    bundle = load_bundle(bundle_path)
    stamp_date = resolve_stamp_date(bundle)

    standings_rows = derive_standings(bundle)
    player_rows = derive_player_table(bundle, args.player_limit)

    subtitle = (
        f"Bundle {bundle.get('bundle_id', 'unknown')} | "
        f"Generated {bundle.get('generated_at', 'unknown')} | "
        f"Rows: standings={len(standings_rows)} players={len(player_rows)}"
    )

    standings_latest = output_dir / "standings_latest.png"
    players_latest = output_dir / "player_stats_latest.png"
    standings_dated = output_dir / f"standings_{stamp_date}.png"
    players_dated = output_dir / f"player_stats_{stamp_date}.png"
    fun_graph_latest = output_dir / "fun_graph_latest.png"
    fun_graph_dated = output_dir / f"fun_graph_{stamp_date}.png"

    render_table_image(standings_rows, "MLB Standings Window", subtitle, standings_latest)
    render_table_image(player_rows, "MLB Player Stats Window", subtitle, players_latest)
    fun_graph_mode = render_fun_graph(standings_rows, player_rows, stamp_date, fun_graph_latest)

    standings_dated.write_bytes(standings_latest.read_bytes())
    players_dated.write_bytes(players_latest.read_bytes())
    fun_graph_dated.write_bytes(fun_graph_latest.read_bytes())

    write_metadata(
        output_dir,
        bundle_path,
        bundle,
        stamp_date,
        standings_rows,
        player_rows,
        fun_graph_mode,
    )

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "standings_latest": str(standings_latest),
                "players_latest": str(players_latest),
                "fun_graph_latest": str(fun_graph_latest),
                "standings_dated": str(standings_dated),
                "players_dated": str(players_dated),
                "fun_graph_dated": str(fun_graph_dated),
                "latest_json": str(output_dir / "latest.json"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
