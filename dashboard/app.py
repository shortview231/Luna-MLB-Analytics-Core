from __future__ import annotations

from pathlib import Path
import duckdb
import streamlit as st


st.set_page_config(page_title="Luna MLB Dashboard", layout="wide")

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = REPO_ROOT / "data" / "warehouse" / "mlb_core.duckdb"
FAVORITE_TEAM_ID = 138
FAVORITE_TEAM_NAME = "St. Louis Cardinals"

DIVISION_MAP = {
    111: ("AL", "AL East"),   # Red Sox
    147: ("AL", "AL East"),   # Yankees
    139: ("AL", "AL East"),   # Rays
    110: ("AL", "AL East"),   # Orioles
    141: ("AL", "AL East"),   # Blue Jays
    114: ("AL", "AL Central"),  # Guardians
    116: ("AL", "AL Central"),  # Tigers
    118: ("AL", "AL Central"),  # Royals
    145: ("AL", "AL Central"),  # White Sox
    142: ("AL", "AL Central"),  # Twins
    117: ("AL", "AL West"),   # Astros
    108: ("AL", "AL West"),   # Angels
    133: ("AL", "AL West"),   # Athletics
    136: ("AL", "AL West"),   # Mariners
    140: ("AL", "AL West"),   # Rangers
    144: ("NL", "NL East"),   # Braves
    146: ("NL", "NL East"),   # Marlins
    121: ("NL", "NL East"),   # Mets
    143: ("NL", "NL East"),   # Phillies
    120: ("NL", "NL East"),   # Nationals
    112: ("NL", "NL Central"),  # Cubs
    113: ("NL", "NL Central"),  # Reds
    158: ("NL", "NL Central"),  # Brewers
    134: ("NL", "NL Central"),  # Pirates
    138: ("NL", "NL Central"),  # Cardinals
    109: ("NL", "NL West"),   # Diamondbacks
    115: ("NL", "NL West"),   # Rockies
    119: ("NL", "NL West"),   # Dodgers
    135: ("NL", "NL West"),   # Padres
    137: ("NL", "NL West"),   # Giants
}

TEAM_ABBR_BY_ID = {
    111: "BOS",
    147: "NYY",
    139: "TB",
    110: "BAL",
    141: "TOR",
    114: "CLE",
    116: "DET",
    118: "KC",
    145: "CWS",
    142: "MIN",
    117: "HOU",
    108: "LAA",
    133: "ATH",
    136: "SEA",
    140: "TEX",
    144: "ATL",
    146: "MIA",
    121: "NYM",
    143: "PHI",
    120: "WSH",
    112: "CHC",
    113: "CIN",
    158: "MIL",
    134: "PIT",
    138: "STL",
    109: "AZ",
    115: "COL",
    119: "LAD",
    135: "SD",
    137: "SF",
}

BATTING_STATS = ["ops", "hr", "rbi", "h", "ab", "bb", "so", "r", "doubles", "triples", "sb"]
PITCHING_STATS = ["era", "so_pitched", "ip_outs", "er", "h_allowed", "bb_allowed", "hr_allowed"]


def _conn() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return duckdb.connect(str(DB_PATH), read_only=True)


def query_rows(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict]:
    result = con.execute(sql, params or [])
    cols = [d[0] for d in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]


def scalar(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None):
    return con.execute(sql, params or []).fetchone()[0]


def _format_ip(ip_outs: int | None) -> str:
    if ip_outs is None:
        return ""
    whole = int(ip_outs) // 3
    rem = int(ip_outs) % 3
    return f"{whole}.{rem}"


def _fmt3(value) -> str:
    try:
        return f"{float(value):.3f}"
    except Exception:
        return ""


def _fmt2(value) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return ""


def _team_abbr(team_id: int | None, name: str | None) -> str:
    if team_id in TEAM_ABBR_BY_ID:
        return TEAM_ABBR_BY_ID[int(team_id)]
    if not name:
        return ""
    words = [w for w in str(name).split() if w.lower() not in {"the", "of", "and"}]
    if len(words) == 1:
        return words[0][:3].upper()
    return "".join(w[0].upper() for w in words[:3])


def _division_ranked_rows(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        team_id = int(row.get("team_id") or 0)
        league, division = DIVISION_MAP.get(team_id, ("UNK", "Unknown"))
        wins = int(row.get("wins") or 0)
        losses = int(row.get("losses") or 0)
        row = dict(row)
        row["league"] = league
        row["division"] = division
        row["win_pct"] = round((wins / (wins + losses)), 3) if (wins + losses) else 0.0
        grouped.setdefault(division, []).append(row)

    for division, teams in grouped.items():
        teams.sort(
            key=lambda r: (
                -int(r.get("wins") or 0),
                int(r.get("losses") or 0),
                -int(r.get("run_differential") or 0),
                str(r.get("team_name") or ""),
            )
        )
        if teams:
            leader_wins = int(teams[0].get("wins") or 0)
            leader_losses = int(teams[0].get("losses") or 0)
        else:
            leader_wins = 0
            leader_losses = 0
        for idx, team in enumerate(teams, start=1):
            wins = int(team.get("wins") or 0)
            losses = int(team.get("losses") or 0)
            gb = ((leader_wins - wins) + (losses - leader_losses)) / 2.0
            team["rank"] = idx
            team["gb"] = round(gb, 1)
    return grouped


def _fetch_player_season(con: duckdb.DuckDBPyConnection, season: int, player_id: int) -> dict | None:
    rows = query_rows(
        con,
        """
        SELECT
          p.player_id, p.player_name, p.team_id, t.team_name, p.games_played,
          p.ab, p.r, p.h, p.rbi, p.bb, p.so, p.hr, p.doubles, p.triples, p.sb, p.cs, p.hbp, p.sf, p.ops,
          p.ip_outs, p.er, p.h_allowed, p.bb_allowed, p.so_pitched, p.hr_allowed, p.era
        FROM player_season_aggregates p
        LEFT JOIN team_season_aggregates t ON t.season=p.season AND t.team_id=p.team_id
        WHERE p.season=? AND p.player_id=?
        ORDER BY p.team_id
        LIMIT 1
        """,
        [season, player_id],
    )
    return rows[0] if rows else None


def _open_player_modal(con: duckdb.DuckDBPyConnection, season: int, player_id: int) -> None:
    player = _fetch_player_season(con, season, player_id)
    if not player:
        st.warning("Player season record not found.")
        return
    st.markdown(
        f"## {player.get('player_name')}  \n"
        f"Team: {player.get('team_name') or player.get('team_id')} | Season: {season}"
    )
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Batting")
        st.dataframe([{
            "G": player.get("games_played"),
            "AB": player.get("ab"),
            "R": player.get("r"),
            "H": player.get("h"),
            "RBI": player.get("rbi"),
            "BB": player.get("bb"),
            "SO": player.get("so"),
            "HR": player.get("hr"),
            "2B": player.get("doubles"),
            "3B": player.get("triples"),
            "SB": player.get("sb"),
            "CS": player.get("cs"),
            "OPS": _fmt3(player.get("ops")),
        }], hide_index=True, use_container_width=True)
    with c2:
        st.caption("Pitching")
        st.dataframe([{
            "IP": _format_ip(player.get("ip_outs")),
            "SO": player.get("so_pitched"),
            "ER": player.get("er"),
            "H": player.get("h_allowed"),
            "BB": player.get("bb_allowed"),
            "HR": player.get("hr_allowed"),
            "ERA": _fmt2(player.get("era")),
        }], hide_index=True, use_container_width=True)


def _render_game_boxscore(con: duckdb.DuckDBPyConnection, season: int, game_pk: int) -> None:
    game = query_rows(
        con,
        """
        SELECT game_pk, game_date, away_team_name, away_score, home_team_name, home_score, status
        FROM games
        WHERE game_pk=?
        LIMIT 1
        """,
        [game_pk],
    )
    if not game:
        st.warning("Selected game not found.")
        return
    g = game[0]
    st.markdown(
        f"**{g.get('away_team_name')} {g.get('away_score')} @ {g.get('home_team_name')} {g.get('home_score')}**  \n"
        f"{g.get('game_date')} | {g.get('status')} | gamePk {g.get('game_pk')}"
    )
    teams = query_rows(
        con,
        """
        SELECT team_id, team_name, is_home, runs_scored, runs_allowed, hits, errors, left_on_base, team_ops_game, team_era_game
        FROM team_game_results
        WHERE game_pk=?
        ORDER BY is_home ASC, team_name ASC
        """,
        [game_pk],
    )
    st.dataframe(
        [
            {
                "side": "HOME" if r.get("is_home") else "AWAY",
                "team": r.get("team_name"),
                "R": r.get("runs_scored"),
                "RA": r.get("runs_allowed"),
                "H": r.get("hits"),
                "E": r.get("errors"),
                "LOB": r.get("left_on_base"),
                "OPS(G)": _fmt3(r.get("team_ops_game")),
                "ERA(G)": _fmt2(r.get("team_era_game")),
            }
            for r in teams
        ],
        hide_index=True,
        use_container_width=True,
    )
    away_team = next((t for t in teams if not t.get("is_home")), None)
    home_team = next((t for t in teams if t.get("is_home")), None)
    for team in [away_team, home_team]:
        if not team:
            continue
        team_id = int(team["team_id"])
        st.markdown(f"### {'Away' if not team.get('is_home') else 'Home'}: {team.get('team_name')}")
        b_rows = query_rows(
            con,
            """
            SELECT player_id, player_name, position, batting_order, ab, r, h, rbi, bb, so, hr, doubles, triples, sb, cs, hbp, sf, obp_game, slg_game, ops_game
            FROM player_game_batting
            WHERE game_pk=? AND team_id=?
            ORDER BY batting_order ASC NULLS LAST, player_name ASC
            """,
            [game_pk, team_id],
        )
        p_rows = query_rows(
            con,
            """
            SELECT player_id, player_name, ip_outs, h_allowed, er, bb_allowed, so_pitched, hr_allowed, pitches, strikes, era_game
            FROM player_game_pitching
            WHERE game_pk=? AND team_id=?
            ORDER BY ip_outs DESC, so_pitched DESC, player_name ASC
            """,
            [game_pk, team_id],
        )
        bc, pc = st.columns(2)
        with bc:
            st.caption("Batting Box Score")
            bat_rows = [{
                "player_id": r.get("player_id"),
                "Player": r.get("player_name"),
                "POS": r.get("position") or "",
                "AB": r.get("ab"),
                "R": r.get("r"),
                "H": r.get("h"),
                "RBI": r.get("rbi"),
                "BB": r.get("bb"),
                "SO": r.get("so"),
                "HR": r.get("hr"),
                "2B": r.get("doubles"),
                "3B": r.get("triples"),
                "SB": r.get("sb"),
                "CS": r.get("cs"),
                "OBP": _fmt3(r.get("obp_game")),
                "SLG": _fmt3(r.get("slg_game")),
                "OPS": _fmt3(r.get("ops_game")),
            } for r in b_rows]
            ev = st.dataframe(
                [{k: v for k, v in row.items() if k != "player_id"} for row in bat_rows],
                hide_index=True, use_container_width=True,
                on_select="rerun", selection_mode="single-row",
                key=f"box_bat_{game_pk}_{team_id}",
            )
            sel = getattr(getattr(ev, "selection", None), "rows", [])
            if sel:
                st.session_state["selected_player_id"] = int(bat_rows[int(sel[0])].get("player_id") or 0)
        with pc:
            st.caption("Pitching Box Score")
            pit_rows = [{
                "player_id": r.get("player_id"),
                "Pitcher": r.get("player_name"),
                "IP": _format_ip(r.get("ip_outs")),
                "H": r.get("h_allowed"),
                "ER": r.get("er"),
                "BB": r.get("bb_allowed"),
                "SO": r.get("so_pitched"),
                "HR": r.get("hr_allowed"),
                "P-S": f"{r.get('pitches')}-{r.get('strikes')}",
                "ERA": _fmt2(r.get("era_game")),
            } for r in p_rows]
            ev = st.dataframe(
                [{k: v for k, v in row.items() if k != "player_id"} for row in pit_rows],
                hide_index=True, use_container_width=True,
                on_select="rerun", selection_mode="single-row",
                key=f"box_pit_{game_pk}_{team_id}",
            )
            sel = getattr(getattr(ev, "selection", None), "rows", [])
            if sel:
                st.session_state["selected_player_id"] = int(pit_rows[int(sel[0])].get("player_id") or 0)


def _render_score_card(card: dict, key: str) -> bool:
    away = card.get("away_team_name")
    home = card.get("home_team_name")
    a_r = card.get("away_score")
    h_r = card.get("home_score")
    a_h = card.get("away_hits")
    h_h = card.get("home_hits")
    a_e = card.get("away_errors")
    h_e = card.get("home_errors")
    status = card.get("status")
    game_pk = card.get("game_pk")

    st.markdown(
        f"""
        <div style="border:2px solid #4a5568;border-radius:12px;padding:14px;margin-bottom:8px;background:#0b1220;">
          <div style="font-size:0.9em;opacity:0.9;margin-bottom:8px;">{card.get("game_date")} | {status} | gamePk {game_pk}</div>
          <table style="width:100%;border-collapse:collapse;">
            <thead>
              <tr>
                <th style="text-align:left;padding:4px;">Team</th>
                <th style="text-align:center;padding:4px;">R</th>
                <th style="text-align:center;padding:4px;">H</th>
                <th style="text-align:center;padding:4px;">E</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:4px;"><strong>{away}</strong></td>
                <td style="text-align:center;padding:4px;">{a_r}</td>
                <td style="text-align:center;padding:4px;">{a_h}</td>
                <td style="text-align:center;padding:4px;">{a_e}</td>
              </tr>
              <tr>
                <td style="padding:4px;"><strong>{home}</strong></td>
                <td style="text-align:center;padding:4px;">{h_r}</td>
                <td style="text-align:center;padding:4px;">{h_h}</td>
                <td style="text-align:center;padding:4px;">{h_e}</td>
              </tr>
            </tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )
    return st.button("Open Box Score", key=key, use_container_width=True)


def _apply_accessibility_theme(high_contrast: bool, large_text: bool) -> None:
    base_font = "20px" if large_text else "16px"
    metric_font = "34px" if large_text else "26px"
    if high_contrast:
        st.markdown(
            f"""
            <style>
            html, body, [data-testid="stAppViewContainer"] {{
              background: #000000 !important;
              color: #FFFFFF !important;
            }}
            [data-testid="stSidebar"] {{
              background: #111111 !important;
              color: #FFFFFF !important;
            }}
            .stMarkdown, .stText, .stDataFrame, .stTable, label, p, span, div {{
              color: #FFFFFF !important;
              font-size: {base_font} !important;
            }}
            [data-testid="stMetricValue"] {{
              color: #FFFFFF !important;
              font-size: {metric_font} !important;
            }}
            button, [role="button"] {{
              border: 2px solid #FFFFFF !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
    elif large_text:
        st.markdown(
            f"""
            <style>
            .stMarkdown, .stText, .stDataFrame, .stTable, label, p, span, div {{
              font-size: {base_font} !important;
            }}
            [data-testid="stMetricValue"] {{
              font-size: {metric_font} !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )


def main() -> None:
    st.title("Luna MLB Analytics Dashboard")
    st.caption("Boxscore-first season analytics from project DuckDB (regular season only)")

    if not DB_PATH.exists():
        st.error(f"Missing DB file: {DB_PATH}")
        st.info("Run ingestion first: `python scripts/pull_boxscores.py --season 2026 --start 2026-03-01 --end 2026-04-10`")
        return

    con = _conn()

    seasons = [r["season"] for r in query_rows(con, "SELECT DISTINCT season FROM games ORDER BY season DESC")]
    if not seasons:
        st.warning("No seasons found in database.")
        return

    st.sidebar.subheader("Accessibility")
    high_contrast = st.sidebar.toggle("High contrast mode", value=True)
    large_text = st.sidebar.toggle("Large text mode", value=True)
    _apply_accessibility_theme(high_contrast, large_text)

    season = st.sidebar.selectbox("Season", seasons, index=0)

    games = scalar(con, "SELECT COUNT(*) FROM games WHERE season=?", [season])
    finals = scalar(con, "SELECT COUNT(*) FROM games WHERE season=? AND lower(status)='final'", [season])
    teams = scalar(con, "SELECT COUNT(*) FROM team_season_aggregates WHERE season=?", [season])
    players = scalar(con, "SELECT COUNT(*) FROM player_season_aggregates WHERE season=?", [season])
    last_ingested = scalar(con, "SELECT MAX(last_ingested_at) FROM games WHERE season=?", [season])

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Games", games)
    c2.metric("Final Games", finals)
    c3.metric("Teams", teams)
    c4.metric("Players", players)
    st.caption(f"As of: {last_ingested or 'N/A'}")
    health = query_rows(
        con,
        """
        SELECT game_date, COUNT(*) AS games, SUM(CASE WHEN lower(status)='final' THEN 1 ELSE 0 END) AS finals
        FROM games
        WHERE season=?
        GROUP BY game_date
        ORDER BY game_date DESC
        LIMIT 15
        """,
        [season],
    )
    if health:
        missing_finals = sum(1 for r in health if int(r.get("games") or 0) != int(r.get("finals") or 0))
        if missing_finals:
            st.warning(f"Data health: last 15 dates include {missing_finals} date(s) with non-final games.")
        else:
            st.success("Data health: all games in the latest 15 dates are final.")
        with st.expander("Daily ingest coverage (latest 15 dates)"):
            st.dataframe(health, hide_index=True, use_container_width=True)

    tab_standings, tab_scores, tab_stats = st.tabs(["Standings", "Scores", "Stats"])

    with tab_standings:
        st.subheader("Division Standings")
        standings_rows = query_rows(
            con,
            """
            SELECT team_id, team_name, wins, losses, runs_scored, runs_allowed, run_differential, team_ops, team_era
            FROM team_season_aggregates
            WHERE season=?
            """,
            [season],
        )
        by_division = _division_ranked_rows(standings_rows)
        for division in ["AL East", "AL Central", "AL West", "NL East", "NL Central", "NL West"]:
            rows = by_division.get(division, [])
            if not rows:
                continue
            st.markdown(f"#### {division}")
            st.dataframe(
                [
                    {
                        "rank": r["rank"],
                        "abbr": _team_abbr(r.get("team_id"), r["team_name"]),
                        "team": r["team_name"],
                        "W": r["wins"],
                        "L": r["losses"],
                        "PCT": _fmt3(r["win_pct"]),
                        "GB": r["gb"],
                        "RS": r["runs_scored"],
                        "RA": r["runs_allowed"],
                        "DIFF": r["run_differential"],
                        "OPS": _fmt3(r["team_ops"]),
                        "ERA": _fmt2(r["team_era"]),
                    }
                    for r in rows
                ],
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Recent Cardinals Final Box Scores")
        cards = query_rows(
            con,
            """
            SELECT game_pk, game_date, away_team_name, away_score, home_team_name, home_score, status
            FROM games
            WHERE season=? AND lower(status)='final' AND (home_team_id=138 OR away_team_id=138)
            ORDER BY game_date DESC
            LIMIT 20
            """,
            [season],
        )
        st.dataframe(cards, use_container_width=True, hide_index=True)
        if cards:
            game_options = [
                (
                    f"{r.get('game_date')} | {r.get('away_team_name')} {r.get('away_score')} @ "
                    f"{r.get('home_team_name')} {r.get('home_score')} (gamePk {r.get('game_pk')})",
                    int(r.get("game_pk") or 0),
                )
                for r in cards
            ]
            labels = [g[0] for g in game_options]
            selected = st.selectbox("Open box score details", labels, key="boxscore_game")
            selected_pk = dict(game_options).get(selected)
            if selected_pk:
                _render_game_boxscore(con, season, selected_pk)

    with tab_scores:
        st.subheader("League Scores")
        date_rows = query_rows(
            con,
            """
            SELECT DISTINCT game_date
            FROM games
            WHERE season=?
            ORDER BY game_date DESC
            """,
            [season],
        )
        date_options = [str(r["game_date"]) for r in date_rows]
        selected_date = st.selectbox("Game date", date_options, index=0 if date_options else None, key="scores_date")

        scores_rows = query_rows(
            con,
            """
            SELECT
              game_pk, game_date, status, start_time_utc,
              away_team_id, home_team_id,
              away_team_name, away_score,
              home_team_name, home_score,
              COALESCE((SELECT t.hits FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.away_team_id LIMIT 1), 0) AS away_hits,
              COALESCE((SELECT t.errors FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.away_team_id LIMIT 1), 0) AS away_errors,
              COALESCE((SELECT t.hits FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.home_team_id LIMIT 1), 0) AS home_hits,
              COALESCE((SELECT t.errors FROM team_game_results t WHERE t.game_pk=g.game_pk AND t.team_id=g.home_team_id LIMIT 1), 0) AS home_errors
            FROM games
            AS g
            WHERE season=? AND game_date=?
            ORDER BY start_time_utc ASC, game_pk ASC
            """,
            [season, selected_date],
        ) if selected_date else []

        scores_rows.sort(
            key=lambda r: (
                0 if int(r.get("home_team_id") or 0) == FAVORITE_TEAM_ID or int(r.get("away_team_id") or 0) == FAVORITE_TEAM_ID else 1,
                str(r.get("start_time_utc") or ""),
                int(r.get("game_pk") or 0),
            )
        )

        if scores_rows:
            st.info(
                f"Showing every game in the database for {selected_date}. "
                f"{FAVORITE_TEAM_NAME} is pinned first when they play. "
                f"Total games shown: {len(scores_rows)}."
            )
            cols = st.columns(3)
            for i, row in enumerate(scores_rows):
                with cols[i % 3]:
                    opened = _render_score_card(row, key=f"score_card_open_{row.get('game_pk')}_{i}")
                    if opened:
                        st.session_state["selected_score_game_pk"] = int(row.get("game_pk") or 0)
            selected_pk = int(st.session_state.get("selected_score_game_pk") or 0)
            if selected_pk:
                if hasattr(st, "dialog"):
                    @st.dialog("Game Box Score", width="large")
                    def _score_dialog():
                        _render_game_boxscore(con, season, selected_pk)
                    _score_dialog()
                else:
                    st.markdown("### Game Box Score")
                    _render_game_boxscore(con, season, selected_pk)
        else:
            st.info("No games found for selected date/filter.")

    with tab_stats:
        batting_tab, pitching_tab = st.tabs(["Batting", "Pitching"])

        with batting_tab:
            st.subheader("Batting Leaders")
            c1, c2, c3 = st.columns(3)
            stat = c1.selectbox("Primary stat", BATTING_STATS, index=0, key="bat_stat")
            query = c2.text_input("Player search", "", key="bat_search")
            min_ab = c3.number_input("Min AB", min_value=0, value=20, step=1, key="bat_min_ab")

            batting_rows = query_rows(
                con,
                """
                SELECT
                  p.player_id, p.player_name, t.team_name, p.games_played,
                  p.ab, p.r, p.h, p.rbi, p.bb, p.so, p.hr, p.doubles, p.triples, p.sb, p.cs, p.hbp, p.sf, p.ops
                FROM player_season_aggregates p
                LEFT JOIN team_season_aggregates t
                  ON t.season=p.season AND t.team_id=p.team_id
                WHERE p.season=? AND p.ab >= ?
                """,
                [season, int(min_ab)],
            )
            if query.strip():
                q = query.strip().lower()
                batting_rows = [r for r in batting_rows if q in str(r.get("player_name", "")).lower()]
            batting_rows.sort(key=lambda r: float(r.get(stat) or 0), reverse=True)
            top_rows = batting_rows[:100]
            table_rows = []
            for i, r in enumerate(top_rows, start=1):
                ordered = {
                    "rank": i,
                    "player_name": r.get("player_name"),
                    "team_name": r.get("team_name"),
                    stat: r.get(stat),
                }
                for col in ["games_played", "ab", "h", "r", "hr", "rbi", "bb", "so", "doubles", "triples", "sb", "cs", "hbp", "sf", "ops"]:
                    if col != stat:
                        ordered[col] = r.get(col)
                table_rows.append(ordered)
            ev = st.dataframe(
                table_rows,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="batting_table_select",
            )
            sel = getattr(getattr(ev, "selection", None), "rows", [])
            if sel:
                picked = top_rows[int(sel[0])]
                st.session_state["selected_player_id"] = int(picked.get("player_id") or 0)

        with pitching_tab:
            st.subheader("Pitching Leaders")
            c1, c2, c3 = st.columns(3)
            stat = c1.selectbox("Primary stat", PITCHING_STATS, index=0, key="pit_stat")
            query = c2.text_input("Player search", "", key="pit_search")
            min_outs = c3.number_input("Min outs pitched", min_value=0, value=30, step=1, key="pit_min_outs")

            pitching_rows = query_rows(
                con,
                """
                SELECT
                  p.player_id, p.player_name, t.team_name, p.games_played,
                  p.ip_outs, p.er, p.h_allowed, p.bb_allowed, p.so_pitched, p.hr_allowed, p.era
                FROM player_season_aggregates p
                LEFT JOIN team_season_aggregates t
                  ON t.season=p.season AND t.team_id=p.team_id
                WHERE p.season=? AND p.ip_outs >= ?
                """,
                [season, int(min_outs)],
            )
            if query.strip():
                q = query.strip().lower()
                pitching_rows = [r for r in pitching_rows if q in str(r.get("player_name", "")).lower()]
            reverse = stat != "era"
            pitching_rows.sort(key=lambda r: float(r.get(stat) or 0), reverse=reverse)
            top_rows = pitching_rows[:100]
            table_rows = []
            for i, r in enumerate(top_rows, start=1):
                row = dict(r)
                row["ip"] = _format_ip(row.get("ip_outs"))
                ordered = {
                    "rank": i,
                    "player_name": row.get("player_name"),
                    "team_name": row.get("team_name"),
                    stat: row.get(stat),
                }
                for col in ["games_played", "ip", "ip_outs", "era", "so_pitched", "er", "h_allowed", "bb_allowed", "hr_allowed"]:
                    if col != stat:
                        ordered[col] = row.get(col)
                table_rows.append(ordered)
            ev = st.dataframe(
                table_rows,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row",
                key="pitching_table_select",
            )
            sel = getattr(getattr(ev, "selection", None), "rows", [])
            if sel:
                picked = top_rows[int(sel[0])]
                st.session_state["selected_player_id"] = int(picked.get("player_id") or 0)

    selected_player_id = int(st.session_state.get("selected_player_id") or 0)
    if selected_player_id:
        if hasattr(st, "dialog"):
            @st.dialog("Player Season Profile", width="large")
            def _player_dialog():
                _open_player_modal(con, season, selected_player_id)
            _player_dialog()
        else:
            st.markdown("## Player Season Profile")
            _open_player_modal(con, season, selected_player_id)


if __name__ == "__main__":
    main()
