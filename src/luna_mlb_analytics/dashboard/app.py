from __future__ import annotations

import pandas as pd
import streamlit as st

from luna_mlb_analytics.storage.db import connect

DIVISION_BY_TEAM = {
    "NYY": "AL East",
    "BAL": "AL East",
    "BOS": "AL East",
    "TB": "AL East",
    "TOR": "AL East",
    "CLE": "AL Central",
    "DET": "AL Central",
    "KC": "AL Central",
    "CWS": "AL Central",
    "MIN": "AL Central",
    "HOU": "AL West",
    "LAA": "AL West",
    "ATH": "AL West",
    "SEA": "AL West",
    "TEX": "AL West",
    "ATL": "NL East",
    "MIA": "NL East",
    "NYM": "NL East",
    "PHI": "NL East",
    "WSH": "NL East",
    "CHC": "NL Central",
    "CIN": "NL Central",
    "MIL": "NL Central",
    "PIT": "NL Central",
    "STL": "NL Central",
    "AZ": "NL West",
    "COL": "NL West",
    "LAD": "NL West",
    "SD": "NL West",
    "SF": "NL West",
}

DIVISION_ORDER = [
    "AL East",
    "AL Central",
    "AL West",
    "NL East",
    "NL Central",
    "NL West",
]

FAVORITE_TEAM = "STL"
TEAM_NAMES = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "CWS": "Chicago White Sox",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "ATH": "Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}


def _load_games(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            game_id,
            game_date,
            away_team,
            away_runs,
            home_team,
            home_runs,
            source_bundle_id,
            COALESCE(
                (
                    SELECT SUM(gp.hits)
                    FROM game_players gp
                    WHERE gp.game_id = g.game_id AND gp.team = g.away_team
                ),
                0
            ) AS away_hits,
            COALESCE(
                (
                    SELECT SUM(gp.hits)
                    FROM game_players gp
                    WHERE gp.game_id = g.game_id AND gp.team = g.home_team
                ),
                0
            ) AS home_hits
        FROM games
        AS g
        ORDER BY game_date DESC, game_id DESC
        """,
        conn,
    )


def _load_standings(conn) -> pd.DataFrame:
    standings = pd.read_sql_query(
        """
        SELECT
            team,
            games_played,
            wins,
            losses,
            runs_scored,
            runs_allowed,
            run_diff,
            win_pct
        FROM team_stats
        ORDER BY win_pct DESC, run_diff DESC, team
        """,
        conn,
    )
    if standings.empty:
        return standings

    standings["division"] = standings["team"].map(DIVISION_BY_TEAM).fillna("Unknown")
    standings["_favorite"] = standings["team"].apply(lambda t: 0 if t == FAVORITE_TEAM else 1)
    standings = standings.sort_values(
        by=["_favorite", "division", "win_pct", "run_diff", "team"],
        ascending=[True, True, False, False, True],
    ).drop(columns=["_favorite"])
    return standings


def _load_players(conn) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            player_id,
            player_name,
            team,
            at_bats,
            hits,
            home_runs,
            rbi,
            batting_avg
        FROM player_stats
        ORDER BY batting_avg DESC, hits DESC, player_name
        """,
        conn,
    )


def _team_name(team_code: str) -> str:
    return TEAM_NAMES.get(team_code, team_code)


def _score_cards(games: pd.DataFrame) -> None:
    if games.empty:
        st.info("No games available yet.")
        return

    cards = games.head(8)
    cols = st.columns(2)
    for i, (_, row) in enumerate(cards.iterrows()):
        winner = (
            row["home_team"]
            if int(row["home_runs"]) >= int(row["away_runs"])
            else row["away_team"]
        )
        is_cardinals_game = row["home_team"] == FAVORITE_TEAM or row["away_team"] == FAVORITE_TEAM
        with cols[i % 2]:
            with st.container(border=True):
                if is_cardinals_game:
                    st.markdown("**Cardinals Game**")

                st.markdown(
                    f"**{_team_name(row['away_team'])} ({row['away_team']}) @ "
                    f"{_team_name(row['home_team'])} ({row['home_team']})**"
                )
                r_h_e = pd.DataFrame(
                    [
                        {
                            "Team": row["away_team"],
                            "R": int(row["away_runs"]),
                            "H": int(row["away_hits"]),
                            "E": "N/A",
                        },
                        {
                            "Team": row["home_team"],
                            "R": int(row["home_runs"]),
                            "H": int(row["home_hits"]),
                            "E": "N/A",
                        },
                    ]
                )
                st.table(r_h_e)
                st.caption(f"Winner: {winner}")
                st.caption(f"{row['game_date']} | game_id={row['game_id']}")
                if st.button("Open box score", key=f"open_box_{row['game_id']}"):
                    st.session_state["selected_game_id"] = str(row["game_id"])


def _standings_view(standings: pd.DataFrame) -> None:
    for division in DIVISION_ORDER:
        block = standings[standings["division"] == division].copy()
        if block.empty:
            continue
        st.markdown(f"### {division}")
        block.insert(0, "rank", range(1, len(block) + 1))
        st.dataframe(
            block[
                [
                    "rank",
                    "team",
                    "wins",
                    "losses",
                    "win_pct",
                    "runs_scored",
                    "runs_allowed",
                    "run_diff",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )


def _box_score_details(conn, games: pd.DataFrame) -> None:
    if games.empty:
        st.info("No games available for box scores.")
        return

    if "selected_game_id" not in st.session_state:
        st.session_state["selected_game_id"] = str(games.iloc[0]["game_id"])

    labels = {}
    for r in games.head(80).itertuples():
        labels[str(r.game_id)] = (
            f"{r.game_date} | {r.away_team} {int(r.away_runs)} - "
            f"{int(r.home_runs)} {r.home_team}"
        )

    selected_game_id = st.session_state["selected_game_id"]
    if selected_game_id not in labels:
        selected_game_id = next(iter(labels))
        st.session_state["selected_game_id"] = selected_game_id

    selected_label = st.selectbox(
        "Open box score details",
        options=list(labels.values()),
        index=list(labels.keys()).index(selected_game_id),
        key="boxscore_game_label",
    )
    game_id = next(k for k, v in labels.items() if v == selected_label)
    st.session_state["selected_game_id"] = game_id

    players = pd.read_sql_query(
        """
        SELECT
            team,
            player_name,
            at_bats,
            hits,
            home_runs,
            rbi
        FROM game_players
        WHERE game_id = ?
        ORDER BY team, player_name
        """,
        conn,
        params=(game_id,),
    )

    if players.empty:
        st.warning("No player lines found for this game.")
        return

    selected_game = games.loc[games["game_id"].astype(str) == str(game_id)].iloc[0]
    st.markdown(
        f"**{_team_name(selected_game['away_team'])} ({selected_game['away_team']}) @ "
        f"{_team_name(selected_game['home_team'])} ({selected_game['home_team']})**"
    )

    st.dataframe(players, use_container_width=True, hide_index=True)


def _stats_view(players: pd.DataFrame) -> None:
    query = st.text_input("Search player", placeholder="Type player name or team")
    filtered = players.copy()
    if query.strip():
        q = query.strip().lower()
        filtered = filtered[
            filtered["player_name"].str.lower().str.contains(q)
            | filtered["team"].str.lower().str.contains(q)
        ]

    st.dataframe(filtered, use_container_width=True, hide_index=True)

    if filtered.empty:
        return

    selected_player = st.selectbox(
        "Player season profile",
        filtered["player_name"].tolist(),
        index=0,
    )
    row = filtered.loc[filtered["player_name"] == selected_player].iloc[0]

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Team", row["team"])
    c2.metric("AB", int(row["at_bats"]))
    c3.metric("H", int(row["hits"]))
    c4.metric("HR", int(row["home_runs"]))
    c5.metric("RBI", int(row["rbi"]))
    c6.metric("AVG", f"{float(row['batting_avg']):.3f}")


def main(db_path: str = "luna_mlb.sqlite") -> None:
    st.set_page_config(page_title="Luna MLB Analytics", layout="wide")
    st.title("Luna MLB Analytics")
    st.caption("Offline-first boxscore analytics dashboard")

    conn = connect(db_path)
    standings = _load_standings(conn)
    players = _load_players(conn)
    games = _load_games(conn)

    if standings.empty:
        st.warning("No derived data found. Run ingest and derivations first.")
        conn.close()
        return

    left, right = st.columns(2)
    left.metric("Teams", int(standings["team"].nunique()))
    right.metric("Players", len(players))

    tab_standings, tab_scores, tab_stats = st.tabs(["Standings", "Scores", "Stats"])

    with tab_standings:
        _standings_view(standings)

    with tab_scores:
        _score_cards(games)
        st.markdown("### Box Score Details")
        _box_score_details(conn, games)

    with tab_stats:
        _stats_view(players)

    conn.close()


if __name__ == "__main__":
    main()
