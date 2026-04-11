from __future__ import annotations

import pandas as pd
import streamlit as st

from luna_mlb_analytics.storage.db import connect


def main(db_path: str = "luna_mlb.sqlite") -> None:
    st.set_page_config(page_title="Luna MLB Analytics", layout="wide")
    st.title("Luna MLB Analytics")
    st.caption("Offline-first boxscore analytics dashboard")

    conn = connect(db_path)
    teams = pd.read_sql_query(
        "SELECT * FROM team_stats ORDER BY win_pct DESC, run_diff DESC, team", conn
    )
    players = pd.read_sql_query(
        "SELECT * FROM player_stats ORDER BY batting_avg DESC, hits DESC, player_name", conn
    )

    if teams.empty:
        st.warning("No derived data found. Run ingest and derivations first.")
        conn.close()
        return

    c1, c2 = st.columns(2)
    c1.metric("Teams", len(teams))
    c2.metric("Players", len(players))

    st.subheader("Team Standings")
    st.dataframe(teams, use_container_width=True)

    st.subheader("Player Snapshot")
    st.dataframe(players, use_container_width=True)

    conn.close()


if __name__ == "__main__":
    main()
