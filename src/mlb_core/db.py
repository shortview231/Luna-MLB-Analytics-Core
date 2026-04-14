from __future__ import annotations

from pathlib import Path
import duckdb


DDL = """
CREATE TABLE IF NOT EXISTS games (
  game_pk BIGINT PRIMARY KEY,
  game_date DATE,
  season INTEGER,
  status TEXT,
  home_team_id INTEGER,
  home_team_name TEXT,
  away_team_id INTEGER,
  away_team_name TEXT,
  home_score INTEGER,
  away_score INTEGER,
  venue_id INTEGER,
  venue_name TEXT,
  start_time_utc TIMESTAMP,
  source_payload_hash TEXT,
  last_ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_game_results (
  game_pk BIGINT,
  team_id INTEGER,
  team_name TEXT,
  opponent_team_id INTEGER,
  is_home BOOLEAN,
  win_flag INTEGER,
  loss_flag INTEGER,
  runs_scored INTEGER,
  runs_allowed INTEGER,
  hits INTEGER,
  errors INTEGER,
  left_on_base INTEGER,
  team_ops_game DOUBLE,
  team_era_game DOUBLE,
  last_ingested_at TIMESTAMP,
  PRIMARY KEY (game_pk, team_id)
);

CREATE TABLE IF NOT EXISTS player_game_batting (
  game_pk BIGINT,
  team_id INTEGER,
  player_id BIGINT,
  player_name TEXT,
  batting_order TEXT,
  position TEXT,
  ab INTEGER,
  r INTEGER,
  h INTEGER,
  rbi INTEGER,
  bb INTEGER,
  so INTEGER,
  hr INTEGER,
  doubles INTEGER,
  triples INTEGER,
  sb INTEGER,
  cs INTEGER,
  hbp INTEGER,
  sf INTEGER,
  obp_game DOUBLE,
  slg_game DOUBLE,
  ops_game DOUBLE,
  last_ingested_at TIMESTAMP,
  PRIMARY KEY (game_pk, team_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_game_pitching (
  game_pk BIGINT,
  team_id INTEGER,
  player_id BIGINT,
  player_name TEXT,
  ip_outs INTEGER,
  h_allowed INTEGER,
  er INTEGER,
  bb_allowed INTEGER,
  so_pitched INTEGER,
  hr_allowed INTEGER,
  pitches INTEGER,
  strikes INTEGER,
  decision TEXT,
  era_game DOUBLE,
  last_ingested_at TIMESTAMP,
  PRIMARY KEY (game_pk, team_id, player_id)
);

CREATE TABLE IF NOT EXISTS team_season_aggregates (
  season INTEGER,
  team_id INTEGER,
  team_name TEXT,
  games_played INTEGER,
  wins INTEGER,
  losses INTEGER,
  runs_scored INTEGER,
  runs_allowed INTEGER,
  run_differential INTEGER,
  team_ops DOUBLE,
  team_era DOUBLE,
  streak TEXT,
  last_10 TEXT,
  next_game BIGINT,
  next_game_time TIMESTAMP,
  as_of_date DATE,
  PRIMARY KEY (season, team_id)
);

CREATE TABLE IF NOT EXISTS player_season_aggregates (
  season INTEGER,
  player_id BIGINT,
  team_id INTEGER,
  player_name TEXT,
  games_played INTEGER,
  ab INTEGER,
  r INTEGER,
  h INTEGER,
  rbi INTEGER,
  bb INTEGER,
  so INTEGER,
  hr INTEGER,
  doubles INTEGER,
  triples INTEGER,
  sb INTEGER,
  cs INTEGER,
  hbp INTEGER,
  sf INTEGER,
  ops DOUBLE,
  ip_outs INTEGER,
  er INTEGER,
  h_allowed INTEGER,
  bb_allowed INTEGER,
  so_pitched INTEGER,
  hr_allowed INTEGER,
  era DOUBLE,
  as_of_date DATE,
  PRIMARY KEY (season, player_id, team_id)
);

CREATE TABLE IF NOT EXISTS processed_bundles (
  bundle_id TEXT PRIMARY KEY,
  imported_at TIMESTAMP,
  source_path TEXT,
  season INTEGER,
  start_date DATE,
  end_date DATE,
  game_count INTEGER,
  boxscore_count INTEGER,
  status TEXT,
  error_text TEXT
);
"""


AGG_TEAM_SQL = """
INSERT OR REPLACE INTO team_season_aggregates
WITH game_rollup AS (
  SELECT
    EXTRACT(YEAR FROM g.game_date)::INTEGER AS season,
    t.team_id,
    MAX(t.team_name) AS team_name,
    COUNT(*) AS games_played,
    SUM(t.win_flag) AS wins,
    SUM(t.loss_flag) AS losses,
    SUM(t.runs_scored) AS runs_scored,
    SUM(t.runs_allowed) AS runs_allowed,
    SUM(t.runs_scored - t.runs_allowed) AS run_differential
  FROM team_game_results t
  JOIN games g USING(game_pk)
  WHERE g.season = ? AND lower(g.status)='final'
  GROUP BY EXTRACT(YEAR FROM g.game_date), t.team_id
),
bat_rollup AS (
  SELECT
    EXTRACT(YEAR FROM g.game_date)::INTEGER AS season,
    pb.team_id,
    SUM(pb.ab) AS ab,
    SUM(pb.h) AS h,
    SUM(pb.bb) AS bb,
    SUM(pb.hbp) AS hbp,
    SUM(pb.sf) AS sf,
    SUM(pb.doubles) AS doubles,
    SUM(pb.triples) AS triples,
    SUM(pb.hr) AS hr
  FROM player_game_batting pb
  JOIN games g USING(game_pk)
  WHERE g.season = ? AND lower(g.status)='final'
  GROUP BY EXTRACT(YEAR FROM g.game_date), pb.team_id
),
pit_rollup AS (
  SELECT
    EXTRACT(YEAR FROM g.game_date)::INTEGER AS season,
    pp.team_id,
    SUM(pp.ip_outs) AS ip_outs,
    SUM(pp.er) AS er
  FROM player_game_pitching pp
  JOIN games g USING(game_pk)
  WHERE g.season = ? AND lower(g.status)='final'
  GROUP BY EXTRACT(YEAR FROM g.game_date), pp.team_id
)
SELECT
  gr.season,
  gr.team_id,
  gr.team_name,
  gr.games_played,
  gr.wins,
  gr.losses,
  gr.runs_scored,
  gr.runs_allowed,
  gr.run_differential,
  CASE
    WHEN (COALESCE(br.ab, 0) + COALESCE(br.bb, 0) + COALESCE(br.hbp, 0) + COALESCE(br.sf, 0)) = 0
      THEN NULL
    ELSE (
      (COALESCE(br.h, 0) + COALESCE(br.bb, 0) + COALESCE(br.hbp, 0))::DOUBLE
        / NULLIF((COALESCE(br.ab, 0) + COALESCE(br.bb, 0) + COALESCE(br.hbp, 0) + COALESCE(br.sf, 0)), 0)
      +
      (COALESCE(br.h, 0) + COALESCE(br.doubles, 0) + 2 * COALESCE(br.triples, 0) + 3 * COALESCE(br.hr, 0))::DOUBLE
        / NULLIF(COALESCE(br.ab, 0), 0)
    )
  END AS team_ops,
  CASE
    WHEN COALESCE(pr.ip_outs, 0) = 0 THEN NULL
    ELSE (COALESCE(pr.er, 0)::DOUBLE * 27.0 / NULLIF(COALESCE(pr.ip_outs, 0), 0))
  END AS team_era,
  NULL AS streak,
  NULL AS last_10,
  NULL AS next_game,
  NULL AS next_game_time,
  CURRENT_DATE AS as_of_date
FROM game_rollup gr
LEFT JOIN bat_rollup br
  ON br.season = gr.season AND br.team_id = gr.team_id
LEFT JOIN pit_rollup pr
  ON pr.season = gr.season AND pr.team_id = gr.team_id;
"""


AGG_PLAYER_SQL = """
INSERT OR REPLACE INTO player_season_aggregates
WITH b AS (
  SELECT
    EXTRACT(YEAR FROM g.game_date)::INTEGER AS season,
    p.player_id,
    p.team_id,
    MAX(p.player_name) AS player_name,
    COUNT(DISTINCT p.game_pk) AS games_played,
    SUM(p.ab) AS ab,
    SUM(p.r) AS r,
    SUM(p.h) AS h,
    SUM(p.rbi) AS rbi,
    SUM(p.bb) AS bb,
    SUM(p.so) AS so,
    SUM(p.hr) AS hr,
    SUM(p.doubles) AS doubles,
    SUM(p.triples) AS triples,
    SUM(p.sb) AS sb,
    SUM(p.cs) AS cs,
    SUM(p.hbp) AS hbp,
    SUM(p.sf) AS sf
  FROM player_game_batting p
  JOIN games g USING(game_pk)
  WHERE g.season=? AND lower(g.status)='final'
  GROUP BY EXTRACT(YEAR FROM g.game_date), p.player_id, p.team_id
),
p AS (
  SELECT
    EXTRACT(YEAR FROM g.game_date)::INTEGER AS season,
    q.player_id,
    q.team_id,
    SUM(q.ip_outs) AS ip_outs,
    SUM(q.er) AS er,
    SUM(q.h_allowed) AS h_allowed,
    SUM(q.bb_allowed) AS bb_allowed,
    SUM(q.so_pitched) AS so_pitched,
    SUM(q.hr_allowed) AS hr_allowed
  FROM player_game_pitching q
  JOIN games g USING(game_pk)
  WHERE g.season=? AND lower(g.status)='final'
  GROUP BY EXTRACT(YEAR FROM g.game_date), q.player_id, q.team_id
)
SELECT
  COALESCE(b.season,p.season) AS season,
  COALESCE(b.player_id,p.player_id) AS player_id,
  COALESCE(b.team_id,p.team_id) AS team_id,
  COALESCE(b.player_name,'Unknown') AS player_name,
  COALESCE(b.games_played,0) AS games_played,
  COALESCE(b.ab,0), COALESCE(b.r,0), COALESCE(b.h,0), COALESCE(b.rbi,0), COALESCE(b.bb,0),
  COALESCE(b.so,0), COALESCE(b.hr,0), COALESCE(b.doubles,0), COALESCE(b.triples,0),
  COALESCE(b.sb,0), COALESCE(b.cs,0), COALESCE(b.hbp,0), COALESCE(b.sf,0),
  CASE WHEN COALESCE(b.ab,0)=0 THEN NULL
       ELSE (
         (COALESCE(b.h,0)+COALESCE(b.bb,0)+COALESCE(b.hbp,0))::DOUBLE / NULLIF((COALESCE(b.ab,0)+COALESCE(b.bb,0)+COALESCE(b.hbp,0)+COALESCE(b.sf,0)),0)
         +
         (COALESCE(b.h,0)+COALESCE(b.doubles,0)+2*COALESCE(b.triples,0)+3*COALESCE(b.hr,0))::DOUBLE / NULLIF(COALESCE(b.ab,0),0)
       ) END AS ops,
  COALESCE(p.ip_outs,0), COALESCE(p.er,0), COALESCE(p.h_allowed,0), COALESCE(p.bb_allowed,0),
  COALESCE(p.so_pitched,0), COALESCE(p.hr_allowed,0),
  CASE WHEN COALESCE(p.ip_outs,0)=0 THEN NULL
       ELSE (COALESCE(p.er,0)::DOUBLE * 27.0 / NULLIF(COALESCE(p.ip_outs,0),0)) END AS era,
  CURRENT_DATE AS as_of_date
FROM b
FULL OUTER JOIN p
  ON b.season=p.season AND b.player_id=p.player_id AND b.team_id=p.team_id;
"""


TEAM_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW team_summary_view AS
SELECT
  team_id,
  team_name,
  wins,
  losses,
  runs_scored,
  runs_allowed,
  run_differential,
  team_era,
  team_ops,
  streak,
  last_10,
  next_game,
  next_game_time
FROM team_season_aggregates;
"""


PLAYER_SUMMARY_VIEW = """
CREATE OR REPLACE VIEW player_summary_view AS
SELECT
  player_id,
  player_name,
  team_id,
  games_played,
  ab, r, h, rbi, bb, so, hr, doubles, triples, sb, cs, hbp, sf,
  ops,
  ip_outs, er, h_allowed, bb_allowed, so_pitched, hr_allowed,
  era,
  as_of_date
FROM player_season_aggregates;
"""


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    con.execute(TEAM_SUMMARY_VIEW)
    con.execute(PLAYER_SUMMARY_VIEW)
    return con
