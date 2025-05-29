import duckdb,textwrap
con = duckdb.connect("cricket.duckdb")


con.execute("""
    CREATE OR REPLACE VIEW balls AS
    SELECT * FROM 'balls_parted/**/*.parquet'
""")


con.execute("""
/*──────────────────────────────────────────────────────────────────────────────
   Player batting summary  •  split by match_type & event_name (tournament)
──────────────────────────────────────────────────────────────────────────────*/
COPY (

WITH per_ball AS (
  SELECT
    season,
    match_type,
    event_name,                 -- ← just use the column that already exists
    match_id,
    batter,

    runs_batter            AS runs,
    is_boundary_4::INT     AS four,
    is_boundary_6::INT     AS six,

    CASE WHEN extras_type = 'wides' THEN 0 ELSE 1 END AS legal_ball,
    CASE WHEN wicket_type IS NOT NULL
           AND player_out = batter
         THEN 1 ELSE 0 END            AS dismissal
  FROM balls
),

per_innings AS (
  SELECT
    season, match_type, event_name,
    match_id, batter,

    SUM(runs)         AS runs,
    SUM(four)         AS fours,
    SUM(six)          AS sixes,
    SUM(legal_ball)   AS balls_faced,
    SUM(dismissal)    AS outs
  FROM per_ball
  GROUP BY season, match_type, event_name, match_id, batter
)

SELECT
  /* grouping keys */
  season,
  match_type,
  event_name,
  batter,

  /* totals */
  COUNT(*)                  AS innings,
  SUM(runs)                 AS runs,
  SUM(fours)                AS fours,
  SUM(sixes)                AS sixes,
  SUM(balls_faced)          AS balls_faced,

  /* core ratios */
  100.0 * SUM(runs) / NULLIF(SUM(balls_faced),0)          AS strike_rate,
  CASE WHEN SUM(outs)=0 THEN NULL
       ELSE CAST(SUM(runs) AS DOUBLE)/SUM(outs) END       AS average,

  /* raw inning counts — fours 1-5+, sixes 1-3+ */
  SUM(CASE WHEN fours >= 1 THEN 1 ELSE 0 END) AS n_inns_≥1_fours,
  SUM(CASE WHEN fours >= 2 THEN 1 ELSE 0 END) AS n_inns_≥2_fours,
  SUM(CASE WHEN fours >= 3 THEN 1 ELSE 0 END) AS n_inns_≥3_fours,
  SUM(CASE WHEN fours >= 4 THEN 1 ELSE 0 END) AS n_inns_≥4_fours,
  SUM(CASE WHEN fours >= 5 THEN 1 ELSE 0 END) AS n_inns_≥5_fours,

  SUM(CASE WHEN sixes >= 1 THEN 1 ELSE 0 END) AS n_inns_≥1_sixes,
  SUM(CASE WHEN sixes >= 2 THEN 1 ELSE 0 END) AS n_inns_≥2_sixes,
  SUM(CASE WHEN sixes >= 3 THEN 1 ELSE 0 END) AS n_inns_≥3_sixes,

  /* percentages */
  100.0 * n_inns_≥1_fours / innings  AS pct_inns_≥1_fours,
  100.0 * n_inns_≥2_fours / innings  AS pct_inns_≥2_fours,
  100.0 * n_inns_≥3_fours / innings  AS pct_inns_≥3_fours,
  100.0 * n_inns_≥4_fours / innings  AS pct_inns_≥4_fours,
  100.0 * n_inns_≥5_fours / innings  AS pct_inns_≥5_fours,

  100.0 * n_inns_≥1_sixes / innings  AS pct_inns_≥1_sixes,
  100.0 * n_inns_≥2_sixes / innings  AS pct_inns_≥2_sixes,
  100.0 * n_inns_≥3_sixes / innings  AS pct_inns_≥3_sixes

FROM per_innings
GROUP BY season, match_type, event_name, batter

)
TO 'player_batting.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")


con.execute("""
COPY (

WITH per_ball AS (
  SELECT
    season,
    match_type,
    event_name,
    match_id,
    bowler,

    runs_total                      AS runs_conceded,

    -- legal ball flag (wides / no-balls excluded from ball count)
    CASE WHEN extras_type IN ('wides','no-balls') THEN 0 ELSE 1 END
      AS legal_ball,

    CASE WHEN runs_total = 0 THEN 1 ELSE 0 END                     AS dot_ball,
    CASE WHEN is_boundary_4 OR is_boundary_6 THEN 1 ELSE 0 END     AS boundary,

    -- wicket credited to this bowler?
    CASE
      WHEN wicket_type IS NOT NULL
       AND bowler = (SELECT bowler)
      THEN 1 ELSE 0
    END AS wicket
  FROM balls
),

per_innings AS (
  SELECT
    season, match_type, event_name,
    match_id, bowler,

    SUM(legal_ball)       AS balls_bowled,
    SUM(runs_conceded)    AS runs_conceded,
    SUM(wicket)           AS wickets,
    SUM(dot_ball)         AS dot_balls,
    SUM(boundary)         AS boundaries_conceded
  FROM per_ball
  GROUP BY season, match_type, event_name, match_id, bowler
)

SELECT
  /* grouping */
  season,
  match_type,
  event_name,
  bowler,

  COUNT(DISTINCT match_id)           AS matches,
  COUNT(*)                           AS innings_bowled,

  SUM(balls_bowled)                  AS balls_bowled,
  ROUND(SUM(balls_bowled) / 6.0, 2)  AS overs_bowled,
  SUM(runs_conceded)                 AS runs_conceded,
  SUM(wickets)                       AS wickets,
  SUM(dot_balls)                     AS dot_balls,
  SUM(boundaries_conceded)           AS boundaries_conceded,

  /* rate stats */
  CASE WHEN SUM(balls_bowled)=0 THEN NULL
       ELSE 6.0 * SUM(runs_conceded) / SUM(balls_bowled) END            AS economy,

  CASE WHEN SUM(wickets)=0 THEN NULL
       ELSE SUM(balls_bowled)::DOUBLE / SUM(wickets) END                AS strike_rate,

  CASE WHEN SUM(wickets)=0 THEN NULL
       ELSE SUM(runs_conceded)::DOUBLE / SUM(wickets) END               AS average,

  /* big-haul raw counts (NOW includes 1+) */
  SUM(CASE WHEN wickets >= 1 THEN 1 ELSE 0 END) AS n_inns_≥1_wkts,
  SUM(CASE WHEN wickets >= 2 THEN 1 ELSE 0 END) AS n_inns_≥2_wkts,
  SUM(CASE WHEN wickets >= 3 THEN 1 ELSE 0 END) AS n_inns_≥3_wkts,
  SUM(CASE WHEN wickets >= 4 THEN 1 ELSE 0 END) AS n_inns_≥4_wkts,
  SUM(CASE WHEN wickets >= 5 THEN 1 ELSE 0 END) AS n_inns_≥5_wkts,

  /* percentages */
  100.0 * n_inns_≥1_wkts / innings_bowled       AS pct_inns_≥1_wkts,
  100.0 * n_inns_≥2_wkts / innings_bowled       AS pct_inns_≥2_wkts,
  100.0 * n_inns_≥3_wkts / innings_bowled       AS pct_inns_≥3_wkts,
  100.0 * n_inns_≥4_wkts / innings_bowled       AS pct_inns_≥4_wkts,
  100.0 * n_inns_≥5_wkts / innings_bowled       AS pct_inns_≥5_wkts,

  100.0 * SUM(dot_balls)::DOUBLE / NULLIF(SUM(balls_bowled),0)          AS dot_pct

FROM per_innings
GROUP BY season, match_type, event_name, bowler

)
TO 'bowler_summary.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")

con.execute(textwrap.dedent("""
COPY (

/*──────────────────────────────────────────────────────────────
  1. Flag every delivery
──────────────────────────────────────────────────────────────*/
WITH per_ball AS (
  SELECT
    season,
    match_type,
    event_name,
    match_id,
    innings_number,
    batting_team,

    ball_number_absolute,                 -- already 1-based in master table
    runs_total,
    is_boundary_4::INT  AS four,
    is_boundary_6::INT  AS six,
    CASE WHEN player_out IS NOT NULL
          AND wicket_type IS NOT NULL
         THEN 1 ELSE 0 END AS wkt
  FROM balls
),

/*──────────────────────────────────────────────────────────────
  2. Collapse to one row per innings × batting team
──────────────────────────────────────────────────────────────*/
per_innings AS (
  SELECT
    season, match_type, event_name,
    match_id, innings_number,
    batting_team,

    /* whole-innings totals */
    SUM(runs_total)  AS runs_total,
    SUM(four)        AS fours_total,
    SUM(six)         AS sixes_total,
    SUM(wkt)         AS wkts_total,

    /* ---- 6-over window (≤36 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 36 THEN runs_total ELSE 0 END) AS runs_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN four       ELSE 0 END) AS fours_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN six        ELSE 0 END) AS sixes_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN wkt        ELSE 0 END) AS wkts_6,

    /* ---- 10-over window (≤60 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 60 THEN runs_total ELSE 0 END) AS runs_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN four       ELSE 0 END) AS fours_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN six        ELSE 0 END) AS sixes_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN wkt        ELSE 0 END) AS wkts_10,

    /* ---- 12-over window (≤72 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 72 THEN runs_total ELSE 0 END) AS runs_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN four       ELSE 0 END) AS fours_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN six        ELSE 0 END) AS sixes_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN wkt        ELSE 0 END) AS wkts_12,

    /* ---- 15-over window (≤90 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 90 THEN runs_total ELSE 0 END) AS runs_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN four       ELSE 0 END) AS fours_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN six        ELSE 0 END) AS sixes_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN wkt        ELSE 0 END) AS wkts_15
  FROM per_ball
  GROUP BY
    season, match_type, event_name,
    match_id, innings_number, batting_team
)

/*──────────────────────────────────────────────────────────────
  3. Season × format × tournament × team roll-up
──────────────────────────────────────────────────────────────*/
SELECT
  season,
  match_type,
  event_name,
  batting_team,

  COUNT(*)                      AS innings,

  SUM(runs_total)   AS runs_total,
  SUM(fours_total)  AS fours_total,
  SUM(sixes_total)  AS sixes_total,
  SUM(wkts_total)   AS wkts_total,

  SUM(runs_6)   AS runs_6,   SUM(fours_6)  AS fours_6,   SUM(sixes_6)  AS sixes_6,   SUM(wkts_6)  AS wkts_6,
  SUM(runs_10)  AS runs_10,  SUM(fours_10) AS fours_10,  SUM(sixes_10) AS sixes_10,  SUM(wkts_10) AS wkts_10,
  SUM(runs_12)  AS runs_12,  SUM(fours_12) AS fours_12,  SUM(sixes_12) AS sixes_12,  SUM(wkts_12) AS wkts_12,
  SUM(runs_15)  AS runs_15,  SUM(fours_15) AS fours_15,  SUM(sixes_15) AS sixes_15,  SUM(wkts_15) AS wkts_15,

  /* handy phase averages */
  ROUND(SUM(runs_6 )::DOUBLE / COUNT(*), 2) AS avg_runs_6,
  ROUND(SUM(runs_10)::DOUBLE / COUNT(*), 2) AS avg_runs_10,
  ROUND(SUM(runs_12)::DOUBLE / COUNT(*), 2) AS avg_runs_12,
  ROUND(SUM(runs_15)::DOUBLE / COUNT(*), 2) AS avg_runs_15

FROM per_innings
GROUP BY season, match_type, event_name, batting_team

)
TO 'team_phase_summary.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
"""))

con.execute(textwrap.dedent("""
COPY (

/*──────────────────────────────────────────────────────────────
  1. Flag every delivery
──────────────────────────────────────────────────────────────*/
WITH per_ball AS (
  SELECT
    season,
    match_type,
    event_name,
    match_id,
    innings_number,
    batting_team,

    ball_number_absolute,                 -- already 1-based in master table
    runs_total,
    is_boundary_4::INT  AS four,
    is_boundary_6::INT  AS six,
    CASE WHEN player_out IS NOT NULL
          AND wicket_type IS NOT NULL
         THEN 1 ELSE 0 END AS wkt
  FROM balls
),

/*──────────────────────────────────────────────────────────────
  2. Collapse to one row per innings × batting team
──────────────────────────────────────────────────────────────*/
per_innings AS (
  SELECT
    season, match_type, event_name,
    match_id, innings_number,
    batting_team,

    /* whole-innings totals */
    SUM(runs_total)  AS runs_total,
    SUM(four)        AS fours_total,
    SUM(six)         AS sixes_total,
    SUM(wkt)         AS wkts_total,

    /* ---- 6-over window (≤36 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 36 THEN runs_total ELSE 0 END) AS runs_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN four       ELSE 0 END) AS fours_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN six        ELSE 0 END) AS sixes_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN wkt        ELSE 0 END) AS wkts_6,

    /* ---- 10-over window (≤60 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 60 THEN runs_total ELSE 0 END) AS runs_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN four       ELSE 0 END) AS fours_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN six        ELSE 0 END) AS sixes_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN wkt        ELSE 0 END) AS wkts_10,

    /* ---- 12-over window (≤72 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 72 THEN runs_total ELSE 0 END) AS runs_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN four       ELSE 0 END) AS fours_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN six        ELSE 0 END) AS sixes_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN wkt        ELSE 0 END) AS wkts_12,

    /* ---- 15-over window (≤90 balls) ---- */
    SUM(CASE WHEN ball_number_absolute <= 90 THEN runs_total ELSE 0 END) AS runs_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN four       ELSE 0 END) AS fours_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN six        ELSE 0 END) AS sixes_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN wkt        ELSE 0 END) AS wkts_15
  FROM per_ball
  GROUP BY
    season, match_type, event_name,
    match_id, innings_number, batting_team
)

/*──────────────────────────────────────────────────────────────
  3. Season × format × tournament × team roll-up
──────────────────────────────────────────────────────────────*/
SELECT
  season,
  match_type,
  event_name,
  batting_team,

  COUNT(*)                      AS innings,

  SUM(runs_total)   AS runs_total,
  SUM(fours_total)  AS fours_total,
  SUM(sixes_total)  AS sixes_total,
  SUM(wkts_total)   AS wkts_total,

  SUM(runs_6)   AS runs_6,   SUM(fours_6)  AS fours_6,   SUM(sixes_6)  AS sixes_6,   SUM(wkts_6)  AS wkts_6,
  SUM(runs_10)  AS runs_10,  SUM(fours_10) AS fours_10,  SUM(sixes_10) AS sixes_10,  SUM(wkts_10) AS wkts_10,
  SUM(runs_12)  AS runs_12,  SUM(fours_12) AS fours_12,  SUM(sixes_12) AS sixes_12,  SUM(wkts_12) AS wkts_12,
  SUM(runs_15)  AS runs_15,  SUM(fours_15) AS fours_15,  SUM(sixes_15) AS sixes_15,  SUM(wkts_15) AS wkts_15,

  /* handy phase averages */
  ROUND(SUM(runs_6 )::DOUBLE / COUNT(*), 2) AS avg_runs_6,
  ROUND(SUM(runs_10)::DOUBLE / COUNT(*), 2) AS avg_runs_10,
  ROUND(SUM(runs_12)::DOUBLE / COUNT(*), 2) AS avg_runs_12,
  ROUND(SUM(runs_15)::DOUBLE / COUNT(*), 2) AS avg_runs_15

FROM per_innings
GROUP BY season, match_type, event_name, batting_team

)
TO 'team_phase_summary.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
"""))

con.execute(textwrap.dedent("""

COPY (

/*─────────────────────────────────────────────────────────────
  1. Mark each delivery from the bowling side’s POV
─────────────────────────────────────────────────────────────*/
WITH per_ball AS (
  SELECT
    season,
    match_type,
    event_name,
    match_id,
    innings_number,

    bowling_team,                        -- ← fielding side key
    ball_number_absolute,
    runs_total,                          -- runs conceded on this ball
    is_boundary_4::INT  AS four_conc,
    is_boundary_6::INT  AS six_conc,

    CASE WHEN player_out IS NOT NULL
          AND wicket_type IS NOT NULL
         THEN 1 ELSE 0 END AS wkt
  FROM balls
),

/*─────────────────────────────────────────────────────────────
  2. Aggregate once per innings × bowling team
─────────────────────────────────────────────────────────────*/
per_innings AS (
  SELECT
    season, match_type, event_name,
    match_id, innings_number,
    bowling_team                                   AS fielding_team,

    /* whole-innings totals */
    SUM(runs_total)  AS runs_conc_total,
    SUM(four_conc)   AS fours_conc_total,
    SUM(six_conc)    AS sixes_conc_total,
    SUM(wkt)         AS wkts_total,

    /* ---- phase windows ---- */
    /* 0-6 overs */
    SUM(CASE WHEN ball_number_absolute <= 36 THEN runs_total  ELSE 0 END) AS runs_conc_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN four_conc   ELSE 0 END) AS fours_conc_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN six_conc    ELSE 0 END) AS sixes_conc_6,
    SUM(CASE WHEN ball_number_absolute <= 36 THEN wkt         ELSE 0 END) AS wkts_6,

    /* 0-10 overs */
    SUM(CASE WHEN ball_number_absolute <= 60 THEN runs_total  ELSE 0 END) AS runs_conc_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN four_conc   ELSE 0 END) AS fours_conc_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN six_conc    ELSE 0 END) AS sixes_conc_10,
    SUM(CASE WHEN ball_number_absolute <= 60 THEN wkt         ELSE 0 END) AS wkts_10,

    /* 0-12 overs */
    SUM(CASE WHEN ball_number_absolute <= 72 THEN runs_total  ELSE 0 END) AS runs_conc_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN four_conc   ELSE 0 END) AS fours_conc_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN six_conc    ELSE 0 END) AS sixes_conc_12,
    SUM(CASE WHEN ball_number_absolute <= 72 THEN wkt         ELSE 0 END) AS wkts_12,

    /* 0-15 overs */
    SUM(CASE WHEN ball_number_absolute <= 90 THEN runs_total  ELSE 0 END) AS runs_conc_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN four_conc   ELSE 0 END) AS fours_conc_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN six_conc    ELSE 0 END) AS sixes_conc_15,
    SUM(CASE WHEN ball_number_absolute <= 90 THEN wkt         ELSE 0 END) AS wkts_15
  FROM per_ball
  GROUP BY
    season, match_type, event_name,
    match_id, innings_number, bowling_team
)

/*─────────────────────────────────────────────────────────────
  3. Roll up to season × format × tournament × fielding team
─────────────────────────────────────────────────────────────*/
SELECT
  season,
  match_type,
  event_name,
  fielding_team,

  COUNT(*)                     AS innings,

  /* whole-innings totals */
  SUM(runs_conc_total)   AS runs_conc_total,
  SUM(fours_conc_total)  AS fours_conc_total,
  SUM(sixes_conc_total)  AS sixes_conc_total,
  SUM(wkts_total)        AS wkts_total,

  /* phase totals */
  SUM(runs_conc_6)   AS runs_conc_6,   SUM(fours_conc_6)  AS fours_conc_6,   SUM(sixes_conc_6)  AS sixes_conc_6,   SUM(wkts_6)   AS wkts_6,
  SUM(runs_conc_10)  AS runs_conc_10,  SUM(fours_conc_10) AS fours_conc_10,  SUM(sixes_conc_10) AS sixes_conc_10,  SUM(wkts_10)  AS wkts_10,
  SUM(runs_conc_12)  AS runs_conc_12,  SUM(fours_conc_12) AS fours_conc_12,  SUM(sixes_conc_12) AS sixes_conc_12,  SUM(wkts_12)  AS wkts_12,
  SUM(runs_conc_15)  AS runs_conc_15,  SUM(fours_conc_15) AS fours_conc_15,  SUM(sixes_conc_15) AS sixes_conc_15,  SUM(wkts_15)  AS wkts_15,

  /* convenience averages (runs conceded per innings) */
  ROUND(SUM(runs_conc_6 )::DOUBLE / COUNT(*), 2) AS avg_runs_conc_6,
  ROUND(SUM(runs_conc_10)::DOUBLE / COUNT(*), 2) AS avg_runs_conc_10,
  ROUND(SUM(runs_conc_12)::DOUBLE / COUNT(*), 2) AS avg_runs_conc_12,
  ROUND(SUM(runs_conc_15)::DOUBLE / COUNT(*), 2) AS avg_runs_conc_15

FROM per_innings
GROUP BY season, match_type, event_name, fielding_team

)
TO 'team_bowling_phase_summary.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD)
"""))

print("✓ all summary parquets rebuilt")