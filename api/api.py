##############################################################################
# api.py  –  FastAPI wrapper with case-insensitive and substring matching
##############################################################################
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
import duckdb

# ── Parquet paths (edit if needed) ───────────────────────────────────────────
PLAYER_PARQUET       = "player_batting.parquet"
BOWLER_PARQUET       = "bowler_summary.parquet"
TEAM_BAT_PARQUET     = "team_phase_summary.parquet"
TEAM_BOWL_PARQUET    = "team_bowling_phase_summary.parquet"
BVSB_PARQUET         = "batter_vs_bowler.parquet"
BVS_TEAM_PARQUET     = "batter_vs_team.parquet"

# ── In-memory DuckDB (read-only; no file locks) ──────────────────────────────
con = duckdb.connect(database=":memory:", read_only=True)
app = FastAPI(title="Cricket Betting Stats API")

# ── Helper: run SQL + params → list[dict] ────────────────────────────────────
def q(sql: str, params: tuple) -> List[dict]:
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    except duckdb.Error as e:
        raise HTTPException(400, str(e))

# ─────────────────────────────────────────────────────────────────────────────
# 1️⃣  Player batting card
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/player/{name}")
def player_card(
    name: str,
    match_type: str = Query(..., examples=["T20", "ODI", "Test"]),
    event: Optional[str] = None,
    season: Optional[int] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{PLAYER_PARQUET}')
      WHERE batter ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
        { 'AND season = ?'         if season else '' }
      ORDER BY season, event_name
      LIMIT {limit}
    """
    params = (name, match_type) + \
             ((f"%{event}%",) if event else ()) + \
             ((season,)       if season else ())
    rows = q(sql, params)
    if not rows:
        raise HTTPException(404, f"No rows for {name}")
    return rows

# ─────────────────────────────────────────────────────────────────────────────
# 2️⃣  Bowler summary
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/bowler/{name}")
def bowler_card(
    name: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    season: Optional[int] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{BOWLER_PARQUET}')
      WHERE bowler ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
        { 'AND season = ?'         if season else '' }
      ORDER BY season
      LIMIT {limit}
    """
    params = (name, match_type) + \
             ((f"%{event}%",) if event else ()) + \
             ((season,)       if season else ())
    return q(sql, params)

# ─────────────────────────────────────────────────────────────────────────────
# 3️⃣  Team batting phase stats
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/team/batting/{team}")
def team_bat_phase(
    team: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    season: Optional[int] = None,
):
    sql = f"""
      SELECT *
      FROM read_parquet('{TEAM_BAT_PARQUET}')
      WHERE batting_team ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
        { 'AND season = ?'         if season else '' }
    """
    params = (team, match_type) + \
             ((f"%{event}%",) if event else ()) + \
             ((season,)       if season else ())
    return q(sql, params)

# ─────────────────────────────────────────────────────────────────────────────
# 4️⃣  Team bowling phase stats
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/team/bowling/{team}")
def team_bowl_phase(
    team: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    season: Optional[int] = None,
):
    sql = f"""
      SELECT *
      FROM read_parquet('{TEAM_BOWL_PARQUET}')
      WHERE fielding_team ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
        { 'AND season = ?'         if season else '' }
    """
    params = (team, match_type) + \
             ((f"%{event}%",) if event else ()) + \
             ((season,)       if season else ())
    return q(sql, params)

# ─────────────────────────────────────────────────────────────────────────────
# 5️⃣  Batter-vs-Bowler match-up
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/matchup/player-bowler")
def player_vs_bowler(
    batter: str,
    bowler: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{BVSB_PARQUET}')
      WHERE batter ILIKE ?
        AND bowler ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
      LIMIT {limit}
    """
    params = (batter, bowler, match_type) + \
             ((f"%{event}%",) if event else ())
    return q(sql, params)

# ─────────────────────────────────────────────────────────────────────────────
# 6️⃣  Batter-vs-Team match-up
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/matchup/player-team")
def player_vs_team(
    batter: str,
    bowling_team: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{BVS_TEAM_PARQUET}')
      WHERE batter ILIKE ?
        AND bowling_team ILIKE ?
        AND match_type = ?
        { 'AND event_name ILIKE ?' if event else '' }
      LIMIT {limit}
    """
    params = (batter, bowling_team, match_type) + \
             ((f"%{event}%",) if event else ())
    return q(sql, params)
