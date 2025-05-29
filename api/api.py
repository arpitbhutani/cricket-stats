##############################################################################
# api/api.py  –  FastAPI wrapper for Cricket Betting Stats
##############################################################################
from typing import Optional, List, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb

# ── Parquet paths (edit if needed) ──────────────────────────────────────────
# ── Parquet paths ──────────────────────────────────────────────────────────
PLAYER_PARQUET       = "api/parquet/player_batting.parquet"
BOWLER_PARQUET       = "api/parquet/bowler_summary.parquet"
TEAM_BAT_PARQUET     = "api/parquet/team_phase_summary.parquet"
TEAM_BOWL_PARQUET    = "api/parquet/team_bowling_phase_summary.parquet"
BVSB_PARQUET         = "api/parquet/batter_vs_bowler.parquet"
BVS_TEAM_PARQUET     = "api/parquet/batter_vs_team.parquet"


# ── DuckDB connection (in-memory) ───────────────────────────────────────────
con = duckdb.connect(database=":memory:")

# ── FastAPI app with CORS ───────────────────────────────────────────────────
app = FastAPI(title="Cricket Betting Stats API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── helpers ────────────────────────────────────────────────────────────────
# ── helpers ────────────────────────────────────────────────────────────────
def event_clause(event: Optional[str]) -> str:
    return "AND event_name ILIKE '%' || ? || '%'" if event else ""

def run(sql: str, params: Tuple) -> List[dict]:
    """Execute SQL; return list-of-dicts or raise 404 if empty."""
    df = con.execute(sql, params).fetchdf()          # pandas DataFrame
    if df.empty:
        raise HTTPException(status_code=404, detail="No rows found")
    return df.to_dict(orient="records")


# ═══════════════════════════════════════════════════════════════════════════
# 1️⃣  Player batting card
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/player/{name}")
def player_card(
    name: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    season: Optional[int] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{PLAYER_PARQUET}')
      WHERE batter     ILIKE '%' || ? || '%'
        AND match_type = ?
        {event_clause(event)}
        {'AND season = ?' if season else ''}
      ORDER BY season, event_name
      LIMIT {limit}
    """
    params = (name, match_type) + \
             ((event,)  if event  else ()) + \
             ((season,) if season else ())
    return run(sql, params)

# ═══════════════════════════════════════════════════════════════════════════
# 2️⃣  Bowler card
# ═══════════════════════════════════════════════════════════════════════════
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
      WHERE bowler     ILIKE '%' || ? || '%'
        AND match_type = ?
        {event_clause(event)}
        {'AND season = ?' if season else ''}
      ORDER BY season
      LIMIT {limit}
    """
    params = (name, match_type) + \
             ((event,)  if event  else ()) + \
             ((season,) if season else ())
    return run(sql, params)

# ═══════════════════════════════════════════════════════════════════════════
# 3️⃣  Team batting phase
# ═══════════════════════════════════════════════════════════════════════════
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
      WHERE batting_team ILIKE '%' || ? || '%'
        AND match_type   = ?
        {event_clause(event)}
        {'AND season = ?' if season else ''}
    """
    params = (team, match_type) + \
             ((event,)  if event  else ()) + \
             ((season,) if season else ())
    return run(sql, params)

# ═══════════════════════════════════════════════════════════════════════════
# 4️⃣  Team bowling phase
# ═══════════════════════════════════════════════════════════════════════════
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
      WHERE fielding_team ILIKE '%' || ? || '%'
        AND match_type    = ?
        {event_clause(event)}
        {'AND season = ?' if season else ''}
    """
    params = (team, match_type) + \
             ((event,)  if event  else ()) + \
             ((season,) if season else ())
    return run(sql, params)

# ═══════════════════════════════════════════════════════════════════════════
# 5️⃣  Batter vs Bowler
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/matchup/player-bowler")
def matchup_player_bowler(
    batter: str,
    bowler: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{BVSB_PARQUET}')
      WHERE batter ILIKE '%' || ? || '%'
        AND bowler ILIKE '%' || ? || '%'
        AND match_type = ?
        {event_clause(event)}
      LIMIT {limit}
    """
    params = (batter, bowler, match_type) + ((event,) if event else ())
    return run(sql, params)

# ═══════════════════════════════════════════════════════════════════════════
# 6️⃣  Batter vs Team
# ═══════════════════════════════════════════════════════════════════════════
@app.get("/matchup/player-team")
def matchup_player_team(
    batter: str,
    bowling_team: str,
    match_type: str = Query(...),
    event: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    sql = f"""
      SELECT *
      FROM read_parquet('{BVS_TEAM_PARQUET}')
      WHERE batter       ILIKE '%' || ? || '%'
        AND bowling_team ILIKE '%' || ? || '%'
        AND match_type   = ?
        {event_clause(event)}
      LIMIT {limit}
    """
    params = (batter, bowling_team, match_type) + ((event,) if event else ())
    return run(sql, params)
