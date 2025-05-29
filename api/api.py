##############################################################################
# FULL Cricket Betting Stats API
# • search endpoints
# • batters summary  (runs, avg, SR, bp4/6, % boundaries, top-X helpers)
# • bowlers summary  (wkts, econ, SR, avg, % multi-wicket inns)
# • team phase views
##############################################################################
from typing import Optional, List, Tuple, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb, math, os, datetime

DATA_DIR      = "api/parquet"
BALLS         = os.path.join(DATA_DIR, "cricket_balls.parquet")
TEAM_BAT_PQ   = os.path.join(DATA_DIR, "team_phase_summary.parquet")
TEAM_BOWL_PQ  = os.path.join(DATA_DIR, "team_bowling_phase_summary.parquet")

con = duckdb.connect(database=":memory:")

app = FastAPI(title="Cricket Betting Stats – v2")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ── utility -----------------------------------------------------------------
def wild(txt: Optional[str]) -> str:
    return f"ILIKE '%{txt}%'" if txt else "IS NOT NULL"

def season_filter(lookback: int) -> str:
    return "TRUE" if lookback <= 0 else f"season >= {datetime.date.today().year - lookback}"

def sanitise(v: Any) -> Any:
    return None if isinstance(v, float) and not math.isfinite(v) else v

def cur_to_dict(cur) -> List[dict]:
    cols = [d[0] for d in cur.description]
    rows = [{c: sanitise(v) for c, v in zip(cols, r)} for r in cur.fetchall()]
    if not rows:
        raise HTTPException(404, "No rows found")
    return rows

# ═════════════════════ SEARCH  ══════════════════════════════════════════════
@app.get("/search/{kind}")
def search(kind: str, query: str = "", match_type: str = "T20", limit: int = 20):
    col = {
        "players": "batter",
        "teams":   "batting_team",
        "events":  "event_name",
        "venues":  "venue",
    }.get(kind)
    if col is None:
        raise HTTPException(400, "kind must be players / teams / events / venues")

    extra = "AND match_type = ?" if kind == "events" else ""
    sql = f"""
        SELECT DISTINCT {col} AS name
        FROM read_parquet('{BALLS}')
        WHERE {col} ILIKE '%' || ? || '%'
        {extra}
        LIMIT ?
    """
    params = (query, match_type, limit) if extra else (query, limit)
    return cur_to_dict(con.execute(sql, params))

# ═══════════════════ BATTERS =================================================
@app.get("/batting/summary")
def batting_summary(
    match_type: str,
    years: int = Query(3, ge=0, le=25),
    min_inns: int = Query(3, ge=1),
    event: Optional[str] = None,
    team: Optional[str] = None,
    opponent: Optional[str] = None,
    venue: Optional[str] = None,
    innings: Optional[int] = Query(None, ge=1, le=4),
    batter: Optional[str] = None,
):
    sql = f"""
      WITH F AS (
        SELECT *
        FROM read_parquet('{BALLS}')
        WHERE match_type = ?
          AND ({wild(event)})
          AND ({wild(team)})
          AND ({wild(opponent)})
          AND ({wild(venue)})
          AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
          AND ({wild(batter)})
          AND ({season_filter(years)})
      )
      SELECT
        batter,
        COUNT(DISTINCT match_id)                       AS inns,
        SUM(runs_batter)                               AS runs,
        SUM(CASE WHEN wicket_type IS NOT NULL
                  AND player_out = batter THEN 1 END)  AS outs,
        COUNT(*)                                       AS balls,
        SUM(is_boundary_4::INT)                        AS fours,
        SUM(is_boundary_6::INT)                        AS sixes
      FROM F
      GROUP BY batter
      HAVING inns >= ?
      ORDER BY runs DESC
      LIMIT 1000
    """
    cur = con.execute(sql, (match_type, min_inns))
    data = cur_to_dict(cur)

    for r in data:
        r.update({
            "avg": sanitise(round(r["runs"]/r["outs"],2) if r["outs"] else None),
            "sr":  round(100*r["runs"]/r["balls"],2),
            "bp4": sanitise(round(r["balls"]/r["fours"],1) if r["fours"] else None),
            "bp6": sanitise(round(r["balls"]/r["sixes"],1) if r["sixes"] else None),
            "%4s": sanitise(round(100*r["fours"]/r["balls"],2)),
            "%6s": sanitise(round(100*r["sixes"]/r["balls"],2)),
        })
    return data

# ═══════════════════ BOWLERS =================================================
@app.get("/bowling/summary")
def bowling_summary(
    match_type: str,
    years: int = Query(3, ge=0, le=25),
    min_inns: int = Query(3, ge=1),
    event: Optional[str] = None,
    team: Optional[str] = None,
    opponent: Optional[str] = None,
    venue: Optional[str] = None,
    innings: Optional[int] = Query(None, ge=1, le=4),
    bowler: Optional[str] = None,
):
    sql = f"""
      WITH F AS (
        SELECT *
        FROM read_parquet('{BALLS}')
        WHERE match_type = ?
          AND ({wild(event)})
          AND ({wild(team)})
          AND ({wild(opponent)})
          AND ({wild(venue)})
          AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
          AND ({wild(bowler)})
          AND ({season_filter(years)})
      ),
      per_match AS (
        SELECT match_id, bowler,
               COUNT(*)                       AS balls,
               SUM(runs_total)                AS runs,
               SUM(CASE WHEN wicket_type IS NOT NULL
                         AND bowler = ANY(string_split(player_out,','))
                        THEN 1 END)           AS wkts
        FROM F GROUP BY match_id, bowler
      )
      SELECT bowler,
             COUNT(*)        AS inns,
             SUM(balls)      AS balls,
             SUM(runs)       AS runs,
             SUM(wkts)       AS wkts
      FROM per_match
      GROUP BY bowler
      HAVING inns >= ?
      ORDER BY wkts DESC
      LIMIT 1000
    """
    cur = con.execute(sql, (match_type, min_inns))
    data = cur_to_dict(cur)
    for r in data:
        overs = r["balls"]/6
        r.update({
            "econ": sanitise(round(r["runs"]/overs,2)),
            "sr":   sanitise(round(r["balls"]/r["wkts"],1) if r["wkts"] else None),
            "avg":  sanitise(round(r["runs"]/r["wkts"],2)  if r["wkts"] else None),
        })
    return data

# ═══════════════════ TEAM PHASES ============================================
@app.get("/team/batting")
def team_bat(match_type: str, event: str, team: str):
    cur = con.execute(
        f"""SELECT * FROM read_parquet('{TEAM_BAT_PQ}')
            WHERE match_type=? AND event_name=? AND batting_team=?""",
        (match_type, event, team),
    )
    return cur_to_dict(cur)

@app.get("/team/bowling")
def team_bowl(match_type: str, event: str, team: str):
    cur = con.execute(
        f"""SELECT * FROM read_parquet('{TEAM_BOWL_PQ}')
            WHERE match_type=? AND event_name=? AND fielding_team=?""",
        (match_type, event, team),
    )
    return cur_to_dict(cur)
