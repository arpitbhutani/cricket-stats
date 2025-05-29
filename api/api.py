##############################################################################
# FastAPI – FULL Cricket Betting Stats API
##############################################################################
from typing import Optional, Tuple, List, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb, math, os

# ── dataset paths (parquets sit in api/parquet/) ────────────────────────────
DATA_DIR           = "api/parquet"
BALLS              = os.path.join(DATA_DIR, "cricket_balls.parquet")
PLAYER_PARQ        = os.path.join(DATA_DIR, "player_batting.parquet")
BOWLER_PARQ        = os.path.join(DATA_DIR, "bowler_summary.parquet")
TEAM_BAT_PARQ      = os.path.join(DATA_DIR, "team_phase_summary.parquet")
TEAM_BOWL_PARQ     = os.path.join(DATA_DIR, "team_bowling_phase_summary.parquet")

con = duckdb.connect(database=":memory:")

# ── FastAPI w/ CORS ─────────────────────────────────────────────────────────
app = FastAPI(title="Cricket Betting Stats • Full API")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# ── helpers ─────────────────────────────────────────────────────────────────
def wild(txt: Optional[str]) -> str:
    return f"ILIKE '%{txt}%'" if txt else "IS NOT NULL"

def nf(v: Any) -> Any:                # NaN / ±Inf → None
    return None if isinstance(v, float) and not math.isfinite(v) else v

def rows(cur) -> List[dict]:          # cursor → list-of-dicts w/ sanitise
    c = [d[0] for d in cur.description]
    r = [{k: nf(v) for k, v in zip(c, row)} for row in cur.fetchall()]
    if not r:
        raise HTTPException(404, "No rows found")
    return r

def season_clause(season: Optional[str]) -> str:
    return "AND season ILIKE '%' || ? || '%'" if season else ""

# ═══════════════════════════ SEARCH ENDPOINTS ═══════════════════════════════
@app.get("/search/players")   # ?query=ko
def search_players(query: str, limit: int = 20):
    cur = con.execute(
        f"""SELECT batter AS name, COUNT(*) AS balls
            FROM read_parquet('{PLAYER_PARQ}')
            WHERE batter ILIKE '%' || ? || '%'
            GROUP BY batter ORDER BY balls DESC LIMIT ?""",
        (query, limit),
    )
    return rows(cur)

@app.get("/search/events")    # ?match_type=T20&query=blast
def search_events(match_type: str, query: str = "", limit: int = 20):
    cur = con.execute(
        f"""SELECT DISTINCT event_name AS name
            FROM read_parquet('{PLAYER_PARQ}')
            WHERE match_type = ? AND event_name ILIKE '%' || ? || '%'
            LIMIT ?""",
        (match_type, query, limit),
    )
    return rows(cur)

@app.get("/search/venues")    # ?query=lord
def search_venues(query: str = "", limit: int = 20):
    cur = con.execute(
        f"""SELECT DISTINCT venue AS name
            FROM read_parquet('{PLAYER_PARQ}')
            WHERE venue ILIKE '%' || ? || '%' LIMIT ?""",
        (query, limit),
    )
    return rows(cur)

@app.get("/search/teams")     # ?query=eng
def search_teams(query: str = "", limit: int = 20):
    cur = con.execute(
        f"""SELECT DISTINCT batting_team AS name
            FROM read_parquet('{PLAYER_PARQ}')
            WHERE batting_team ILIKE '%' || ? || '%' LIMIT ?""",
        (query, limit),
    )
    return rows(cur)

# ═══════════════════════════ BATTING SUMMARY ═══════════════════════════════
@app.get("/batting/summary")
def batting(
    match_type: str,
    event: Optional[str] = None,
    batting_team: Optional[str] = None,
    opponent: Optional[str] = None,
    venue: Optional[str] = None,
    innings: Optional[int] = Query(None, ge=1, le=4),
    batter: Optional[str] = None,
    run_threshold: int = Query(30, ge=1),
):
    sql = f"""
      SELECT
        batter,
        COUNT(DISTINCT match_id)                     AS inns,
        SUM(runs_batter)                             AS runs,
        SUM(CASE WHEN wicket_type IS NOT NULL
                  AND player_out = batter THEN 1 END) AS outs,
        COUNT(*)                                     AS balls,
        SUM(is_boundary_4::INT)                      AS fours,
        SUM(is_boundary_6::INT)                      AS sixes,
        SUM(CASE WHEN runs_batter >= 10 THEN 1 END)  AS inns_10,
        SUM(CASE WHEN runs_batter >= 20 THEN 1 END)  AS inns_20,
        SUM(CASE WHEN runs_batter >= 30 THEN 1 END)  AS inns_30,
        SUM(CASE WHEN runs_batter >= ?  THEN 1 END)  AS inns_thr
      FROM read_parquet('{BALLS}')
      WHERE match_type = ?
        AND ({wild(event)})
        AND ({wild(batting_team)})
        AND ({wild(opponent)})
        AND ({wild(venue)})
        AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
        AND ({wild(batter)})
      GROUP BY batter ORDER BY runs DESC LIMIT 500
    """
    cur = con.execute(
        sql,
        (run_threshold, match_type),
    )
    out = rows(cur)
    for r in out:
        r["avg"] = None if r["outs"] == 0 else round(r["runs"] / r["outs"], 2)
        r["sr"]  = round(100 * r["runs"] / r["balls"], 2)
        r["bp4"] = None if r["fours"] == 0 else round(r["balls"] / r["fours"], 1)
        r["bp6"] = None if r["sixes"] == 0 else round(r["balls"] / r["sixes"], 1)
        for k, thr in zip(("10", "20", "30", "thr"), (r["inns_10"], r["inns_20"], r["inns_30"], r["inns_thr"])):
            r[f"pct_{k}+"] = nf(round(100 * thr / r["inns"], 1))
    return out

# ═══════════════════════════ BOWLING SUMMARY ═══════════════════════════════
@app.get("/bowling/summary")
def bowling(
    match_type: str,
    event: Optional[str] = None,
    bowling_team: Optional[str] = None,
    opponent: Optional[str] = None,
    venue: Optional[str] = None,
    innings: Optional[int] = Query(None, ge=1, le=4),
    bowler: Optional[str] = None,
    wkt_threshold: int = Query(3, ge=1),
):
    sql = f"""
      SELECT
        bowler,
        COUNT(DISTINCT match_id)                       AS inns,
        COUNT(*)                                       AS balls,
        SUM(runs_total)                                AS runs_conc,
        SUM(CASE WHEN wicket_type IS NOT NULL
                  AND bowler = ANY(string_split(player_out,','))
                 THEN 1 END)                           AS wkts,
        SUM(CASE WHEN wkt IS NOT NULL THEN 1 END)      AS innings_wkt,
        SUM(CASE WHEN wkts >= 1 THEN 1 END)            AS inns_1,
        SUM(CASE WHEN wkts >= 2 THEN 1 END)            AS inns_2,
        SUM(CASE WHEN wkts >= 3 THEN 1 END)            AS inns_3,
        SUM(CASE WHEN wkts >= ? THEN 1 END)            AS inns_thr
      FROM (
        SELECT
          match_id,
          bowler,
          SUM(runs_total)  AS runs_total,
          SUM(CASE WHEN wicket_type IS NOT NULL
                   AND bowler = ANY(string_split(player_out,','))
                  THEN 1 END) AS wkts
        FROM read_parquet('{BALLS}')
        WHERE match_type = ?
          AND ({wild(event)})
          AND ({wild(bowling_team)})
          AND ({wild(opponent)})
          AND ({wild(venue)})
          AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
          AND ({wild(bowler)})
        GROUP BY match_id, bowler
      )
      GROUP BY bowler ORDER BY wkts DESC LIMIT 500
    """
    cur = con.execute(
        sql,
        (wkt_threshold, match_type),
    )
    out = rows(cur)
    for r in out:
        overs = r["balls"] / 6
        r["econ"] = nf(round(r["runs_conc"] / overs, 2))
        r["sr"]   = nf(round(r["balls"] / r["wkts"], 1)) if r["wkts"] else None
        r["avg"]  = nf(round(r["runs_conc"] / r["wkts"], 2)) if r["wkts"] else None
        for k, x in zip(("1", "2", "3", "thr"), (r["inns_1"], r["inns_2"], r["inns_3"], r["inns_thr"])):
            r[f"pct_{k}w+"] = nf(round(100 * x / r["inns"], 1))
    return out

# ═══════════════════════════ TEAM SUMMARY ══════════════════════════════════
@app.get("/team/batting")
def team_bat(match_type: str, event: str, team: str):
    cur = con.execute(
        f"""SELECT * FROM read_parquet('{TEAM_BAT_PARQ}')
            WHERE match_type = ? AND event_name = ? AND batting_team = ?""",
        (match_type, event, team),
    )
    return rows(cur)

@app.get("/team/bowling")
def team_bowl(match_type: str, event: str, team: str):
    cur = con.execute(
        f"""SELECT * FROM read_parquet('{TEAM_BOWL_PARQ}')
            WHERE match_type = ? AND event_name = ? AND fielding_team = ?""",
        (match_type, event, team),
    )
    return rows(cur)
