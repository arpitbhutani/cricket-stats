##############################################################################
# Cricket Stats • Production-ready API
#  - lists/{formats,events,teams,players}
#  - batting, bowling, teams, matchup
##############################################################################
from typing import Optional, List, Dict, Any, Tuple
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import duckdb, os, math, datetime, re

# ----------------------------------------------------------------------------
PARQ_DIR = "api/parquet"
BALLS    = os.path.join(PARQ_DIR, "cricket_balls.parquet")
TEAM_BAT = os.path.join(PARQ_DIR, "team_phase_summary.parquet")
TEAM_BWL = os.path.join(PARQ_DIR, "team_bowling_phase_summary.parquet")

db = duckdb.connect(database=":memory:")

app = FastAPI(title="Cricket Stats – Production")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"])

FORMATS = ("T20", "ODI", "Test")

# ----------------------------------------------------------------— helpers --
def ok(v: Any) -> Any:
    return None if isinstance(v, float) and not math.isfinite(v) else v

def rows(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    data = [{c: ok(v) for c, v in zip(cols, r)} for r in cur.fetchall()]
    if not data:
        raise HTTPException(404, "No rows")
    return data

def w(col: str, val: Optional[str]) -> str:
    return f"{col} ILIKE '%' || ? || '%'" if val else f"{col} IS NOT NULL"

def season(last: int) -> str:
    return "TRUE" if last <= 0 else f"season >= {datetime.date.today().year - last}"

# ========================================================================== #
# LIST ENDPOINTS
# ========================================================================== #
@app.get("/lists/formats")
def list_formats(): return FORMATS

@app.get("/lists/events")
def list_events(fmt: str):
    if fmt not in FORMATS: raise HTTPException(400, "bad format")
    cur = db.execute(
        f"SELECT DISTINCT event_name AS name "
        f"FROM read_parquet('{BALLS}') "
        "WHERE match_type = ? ORDER BY 1",
        (fmt,),
    )
    return rows(cur)

@app.get("/lists/teams")
def list_teams(fmt: str, event: str = ""):
    cur = db.execute(
        f"SELECT DISTINCT batting_team AS name "
        f"FROM read_parquet('{BALLS}') "
        "WHERE match_type = ? AND " + w("event_name", event) +
        " ORDER BY 1",
        (fmt, *(event,) if event else ()),
    )
    return rows(cur)

@app.get("/lists/players")
def list_players(team: str):
    cur = db.execute(
        f"SELECT DISTINCT batter AS name "
        f"FROM read_parquet('{BALLS}') "
        "WHERE batting_team = ? ORDER BY 1",
        (team,),
    )
    return rows(cur)

# ========================================================================== #
# BATTING SUMMARY + DRILL-DOWN
# ========================================================================== #
@app.get("/batting")
def batting(
    fmt: str,
    last: int = 3,
    min_inns: int = 3,
    event: str = "",
    team: str = "",
    opp: str = "",
    venue: str = "",
    innings: Optional[int] = None,
    players: str = "",          # CSV, substring match
):
    plist = [p.strip() for p in players.split(",") if p.strip()]
    sql = f"""
    WITH f AS (
      SELECT *
      FROM read_parquet('{BALLS}')
      WHERE match_type = ?
        AND {w('event_name',   event)}
        AND {w('batting_team', team)}
        AND {w('bowling_team', opp)}
        AND {w('venue',        venue)}
        AND {w('batter', '|'.join(plist)) if plist else 'TRUE'}
        AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
        AND ({season(last)})
    )
    SELECT
      batter,
      COUNT(DISTINCT match_id) inns,
      SUM(runs_batter)         runs,
      SUM(CASE WHEN wicket_type IS NOT NULL
                AND player_out = batter THEN 1 END) outs,
      COUNT(*)                 balls,
      SUM(is_boundary_4::INT)  fours,
      SUM(is_boundary_6::INT)  sixes
    FROM f
    GROUP BY batter
    HAVING inns >= ?
    ORDER BY runs DESC
    """
    params: List[Any] = [fmt]
    params += [event] if event else []
    params += [team] if team else []
    params += [opp] if opp else []
    params += [venue] if venue else []
    if plist: params.append("|".join(plist))
    params.append(min_inns)
    data = rows(db.execute(sql, tuple(params)))
    for r in data:
        r["avg"] = ok(round(r["runs"] / r["outs"], 2) if r["outs"] else None)
        r["sr"]  = round(100 * r["runs"] / r["balls"], 2)
        r["bp4"] = ok(round(r["balls"] / r["fours"], 1) if r["fours"] else None)
        r["bp6"] = ok(round(r["balls"] / r["sixes"], 1) if r["sixes"] else None)
        r["%4s"] = ok(round(100 * r["fours"] / r["balls"], 2))
        r["%6s"] = ok(round(100 * r["sixes"] / r["balls"], 2))
    return data

@app.get("/batting/drill")
def batting_drill(fmt: str, batter: str, **base_filters):
    """Per-match breakdown for a single batter (inherits same filters)."""
    base_filters["players"] = batter
    rows_all = batting(fmt=fmt, **base_filters)
    if not rows_all: raise HTTPException(404, "no such player in filter set")

    cur = db.execute(
        f"""SELECT match_id, bowling_team AS opponent, venue,
                   SUM(runs_batter) AS runs,
                   COUNT(*)         AS balls,
                   SUM(is_boundary_4::INT) fours,
                   SUM(is_boundary_6::INT) sixes
            FROM read_parquet('{BALLS}')
            WHERE match_type = ?
              AND batter ILIKE ?
              AND ({season(base_filters.get('last', 3))})
            GROUP BY match_id, bowling_team, venue
            ORDER BY match_id""",
        (fmt, f"%{batter}%"),
    )
    return rows(cur)

# ========================================================================== #
# BOWLING SUMMARY
# ========================================================================== #
@app.get("/bowling")
def bowling(fmt: str, last: int = 3, min_inns: int = 3,
            event: str = "", team: str = "", opp: str = "",
            venue: str = "", innings: Optional[int] = None,
            bowlers: str = ""):
    blist = [b.strip() for b in bowlers.split(",") if b.strip()]
    sql = f"""
    WITH f AS (
      SELECT *
      FROM read_parquet('{BALLS}')
      WHERE match_type = ?
        AND {w('event_name',    event)}
        AND {w('bowling_team',  team)}
        AND {w('batting_team',  opp)}
        AND {w('venue',         venue)}
        AND {w('bowler', '|'.join(blist)) if blist else 'TRUE'}
        AND ({'innings_number = ' + str(innings) if innings else 'TRUE'})
        AND ({season(last)})
    ),
    m AS (
      SELECT match_id, bowler,
             COUNT(*)        balls,
             SUM(runs_total) runs,
             SUM(CASE WHEN wicket_type IS NOT NULL
                      AND bowler = ANY(string_split(player_out,','))
                      THEN 1 END) wkts
      FROM f GROUP BY 1,2
    )
    SELECT bowler, COUNT(*) inns, SUM(balls) balls,
           SUM(runs) runs, SUM(wkts) wkts
    FROM m
    GROUP BY bowler HAVING inns >= ?
    ORDER BY wkts DESC
    """
    p=[fmt]; p+=[event] if event else []
    p+=[team] if team else []; p+=[opp] if opp else []
    p+=[venue] if venue else []; p+=[ "|".join(blist) ] if blist else []
    p.append(min_inns)
    data = rows(db.execute(sql, tuple(p)))
    for r in data:
        overs = r["balls"] / 6
        r["econ"] = ok(round(r["runs"] / overs, 2))
        r["sr"]   = ok(round(r["balls"] / r["wkts"], 1) if r["wkts"] else None)
        r["avg"]  = ok(round(r["runs"] / r["wkts"], 2) if r["wkts"] else None)
    return data

# ========================================================================== #
# TEAM PHASE SUMMARY
# ========================================================================== #
@app.get("/team")
def team(fmt: str, event: str, team: str):
    bat = rows(db.execute(
        f"SELECT * FROM read_parquet('{TEAM_BAT}') "
        "WHERE match_type=? AND event_name=? AND batting_team=?",
        (fmt, event, team)))
    bowl = rows(db.execute(
        f"SELECT * FROM read_parquet('{TEAM_BWL}') "
        "WHERE match_type=? AND event_name=? AND fielding_team=?",
        (fmt, event, team)))
    return {"batting": bat, "bowling": bowl}

# ========================================================================== #
# MATCH-UPS (batter vs bowler quick table)
# ========================================================================== #
@app.get("/matchup")
def matchup(fmt: str, batter: str, bowler: str, last: int = 3):
    cur = db.execute(
        f"""SELECT
              COUNT(*) balls,
              SUM(runs_batter) runs,
              SUM(CASE WHEN wicket_type IS NOT NULL
                       AND player_out = ? THEN 1 END) dismissals
            FROM read_parquet('{BALLS}')
            WHERE match_type=? AND batter ILIKE ? AND bowler ILIKE ?
              AND ({season(last)})""",
        (batter, fmt, f"%{batter}%", f"%{bowler}%"),
    )
    return rows(cur)
