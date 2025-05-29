import streamlit as st, requests, pandas as pd

API = "https://cricpick.onrender.com"   # <— your Render URL

st.set_page_config(page_title="Cricket Stats Explorer", layout="wide")
st.title("🏏 Cricket Stats Explorer")

# ── util --------------------------------------------------------------------
def fetch(path: str, params: dict) -> list[dict]:
    r = requests.get(f"{API}{path}", params=params, timeout=30)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()

def search(endpoint: str, q: str, extra=None):
    extra = extra or {}
    return [row["name"] for row in fetch(endpoint, {"query": q, **extra, "limit": 20})]

# ── sidebar filters ---------------------------------------------------------
with st.sidebar:
    st.header("Filters")

    fmt = st.selectbox("Format", ["T20", "ODI", "Test"])

    ev_q = st.text_input("Tournament contains…", "Blast")
    event_opts = ["<Any>"] + search("/search/events", ev_q, {"match_type": fmt})
    event = st.selectbox("Tournament", event_opts, index=0)

    team_q = st.text_input("Team contains…", "")
    team_opts = ["<Any>"] + search("/search/teams", team_q)
    team = st.selectbox("Team", team_opts, index=0)

    opp_q = st.text_input("Opponent contains…", "")
    opp_opts = ["<Any>"] + search("/search/teams", opp_q)
    opponent = st.selectbox("Opponent", opp_opts, index=0)

    venue_q = st.text_input("Venue contains…", "")
    venue_opts = ["<Any>"] + search("/search/venues", venue_q)
    venue = st.selectbox("Venue", venue_opts, index=0)

    innings = st.selectbox("Innings", ["Any", 1, 2], index=0)

    batter_q = st.text_input("Batter contains…", "")
    batter_opts = ["<All>"] + search("/search/players", batter_q)
    batter = st.selectbox("Select batter", batter_opts, index=0)

    bowler_q = st.text_input("Bowler contains…", "")
    bowler_opts = ["<All>"] + search("/search/players", bowler_q)
    bowler = st.selectbox("Select bowler", bowler_opts, index=0)

    run_thr = st.number_input("runs ≥ (batter)", 1, 200, 30)
    wkt_thr = st.number_input("wickets ≥ (bowler)", 1, 10, 3)

    fetch_btn = st.button("Fetch")

# ═════════════════ TABS ═════════════════
bat_tab, bowl_tab, team_tab = st.tabs(["Batters", "Bowlers", "Teams"])

def base_params():
    p = {"match_type": fmt}
    if event != "<Any>": p["event"] = event
    if venue != "<Any>": p["venue"] = venue
    if innings != "Any": p["innings"] = innings
    return p

# ── Batters -----------------------------------------------------------------
with bat_tab:
    if fetch_btn:
        p = base_params() | {
            "batting_team": None if team=="<Any>" else team,
            "opponent": None if opponent=="<Any>" else opponent,
            "batter": None if batter=="<All>" else batter,
            "run_threshold": run_thr,
        }
        df = pd.DataFrame(fetch("/batting/summary", {k:v for k,v in p.items() if v}))
        st.dataframe(df)

# ── Bowlers -----------------------------------------------------------------
with bowl_tab:
    if fetch_btn:
        p = base_params() | {
            "bowling_team": None if team=="<Any>" else team,
            "opponent": None if opponent=="<Any>" else opponent,
            "bowler": None if bowler=="<All>" else bowler,
            "wkt_threshold": wkt_thr,
        }
        df = pd.DataFrame(fetch("/bowling/summary", {k:v for k,v in p.items() if v}))
        st.dataframe(df)

# ── Teams -------------------------------------------------------------------
with team_tab:
    if fetch_btn and event != "<Any>" and team != "<Any>":
        st.subheader("Batting phase")
        tb = pd.DataFrame(fetch("/team/batting",
                                {"match_type": fmt, "event": event, "team": team}))
        st.dataframe(tb)

        st.subheader("Bowling phase")
        tbl = pd.DataFrame(fetch("/team/bowling",
                                 {"match_type": fmt, "event": event, "team": team}))
        st.dataframe(tbl)
    elif fetch_btn:
        st.info("Select a specific Tournament AND Team to view team summaries.")
