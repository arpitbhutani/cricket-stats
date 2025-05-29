import streamlit as st, pandas as pd, requests, functools

API = "https://cricpick.onrender.com"  # ← your Render backend URL

st.set_page_config("Cricket Stats Explorer", layout="wide")
st.title("🏏  Cricket Stats Explorer — v2")

# ── cached GET --------------------------------------------------------------
@functools.lru_cache(maxsize=256)
def _get(endpoint: str, **params):
    r = requests.get(f"{API}{endpoint}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def lookup(kind: str, q: str = "", **extra) -> list[str]:
    if len(q) < 2 and q != "":
        return []                     # require 2+ chars unless empty
    try:
        res = _get(f"/search/{kind}", query=q, **extra, limit=20)
        return [row["name"] for row in res]
    except Exception:
        return []

def fetch_df(path: str, params: dict) -> pd.DataFrame:
    try:
        r = requests.get(f"{API}{path}", params=params, timeout=30)
        if r.status_code == 404:
            return pd.DataFrame()
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        st.error(f"API error: {e}")
        return pd.DataFrame()

# ── sidebar filters ---------------------------------------------------------
with st.sidebar:
    st.header("Filters")

    fmt  = st.selectbox("Format", ["T20", "ODI", "Test"])
    yrs  = st.slider("Look-back years", 0, 10, 3)
    min_inns = st.slider("Min matches/innings", 1, 25, 3)

    # Tournament
    ev_q = st.text_input("Tournament (type ≥2 chars)", "IPL")
    ev_opts = ["<Any>"] + lookup("events", ev_q, match_type=fmt)
    event = st.selectbox("Select tournament", ev_opts, index=0)

    # Team
    team_q = st.text_input("Team search", "")
    team_opts = ["<Any>"] + lookup(
        "teams", team_q,
        event=None if event == "<Any>" else event,
        match_type=fmt
    )
    team = st.selectbox("Team", team_opts, index=0)

    # Opponent
    opp_q = st.text_input("Opponent search", "")
    opp_opts = ["<Any>"] + lookup(
        "teams", opp_q,
        event=None if event == "<Any>" else event,
        match_type=fmt
    )
    opponent = st.selectbox("Opponent", opp_opts, index=0)

    # Venue
    ven_q = st.text_input("Venue search", "")
    venue_opts = ["<Any>"] + lookup("venues", ven_q)
    venue = st.selectbox("Venue", venue_opts, index=0)

    innings = st.selectbox("Innings", ["Any", 1, 2], index=0)

    # Batter
    bat_q = st.text_input("Batter search", "")
    bat_opts = ["<All>"] + lookup("players", bat_q)
    batter = st.selectbox("Batter", bat_opts, index=0)

    # Bowler
    bowl_q = st.text_input("Bowler search", "")
    bowl_opts = ["<All>"] + lookup("players", bowl_q)
    bowler = st.selectbox("Bowler", bowl_opts, index=0)

    run_button = st.button("Fetch")

# ── helper to assemble base params -----------------------------------------
def base_params() -> dict:
    p = {"match_type": fmt, "years": yrs, "min_inns": min_inns}
    if event != "<Any>": p["event"] = event
    if venue != "<Any>": p["venue"] = venue
    if innings != "Any": p["innings"] = innings
    return p

# ── tab layout --------------------------------------------------------------
bat_tab, bowl_tab, team_tab = st.tabs(["Batters", "Bowlers", "Teams"])

# ── Batters tab -------------------------------------------------------------
with bat_tab:
    if run_button:
        params = base_params() | {
            "team": None if team == "<Any>" else team,
            "opponent": None if opponent == "<Any>" else opponent,
            "batter": None if batter == "<All>" else batter,
        }
        df = fetch_df("/batting/summary", {k: v for k, v in params.items() if v})
        st.dataframe(df, use_container_width=True)

# ── Bowlers tab -------------------------------------------------------------
with bowl_tab:
    if run_button:
        params = base_params() | {
            "team": None if team == "<Any>" else team,
            "opponent": None if opponent == "<Any>" else opponent,
            "bowler": None if bowler == "<All>" else bowler,
        }
        df = fetch_df("/bowling/summary", {k: v for k, v in params.items() if v})
        st.dataframe(df, use_container_width=True)

# ── Teams tab ---------------------------------------------------------------
with team_tab:
    if run_button and event != "<Any>" and team != "<Any>":
        st.subheader("Batting phase")
        tb = fetch_df("/team/batting",
                      {"match_type": fmt, "event": event, "team": team})
        st.dataframe(tb, use_container_width=True)

        st.subheader("Bowling phase")
        tl = fetch_df("/team/bowling",
                      {"match_type": fmt, "event": event, "team": team})
        st.dataframe(tl, use_container_width=True)
    elif run_button:
        st.info("Select a specific Tournament **and** Team to see team stats.")
