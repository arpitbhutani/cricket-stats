import time, requests, functools, streamlit as st, pandas as pd

API = "https://cricpick.onrender.com"   # ← your backend URL

@functools.lru_cache(maxsize=512)
def jget(endpoint: str, _tries: int = 3, **params):
    """GET with up-to-3 retries; show 'Backend waking up…' instead of 502."""
    url = f"{API}{endpoint}"
    for i in range(_tries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 502:
                time.sleep(2)
                continue
            if r.status_code == 404:
                return []
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if i == _tries - 1:
                st.error(f"API error {e}")
                return []
            time.sleep(2)

def formats(): return jget("/lists/formats")
def events(fmt): return [d["name"] for d in jget("/lists/events", fmt=fmt)]
def teams(fmt, ev=""): return [d["name"] for d in jget("/lists/teams", fmt=fmt, event=ev)]
def players(team): return [d["name"] for d in jget("/lists/players", team=team)]

# ---------- sidebar ---------------------------------------------------------
with st.sidebar:
    fmt = st.selectbox("Format", formats())
    ev  = st.selectbox("Tournament", ["<Any>"] + events(fmt))
    tms = teams(fmt, ev if ev != "<Any>" else "")
    team = st.selectbox("Team", ["<Any>"] + tms)
    opp  = st.selectbox("Opponent", ["<Any>"] + tms)
    ven  = st.text_input("Venue contains…")
    inns = st.selectbox("Innings", ["Any", 1, 2])
    yrs  = st.slider("Look-back years", 0, 10, 3)
    mmin = st.slider("Min innings", 1, 25, 3)

    st.markdown("#### Batter(s)")

    if team != "<Any>":
        # A team is selected → list only that squad
        bats = st.multiselect(
            "Select from team list",
            players(team),
            placeholder="Choose one or more batters"
        )
    else:
        # No team selected → global player search (2+ characters)
        search_q = st.text_input("Quick search (type ≥2 chars)", "")
        search_opts = (
            [p["name"] for p in jget("/search/players", query=search_q, limit=20)]
            if len(search_q) >= 2 else []
        )
        bats = st.multiselect(
            "Matches",
            search_opts,
            placeholder="Start typing a name"
        )

    top_x = st.slider("Top X", 5, 100, 25)
    run  = st.button("Fetch")

# ---------- main page -------------------------------------------------------
st.title("📊  Batters")

if not run:
    st.stop()

params = {
    "fmt": fmt,
    "event": "" if ev == "<Any>" else ev,
    "team": "" if team == "<Any>" else team,
    "opp": "" if opp == "<Any>" else opp,
    "venue": ven,
    "innings": None if inns == "Any" else inns,
    "last": yrs,
    "min_inns": mmin,
    "players": ", ".join(bats),
}

df = pd.DataFrame(jget("/batting", **params))
if df.empty:
    st.warning("No rows matched.")
    st.stop()

st.dataframe(df, use_container_width=True)

# ─── Drill-down per-match table ─────────────────────────────────────────────
sel = st.selectbox("Drill-down player", df["batter"])
if sel:
    # only send the fmt, batter, and look-back years
    drill_params = {
        "fmt": fmt,
        "batter": sel,
        "last": yrs
    }
    drill_data = jget("/batting/drill", **drill_params)
    if not drill_data:
        st.info("No match-level rows for that player.")
    else:
        drill_df = pd.DataFrame(drill_data)
        st.subheader(f"{sel} – per match breakdown")
        st.dataframe(drill_df, hide_index=True, use_container_width=True)

