import streamlit as st, pandas as pd, requests, functools
API="https://cricpick.onrender.com" 
# ── robust GET with retries ────────────────────────────────────────────────
@functools.lru_cache(maxsize=512)
def jget(endpoint: str, _tries: int = 3, **params):
    url = f"{API}{endpoint}"
    for i in range(_tries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 502:        # backend cold-start
                time.sleep(2); continue
            if r.status_code == 404:
                return []
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException:
            if i == _tries - 1:
                return []
            time.sleep(2)

# ---------- sidebar --------------------------------------------------------
fmt_options = jget("/lists/formats") or []
fmt = st.sidebar.selectbox("Format", fmt_options)

ev_options = ["<Any>"] + [e["name"] for e in jget("/lists/events", fmt=fmt) or []]
ev = st.sidebar.selectbox("Tournament", ev_options)

team_list = [t["name"] for t in jget(
    "/lists/teams",
    fmt=fmt,
    event=ev if ev != "<Any>" else ""
) or []]

team = st.sidebar.selectbox("Bowling team", ["<Any>"] + team_list)
opp  = st.sidebar.selectbox("Opponent", ["<Any>"] + team_list)
venue = st.sidebar.text_input("Venue contains…")
innings = st.sidebar.selectbox("Innings", ["Any", 1, 2])
yrs  = st.sidebar.slider("Look-back years", 0, 10, 3)
min_inns = st.sidebar.slider("Min inns", 1, 25, 3)

bow_opts = jget("/lists/players", team=team) if team != "<Any>" else []
bowlers = st.sidebar.multiselect("Bowler(s)", bow_opts)

if st.sidebar.button("Fetch"):
    params = dict(
        fmt=fmt,
        event="" if ev == "<Any>" else ev,
        team="" if team == "<Any>" else team,
        opp=""  if opp  == "<Any>" else opp,
        venue=venue,
        innings=None if innings == "Any" else innings,
        last=yrs,
        min_inns=min_inns,
        bowlers=", ".join(bowlers),
    )
    data = jget("/bowling", **params)
    if not data:
        st.warning("No rows returned.")
    else:
        st.dataframe(pd.DataFrame(data), use_container_width=True)
