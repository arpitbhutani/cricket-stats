import streamlit as st
import pandas as pd
import requests
import time
import functools

API="https://cricpick.onrender.com" 

# ── robust GET w/ up to 3 retries (handles cold starts) ─────────────────────
@functools.lru_cache(maxsize=256)
def jget(endpoint: str, _tries: int = 3, **params):
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
        except requests.RequestException:
            if i == _tries - 1:
                return []
            time.sleep(2)

# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Match-ups")

    # Format
    fmt_opts = jget("/lists/formats") or []
    fmt = st.selectbox("Format", fmt_opts)

    # Tournament
    ev_opts = ["<Any>"] + [e["name"] for e in jget("/lists/events", fmt=fmt) or []]
    ev = st.selectbox("Tournament", ev_opts)

    # Opponent Team
    tm_opts = [t["name"] for t in jget(
        "/lists/teams",
        fmt=fmt,
        event=(ev if ev != "<Any>" else "")
    ) or []]
    opp = st.selectbox("Opponent Team", ["<Any>"] + tm_opts)

    # Batter search (type-ahead)
    bat_q = st.text_input("Batter search (≥2 chars)", "")
    bat_opts = (
        [p["name"] for p in jget("/search/players", query=bat_q, limit=20) or []]
        if len(bat_q) >= 2 else []
    )
    batter = st.selectbox("Select Batter", ["<None>"] + bat_opts)

    # Look-back years
    last = st.slider("Look-back years", 0, 10, 3)

    run = st.button("Fetch")

# ── Main ───────────────────────────────────────────────────────────────────
st.title("⚔️ Batter vs Bowlers")

if not run:
    st.info("Configure filters and click Fetch.")
    st.stop()

# Build params, dropping placeholders
params = {
    "fmt": fmt,
    "batter": "" if batter in ("<None>", None) else batter,
    "opp": "" if opp in ("<Any>", None) else opp,
    "last": last,
}

data = jget("/matchup", **params)
df = pd.DataFrame(data)

if df.empty:
    st.warning("No bowlers found for those filters.")
else:
    st.dataframe(df, use_container_width=True)
