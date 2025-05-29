# ui/app.py  (keep only what’s inside this box!)

import streamlit as st, requests, pandas as pd

API_BASE = "https://cricket-stats-7ma4.onrender.com"

st.set_page_config(page_title="Cricket Betting Stats", layout="wide")
st.title("🔍 Player Quick Lookup")

with st.sidebar:
    st.header("Filters")
    name   = st.text_input("Player contains…", "buttler")
    fmt    = st.selectbox("Match type / format", ["T20", "ODI", "Test"])
    event  = st.text_input("Tournament contains… (optional)", "Blast")
    season = st.number_input("Season (optional)", 2000, 2100, value=2023)
    limit  = st.slider("Max rows", 10, 500, 100)

if st.button("Fetch"):
    endpoint = f"{API_BASE}/player/{name}"
    params   = {"match_type": fmt, "limit": limit}
    if event:  params["event"]  = event
    if season: params["season"] = int(season)

    try:
        r = requests.get(endpoint, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data:
            st.success(f"{len(data)} rows")
            st.dataframe(pd.DataFrame(data))
        else:
            st.warning("No rows returned – adjust filters.")
    except Exception as e:
        st.error(f"API error: {e}")
