import streamlit as st, requests, pandas as pd

API = "https://cricket-stats-7ma4.onrender.com"   # your Render URL

st.set_page_config(page_title="Cricket Betting Stats", layout="wide")
st.title("🔍 Player Quick Lookup")

with st.sidebar:
    st.header("Filters")
    name  = st.text_input("Player contains…", "buttler")
    fmt   = st.selectbox("Format", ["T20", "ODI", "Test"])
    event = st.text_input("Tournament contains… (optional)", "Blast")
    seasons = ["Any"] + [str(y) for y in range(2000, 2026)]
    season_sel = st.selectbox("Season", seasons, index=0)
    limit = st.slider("Max rows", 10, 500, 100)

if st.button("Fetch"):
    endpoint = f"{API}/player/{name}"
    params   = {"match_type": fmt, "limit": limit}
    if event.strip():
        params["event"] = event
    if season_sel != "Any":
        params["season"] = int(season_sel)

    try:
        r = requests.get(endpoint, params=params, timeout=15)
        if r.status_code == 404:
            st.warning("No rows returned – adjust filters.")
        else:
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            st.success(f"{len(df)} rows")
            st.dataframe(df)
    except Exception as e:
        st.error(f"API error: {e}")
