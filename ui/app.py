import streamlit as st, requests, pandas as pd

API_BASE = "https://cricket-stats-7ma4.onrender.com"   # <- your Render URL

st.set_page_config(page_title="Cricket Betting Stats", layout="wide")
st.title("🔍 Player Quick Lookup")

# ── sidebar filters ──────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    name   = st.text_input("Player contains…", value="buttler")
    fmt    = st.selectbox("Match type / format", ["T20", "ODI", "Test"])
    event  = st.text_input("Tournament contains… (optional)", value="Blast")
    season = st.number_input("Season (optional)", min_value=2000, max_value=2100, step=1, value=2023)
    limit  = st.slider("Max rows", 10, 500, 100)

# ── query API when button clicked ────────────────────────────────────────
if st.button("Fetch"):
    endpoint = f"{API_BASE}/player/{name}"
    params   = {"match_type": fmt, "limit": limit}
    if event:  params["event"]  = event
    if season: params["season"] = season

    try:
        resp = requests.get(endpoint, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            st.warning("No rows returned – try relaxing the filters.")
        else:
            df = pd.DataFrame(data)
            st.success(f"{len(df)} rows")
            st.dataframe(df)                # interactive table
    except Exception as e:
        st.error(f"API error: {e}")
PY
