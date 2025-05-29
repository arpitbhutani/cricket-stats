import streamlit as st, pandas as pd, requests, functools
API="https://cricpick.onrender.com" 
@functools.lru_cache(maxsize=256)
def get(u,**p): r=requests.get(f"{API}{u}",params=p,timeout=15); r.raise_for_status(); return r.json()
fmt  = st.sidebar.selectbox("Format",get("/lists/formats"))
ev   = st.sidebar.selectbox("Tournament",[e["name"] for e in get("/lists/events",fmt=fmt)])
team = st.sidebar.selectbox("Team",[t["name"] for t in get("/lists/teams",fmt=fmt,event=ev)])
if st.sidebar.button("Fetch"):
    data=get("/team",fmt=fmt,event=ev,team=team)
    st.subheader("Batting phase"); st.dataframe(pd.DataFrame(data["batting"]),use_container_width=True)
    st.subheader("Bowling phase"); st.dataframe(pd.DataFrame(data["bowling"]),use_container_width=True)
