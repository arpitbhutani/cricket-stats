import streamlit as st, pandas as pd, requests, functools
API="https://cricpick.onrender.com" 
@functools.lru_cache(maxsize=512)
def j(url,**p):
    r=requests.get(f"{API}{url}",params=p,timeout=15)
    if r.status_code==404:return []; r.raise_for_status();return r.json()
fmt = st.sidebar.selectbox("Format",j("/lists/formats"))
ev  = st.sidebar.selectbox("Tournament",["<Any>"]+[e["name"] for e in j("/lists/events",fmt=fmt)])
tms = [t["name"] for t in j("/lists/teams",fmt=fmt,event=ev if ev!="<Any>" else "")]
team= st.sidebar.selectbox("Bowling team",["<Any>"]+tms)
opp = st.sidebar.selectbox("Opponent",["<Any>"]+tms)
ven = st.sidebar.text_input("Venue contains…")
inn = st.sidebar.selectbox("Innings",["Any",1,2])
yrs = st.sidebar.slider("Look-back years",0,10,3)
minn= st.sidebar.slider("Min inns",1,25,3)
bow = st.multiselect("Bowler(s)", j("/lists/players",team=team) if team!="<Any>" else [])
if st.sidebar.button("Fetch"):
    p=dict(fmt=fmt,event="" if ev=="<Any>" else ev,team="" if team=="<Any>" else team,
           opp="" if opp=="<Any>" else opp,venue=ven,innings=None if inn=="Any" else inn,
           last=yrs,min_inns=minn,bowlers=", ".join(bow))
    st.dataframe(pd.DataFrame(j("/bowling",**p)),use_container_width=True)
