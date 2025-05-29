import streamlit as st, pandas as pd, requests

API = "https://cricpick.onrender.com"   # your Render backend

st.set_page_config("Cricket Stats Explorer", layout="wide")
st.title("🏏 Cricket Stats Explorer")

# ── helpers ----------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def lookup(kind:str, q:str="", **extra)->list[str]:
    try:
        r = requests.get(f"{API}/search/{kind}", params={"query":q,**extra}, timeout=10)
        return [d["name"] for d in r.json()]
    except Exception: return []

def fetch(path:str, p:dict)->pd.DataFrame:
    try:
        r=requests.get(f"{API}{path}",params=p,timeout=30)
        if r.status_code==404: return pd.DataFrame()
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        st.error(f"API error: {e}")
        return pd.DataFrame()

# ── SIDEBAR ----------------------------------------------------------------
with st.sidebar:
    st.header("Filters")
    fmt=st.selectbox("Format",["T20","ODI","Test"])
    yrs=st.slider("Look-back years",0,10,3)
    min_inns=st.slider("Min matches",1,25,3)

    event=st.selectbox("Tournament",["<Any>"]+lookup("events",match_type=fmt))
    team =st.selectbox("Team",["<Any>"]+lookup("teams"))
    opp  =st.selectbox("Opponent",["<Any>"]+lookup("teams"))
    venue=st.selectbox("Venue",["<Any>"]+lookup("venues"))
    inns =st.selectbox("Innings",["Any",1,2])

    batter=st.selectbox("Batter",["<All>"]+lookup("players"))
    bowler=st.selectbox("Bowler",["<All>"]+lookup("players"))

    go=st.button("Fetch")

def base_params():
    p={"match_type":fmt,"years":yrs,"min_inns":min_inns}
    if event!="<Any>": p["event"]=event
    if venue!="<Any>": p["venue"]=venue
    if inns!="Any":   p["innings"]=inns
    return p

# ── TABS -------------------------------------------------------------------
bat,bowl,team_tab=st.tabs(["Batters","Bowlers","Teams"])

with bat:
    if go:
        p=base_params()|{
          "team":None if team=="<Any>" else team,
          "opponent":None if opp=="<Any>" else opp,
          "batter":None if batter=="<All>" else batter,
        }
        df=fetch("/batting/summary",p)
        st.write(df)

with bowl:
    if go:
        p=base_params()|{
          "team":None if team=="<Any>" else team,
          "opponent":None if opp=="<Any>" else opp,
          "bowler":None if bowler=="<All>" else bowler,
        }
        df=fetch("/bowling/summary",p)
        st.write(df)

with team_tab:
    if go and event!="<Any>" and team!="<Any>":
        st.subheader("Batting phase")
        st.write(fetch("/team/batting",{"match_type":fmt,"event":event,"team":team}))
        st.subheader("Bowling phase")
        st.write(fetch("/team/bowling",{"match_type":fmt,"event":event,"team":team}))
    elif go:
        st.info("Select a Tournament & Team for team view.")
