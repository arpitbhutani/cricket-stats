import streamlit as st, pandas as pd, requests
API="https://cricpick.onrender.com" 
fmt=st.sidebar.selectbox("Format",["T20","ODI","Test"])
bat=st.sidebar.text_input("Batter")
bowl=st.sidebar.text_input("Bowler")
yrs=st.sidebar.slider("Last N years",1,10,3)
if st.sidebar.button("Fetch"):
    r=requests.get(f"{API}/matchup",params={"fmt":fmt,"batter":bat,"bowler":bowl,"last":yrs})
    if r.status_code==404: st.warning("No rows"); st.stop()
    st.dataframe(pd.DataFrame(r.json()),use_container_width=True)
