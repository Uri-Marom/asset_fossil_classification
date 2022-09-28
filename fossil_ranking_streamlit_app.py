import streamlit as st
import pandas as pd

st.write("""
# מדרג כסף נקי
הגרסה האינטראקטיבית
""")
st.beta_set_page_config(layout='wide')
ranking = pd.read_csv("data/midrag.csv")
st.write(ranking)

""" Why like this?
"""
