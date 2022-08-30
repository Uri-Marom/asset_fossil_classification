import streamlit as st
import pandas as pd

st.write("""
# מדרג כסף נקי
הגרסה האינטראקטיבית
""")

ranking = pd.read_csv("data/midrag.csv")
st.write(ranking)
