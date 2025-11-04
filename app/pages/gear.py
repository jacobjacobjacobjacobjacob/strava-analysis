# app/pages/gear.py
import streamlit as st
from app.utils import load_data

def run():
    st.title("📊 Stats Page")
    df = load_data()
    st.write("This page could show aggregated stats, charts, or trends.")
    st.dataframe(df.head())
