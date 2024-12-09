import streamlit as st
import pandas as pd
import time

DATA_PATH = "D:/streaming-portugal/data/aggregated_data.csv"

@st.cache_data
def load_data(path):
    return pd.read_csv(path)

st.title("Portugal Real Estate Streaming Dashboard")
placeholder = st.empty()

while True:
    try:
        data = load_data(DATA_PATH)
        with placeholder.container():
            st.header("Агрегированные метрики")
            st.metric("Всего объявлений", data['total_listings'].sum())
            st.metric("Средняя цена", f"{data['avg_price'].mean():,.2f}")
            st.metric("Средняя площадь", f"{data['avg_area'].mean():,.2f}")

            st.subheader("Средняя цена по типам")
            st.bar_chart(data.groupby("Type")['avg_price'].mean())

            st.subheader("Средняя площадь по районам")
            st.bar_chart(data.groupby("District")['avg_area'].mean())

        st.autorefresh(interval=30)
        time.sleep(30)
        
    except FileNotFoundError:
        st.error("Файл с данными пока не создан.")
        time.sleep(5)