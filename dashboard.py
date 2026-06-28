"""Interactive Streamlit dashboard over the Elasticsearch index.

    streamlit run dashboard.py

A search-and-analytics UI on top of the indexed events: a full-text search box,
an error-rate-by-service table, and an events-by-region chart.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

import config
from src.search import EventSearch

st.set_page_config(page_title="LogPulse", page_icon="🔎", layout="wide")
st.title("LogPulse — event search & analytics")
st.caption("Full-text search and aggregations over the Elasticsearch index.")

try:
    search = EventSearch(config.ES_HOSTS, config.ES_INDEX)
    if not search.client.ping():
        raise ConnectionError
except Exception:
    st.error("Elasticsearch not reachable. Start the stack with `docker compose up -d` "
             "and ingest with `python run_pipeline.py` or the Kafka producer/consumer.")
    st.stop()

st.subheader("Full-text search")
query = st.text_input("Search event messages", value="timeout")
if query:
    hits = search.full_text(query, size=20)
    if hits:
        st.dataframe(
            pd.DataFrame(hits)[["timestamp", "service", "level", "status_code", "message"]],
            use_container_width=True, hide_index=True,
        )
    else:
        st.info(f"No events match '{query}'.")

left, right = st.columns(2)
with left:
    st.subheader("Error rate by service")
    df = pd.DataFrame(search.error_rate_by_service())
    if not df.empty:
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.bar_chart(df.set_index("service")["error_rate_pct"])
with right:
    st.subheader("Events by region")
    rdf = pd.DataFrame(search.events_by_region())
    if not rdf.empty:
        st.bar_chart(rdf.set_index("region")["events"])
