import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Connecting Queens: Equity in Transit & Access",
    layout="wide"
)

st.title("Connecting Queens: Equity in Transit & Access")

# TODO: replace all placeholder DataFrames with BigQuery client queries
# Credentials strategy: deferred - see README Limitations

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "Transit Overview",
    "Bus Activity Analysis",
    "Clustering Results",
    "Proximity Analysis",
    "Equity Overlay",
    "Recommendations"
])

# -----------------------------------------------------------------------------
# Tab 1 — Transit Overview
# -----------------------------------------------------------------------------
with tab1:
    st.header("Transit Overview")

    quality_tiers = ["All", "High", "Medium", "Low"]
    selected_tier = st.selectbox("Service Quality Tier", quality_tiers)

    # TODO: replace with BigQuery query against vehicle_positions
    placeholder_overview = pd.DataFrame({
        "route_id": ["MTA NYCT_Q44+", "MTA NYCT_Q46", "MTABC_Q06"],
        "vehicle_count": [12, 8, 5],
        "service_quality_tier": ["High", "Medium", "Low"]
    })

    st.dataframe(placeholder_overview)

# -----------------------------------------------------------------------------
# Tab 2 — Bus Activity Analysis
# -----------------------------------------------------------------------------
with tab2:
    st.header("Bus Activity Analysis")

    # TODO: replace with route list pulled from BigQuery routes table
    placeholder_routes = ["MTA NYCT_Q44+", "MTA NYCT_Q46", "MTABC_Q06"]
    selected_route = st.selectbox("Route", placeholder_routes)

    # TODO: replace with BigQuery query filtered by selected_route
    placeholder_activity = pd.DataFrame({
        "collected_at": pd.date_range("2026-04-23", periods=5, freq="15min"),
        "vehicle_count": [10, 12, 9, 11, 13]
    })

    st.line_chart(placeholder_activity.set_index("collected_at"))

# -----------------------------------------------------------------------------
# Tab 3 — Clustering Results
# -----------------------------------------------------------------------------
with tab3:
    st.header("Clustering Results")

    quality_tiers_c = ["All", "High", "Medium", "Low"]
    selected_tier_c = st.selectbox("Service quality tier", quality_tiers_c, key="clustering_tier")

    # Leiden vs. Louvain comparison — static methodology note
    st.info(
        "Ran community detection using both Leiden and Louvain algorithms. "
        "See the methodology section for modularity scores and coverage rates for both methods. "
        "The primary clustering layer uses Leiden results where coverage is meaningfully higher."
    )

    # TODO: replace with BigQuery query against clustering results table
    placeholder_clusters = pd.DataFrame({
        "cluster_id": [1, 2, 3],
        "stop_count": [45, 30, 22],
        "service_quality_tier": ["Low", "Medium", "High"]
    })

    st.dataframe(placeholder_clusters)

# -----------------------------------------------------------------------------
# Tab 4 — Proximity Analysis
# -----------------------------------------------------------------------------
with tab4:
    st.header("Proximity Analysis")

    es_types = ["All", "Subway", "Schools", "FQHCs", "Libraries", "Parks", "Food Access"]
    selected_es = st.selectbox("Essential Service Type", es_types)

    # TODO: replace with BigQuery query against proximity_scores table
    placeholder_proximity = pd.DataFrame({
        "stop_id": ["101", "102", "103"],
        "subway_count": [2, 0, 1],
        "school_count": [1, 3, 0],
        "composite_score": [0.8, 0.4, 0.6]
    })

    st.dataframe(placeholder_proximity)

# -----------------------------------------------------------------------------
# Tab 5 — Equity Overlay
# -----------------------------------------------------------------------------
with tab5:
    st.header("Equity Overlay")

    score_threshold = st.slider(
        "Composite proximity score threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05
    )

    st.caption(
        "Base layer: transit-dependent census tracts (always visible). "
        "Stop markers colored by composite proximity score. "
        "Tighten the threshold to highlight stops that are both low-access "
        "and inside transit-dependent tracts."
    )

    # TODO: replace with Folium map wired to BigQuery — census_tracts + proximity_scores
    st.info("Map will render here once equity and proximity layers are populated (S7).")

# -----------------------------------------------------------------------------
# Tab 6 — Recommendations
# -----------------------------------------------------------------------------
with tab6:
    st.header("Recommendations")

    rec_categories = [
        "All",
        "1 — Frequency improvements",
        "2 — Coverage gaps",
        "3 — Network redesign evaluation",
        "4 — Essential service access prioritization",
        "5 — Inter-borough connectivity gaps"
    ]
    selected_category = st.selectbox("Recommendation category", rec_categories)

    # TODO: replace with recommendation results derived from analysis layers
    placeholder_recs = pd.DataFrame({
        "category": [1, 2, 3],
        "recommendation": [
            "Placeholder: increase frequency on route X",
            "Placeholder: extend route Y",
            "Placeholder: redesign corridor Z"
        ]
    })

    st.dataframe(placeholder_recs)