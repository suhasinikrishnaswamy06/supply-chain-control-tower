from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.auth.exceptions import DefaultCredentialsError

from src.dashboard.bigquery_client import (
    get_credentials_help_text,
    get_dashboard_config,
    run_query,
    table_name,
)


st.set_page_config(
    page_title="Supply Chain Control Tower",
    page_icon=":bar_chart:",
    layout="wide",
)


@st.cache_data(ttl=300)
def load_executive_dashboard() -> pd.DataFrame:
    return run_query(
        f"""
        select *
        from {table_name("control_tower_executive_dashboard")}
        order by metric_date desc, warehouse_id
        """
    )


@st.cache_data(ttl=300)
def load_carrier_performance() -> pd.DataFrame:
    return run_query(
        f"""
        select *
        from {table_name("mart_carrier_performance_daily")}
        order by metric_date desc, carrier_name
        """
    )


@st.cache_data(ttl=300)
def load_sku_risk() -> pd.DataFrame:
    return run_query(
        f"""
        select *
        from {table_name("mart_sku_inventory_risk_daily")}
        order by metric_date desc, warehouse_id, sku_id
        """
    )


def pct(value: float) -> str:
    return f"{value:.1%}"


def render_overview(executive_df: pd.DataFrame) -> None:
    latest_date = executive_df["metric_date"].max()
    latest_df = executive_df[executive_df["metric_date"] == latest_date].copy()

    st.subheader("Executive Overview")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("OTIF Rate", pct(latest_df["otif_rate"].mean()))
    col2.metric("Backlog Units", f"{int(latest_df['backlog_units'].sum()):,}")
    col3.metric("Inventory At Risk", f"{int(latest_df['inventory_at_risk_units'].sum()):,}")
    col4.metric("Pick Delay Events", f"{int(latest_df['warehouse_pick_delay_events'].sum()):,}")

    st.caption(f"Latest metric date: {latest_date}")

    left, right = st.columns((3, 2))
    with left:
        st.markdown("**Warehouse Performance Snapshot**")
        warehouse_view = latest_df[
            [
                "warehouse_id",
                "warehouse_city",
                "otif_rate",
                "backlog_units",
                "inventory_risk_rate",
                "warehouse_pick_delay_events",
            ]
        ].sort_values(by=["backlog_units", "warehouse_pick_delay_events"], ascending=[False, False])
        st.dataframe(warehouse_view, use_container_width=True)
    with right:
        st.markdown("**Top Risk Warehouses**")
        risk_view = latest_df[
            ["warehouse_id", "inventory_at_risk_units", "backlog_units", "late_shipments"]
        ].sort_values(by=["inventory_at_risk_units", "backlog_units"], ascending=[False, False])
        st.dataframe(risk_view.head(5), use_container_width=True)


def render_warehouse_tab(executive_df: pd.DataFrame) -> None:
    latest_date = executive_df["metric_date"].max()
    latest_df = executive_df[executive_df["metric_date"] == latest_date].copy()
    selected_warehouse = st.selectbox("Warehouse", sorted(latest_df["warehouse_id"].unique()))
    warehouse_df = latest_df[latest_df["warehouse_id"] == selected_warehouse]

    st.markdown(f"**Warehouse Detail: {selected_warehouse}**")
    st.dataframe(warehouse_df, use_container_width=True)


def render_carrier_tab(carrier_df: pd.DataFrame) -> None:
    latest_date = carrier_df["metric_date"].max()
    latest_df = carrier_df[carrier_df["metric_date"] == latest_date].copy()

    st.markdown("**Carrier Performance**")
    st.dataframe(
        latest_df[
            [
                "carrier_name",
                "carrier_mode",
                "carrier_segment",
                "shipment_records",
                "delivered_shipments",
                "late_shipments",
                "on_time_rate",
            ]
        ].sort_values(by=["late_shipments", "shipment_records"], ascending=[False, False]),
        use_container_width=True,
    )


def render_sku_tab(sku_df: pd.DataFrame) -> None:
    latest_date = sku_df["metric_date"].max()
    latest_df = sku_df[sku_df["metric_date"] == latest_date].copy()
    warehouses = ["All"] + sorted(latest_df["warehouse_id"].unique())
    selected_warehouse = st.selectbox("Warehouse Filter", warehouses, key="sku_warehouse")

    if selected_warehouse != "All":
        latest_df = latest_df[latest_df["warehouse_id"] == selected_warehouse]

    st.markdown("**SKU Inventory Risk**")
    st.dataframe(
        latest_df[
            [
                "warehouse_id",
                "sku_id",
                "sku_name",
                "product_family",
                "on_hand_qty",
                "reorder_point",
                "demand_gap_units",
                "is_inventory_at_risk",
                "stock_cover_ratio",
            ]
        ].sort_values(by=["is_inventory_at_risk", "demand_gap_units"], ascending=[False, False]),
        use_container_width=True,
    )


def main() -> None:
    st.title("Supply Chain Control Tower")
    st.caption("Operational KPI dashboard powered by BigQuery marts and dbt models.")

    config = get_dashboard_config()
    with st.sidebar:
        st.markdown("### Dashboard Config")
        st.write(f"Project: `{config['project']}`")
        st.write(f"Dataset: `{config['dataset']}`")
        st.write(f"Credentials: `{config['credentials_path'] or 'Not set'}`")

    try:
        executive_df = load_executive_dashboard()
        carrier_df = load_carrier_performance()
        sku_df = load_sku_risk()
    except DefaultCredentialsError:
        st.error("BigQuery credentials are not configured for the dashboard.")
        st.info(get_credentials_help_text())
        st.code(
            "Copy-Item .env.streamlit.example .env.streamlit",
            language="powershell",
        )
        st.code(
            "streamlit run src/dashboard/streamlit_app.py",
            language="powershell",
        )
        return
    except Exception as exc:
        st.error("Unable to load dashboard data from BigQuery.")
        st.exception(exc)
        st.info(
            "Make sure your BigQuery credentials are configured and the curated models exist in the configured dataset."
        )
        return

    render_overview(executive_df)

    overview_tab, warehouse_tab, carrier_tab, sku_tab = st.tabs(
        ["Overview", "Warehouse", "Carrier", "SKU Risk"]
    )

    with overview_tab:
        st.dataframe(executive_df.sort_values(by=["metric_date", "warehouse_id"], ascending=[False, True]), use_container_width=True)
    with warehouse_tab:
        render_warehouse_tab(executive_df)
    with carrier_tab:
        render_carrier_tab(carrier_df)
    with sku_tab:
        render_sku_tab(sku_df)

    st.markdown("### Run Notes")
    st.write(
        "This dashboard reads directly from the curated BigQuery marts built by dbt. "
        "Refresh the Airflow pipeline before reviewing metrics if you want the latest sample data."
    )
    st.code(
        "streamlit run src/dashboard/streamlit_app.py",
        language="powershell",
    )


if __name__ == "__main__":
    main()
