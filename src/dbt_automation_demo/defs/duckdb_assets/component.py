"""Non-dbt assets that read/write DuckDB tables directly.

Upstream assets (product_catalog, web_events) produce tables that dbt views
consume via source(). The downstream asset (customer_scoring_report) reads
from a dbt view.
"""

from dataclasses import dataclass
from pathlib import Path

import dagster as dg
import duckdb


DUCKDB_PATH = str(Path(__file__).resolve().parents[4] / "analytics_dbt" / "dev.duckdb")


@dg.asset(key=["external", "product_catalog"])
def product_catalog() -> dg.MaterializeResult:
    """Write a product catalog table to DuckDB."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS external;
        CREATE OR REPLACE TABLE external.product_catalog AS
        SELECT * FROM VALUES
            (1, 'Widget A', 'electronics', 29.99),
            (2, 'Widget B', 'electronics', 49.99),
            (3, 'Gadget X', 'accessories', 15.00),
            (4, 'Gadget Y', 'accessories', 22.50),
            (5, 'Gizmo Z', 'home', 89.99),
            (6, 'Gizmo W', 'home', 119.99)
        AS t(product_id, name, category, price)
    """)
    count = con.execute("SELECT count(*) FROM external.product_catalog").fetchone()[0]
    con.close()
    return dg.MaterializeResult(metadata={"row_count": count})


@dg.asset(key=["external", "web_events"])
def web_events() -> dg.MaterializeResult:
    """Write web tracking events to DuckDB."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS external;
        CREATE OR REPLACE TABLE external.web_events AS
        SELECT * FROM VALUES
            (1, 1, 'page_view', '/products', '2024-06-01 10:00:00'),
            (2, 1, 'add_to_cart', '/products/1', '2024-06-01 10:05:00'),
            (3, 2, 'page_view', '/products', '2024-06-02 14:00:00'),
            (4, 2, 'purchase', '/checkout', '2024-06-02 14:30:00'),
            (5, 3, 'page_view', '/home', '2024-06-10 09:00:00'),
            (6, 4, 'page_view', '/products', '2024-06-15 11:00:00'),
            (7, 4, 'add_to_cart', '/products/5', '2024-06-15 11:10:00'),
            (8, 5, 'page_view', '/home', '2024-06-25 16:00:00'),
            (9, 5, 'purchase', '/checkout', '2024-06-25 16:45:00'),
            (10, 1, 'purchase', '/checkout', '2024-07-01 12:00:00')
        AS t(event_id, customer_id, event_type, page_url, event_timestamp)
    """)
    count = con.execute("SELECT count(*) FROM external.web_events").fetchone()[0]
    con.close()
    return dg.MaterializeResult(metadata={"row_count": count})


@dg.asset(key=["external", "customer_demographics"])
def customer_demographics() -> dg.MaterializeResult:
    """Write customer demographic data to DuckDB. Feeds into the full dbt view chain."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS external;
        CREATE OR REPLACE TABLE external.customer_demographics AS
        SELECT * FROM VALUES
            (1, 'US', 'CA', '25-34', 'male', 85000),
            (2, 'US', 'NY', '35-44', 'female', 120000),
            (3, 'UK', 'London', '25-34', 'male', 65000),
            (4, 'US', 'TX', '18-24', 'female', 45000),
            (5, 'CA', 'ON', '45-54', 'male', 95000)
        AS t(customer_id, country, region, age_group, gender, household_income)
    """)
    count = con.execute("SELECT count(*) FROM external.customer_demographics").fetchone()[0]
    con.close()
    return dg.MaterializeResult(metadata={"row_count": count})


@dg.asset(key=["external", "enriched_web_sessions"], deps=["stg_web_events"])
def enriched_web_sessions() -> dg.MaterializeResult:
    """Read from stg_web_events dbt view, sessionize events, write to DuckDB."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS external;
        CREATE OR REPLACE TABLE external.enriched_web_sessions AS
        SELECT
            customer_id,
            min(event_timestamp) as session_start,
            max(event_timestamp) as session_end,
            count(*) as event_count,
            count(case when event_type = 'page_view' then 1 end) as page_views,
            count(case when event_type = 'add_to_cart' then 1 end) as add_to_carts,
            count(case when event_type = 'purchase' then 1 end) as purchases,
            case when count(case when event_type = 'purchase' then 1 end) > 0
                then true else false
            end as converted
        FROM stg_web_events
        GROUP BY customer_id
    """)
    count = con.execute("SELECT count(*) FROM external.enriched_web_sessions").fetchone()[0]
    con.close()
    return dg.MaterializeResult(metadata={"row_count": count})


@dg.asset(deps=["int_customer_scoring"])
def customer_scoring_report() -> dg.MaterializeResult:
    """Read from the int_customer_scoring dbt view and write a summary report table."""
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE OR REPLACE TABLE customer_scoring_report AS
        SELECT
            clv_tier,
            count(*) as customer_count,
            round(avg(frequency_score), 2) as avg_frequency_score,
            round(avg(monetary_score), 2) as avg_monetary_score,
            round(avg(reliability_score), 2) as avg_reliability_score,
            round(avg(total_spent), 2) as avg_total_spent,
            sum(total_spent) as total_revenue
        FROM (
            SELECT
                *,
                CASE
                    WHEN (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 4.0 THEN 'platinum'
                    WHEN (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 3.0 THEN 'gold'
                    WHEN (frequency_score * 0.35 + monetary_score * 0.45 + reliability_score * 0.20) >= 2.0 THEN 'silver'
                    ELSE 'bronze'
                END as clv_tier
            FROM int_customer_scoring
        )
        GROUP BY clv_tier
        ORDER BY total_revenue DESC
    """)
    result = con.execute("SELECT * FROM customer_scoring_report").fetchall()
    con.close()
    return dg.MaterializeResult(
        metadata={"tier_count": len(result), "preview": str(result)}
    )


@dataclass
class DuckDbAssetsComponent(dg.Component, dg.Resolvable):
    """Component that registers non-dbt DuckDB assets."""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        return dg.Definitions(
            assets=[product_catalog, web_events, customer_demographics, enriched_web_sessions, customer_scoring_report],
        )
