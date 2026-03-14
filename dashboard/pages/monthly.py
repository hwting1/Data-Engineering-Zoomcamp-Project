"""Monthly Citi Bike Metrics page."""

import dash
from dash import html, dcc, callback, Output, Input
import plotly.express as px
import polars as pl

from data import load_monthly_metrics, load_hourly_metrics

dash.register_page(__name__, name="Monthly Metrics", path="/monthly")

_df: pl.DataFrame | None = None

CARD = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "18px 24px",
    "textAlign": "center",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "flex": "1",
    "minWidth": "180px",
}
GRAPH_BOX = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "8px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
}


def _get_df() -> pl.DataFrame:
    global _df
    if _df is None:
        _df = pl.DataFrame(load_monthly_metrics())
    return _df


def _date_range() -> tuple[str, str]:
    hourly = pl.DataFrame(load_hourly_metrics())
    if not hourly.is_empty():
        return str(hourly["metric_date"].min()), str(hourly["metric_date"].max())
    df = _get_df()
    return str(df["metric_month"].min()), str(df["metric_month"].max())


# --- Layout ----------------------------------------------------------------

layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H3(id="monthly-title", style={"margin": "0"}),
                        html.P(
                            id="monthly-subtitle",
                            style={"margin": "0", "color": "#555", "fontSize": "13px"},
                        ),
                    ]
                ),
                html.Div(
                    [
                        html.Label("Membership:", style={"fontWeight": "bold", "fontSize": "13px"}),
                        dcc.Dropdown(
                            id="monthly-member-filter",
                            options=[{"label": "All", "value": "All"}],
                            value="All",
                            clearable=False,
                            style={"width": "160px"},
                        ),
                    ],
                    style={"display": "flex", "gap": "8px", "alignItems": "center"},
                ),
            ],
            style={
                "display": "flex",
                "justifyContent": "space-between",
                "alignItems": "center",
                "marginBottom": "16px",
            },
        ),
        # --- KPI cards ---
        html.Div(id="monthly-kpis", style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),
        # --- Row 1: Monthly rides line + stacked bike bar ---
        html.Div(
            [
                html.Div(dcc.Graph(id="monthly-rides-line"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="monthly-bike-stack"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 2: Duration trend + Distance trend ---
        html.Div(
            [
                html.Div(dcc.Graph(id="monthly-dur-line"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="monthly-dist-line"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px"},
        ),
    ]
)


# --- Callbacks --------------------------------------------------------------

def _kpi_card(label: str, value: str) -> html.Div:
    return html.Div(
        [
            html.Div(label, style={"fontSize": "12px", "color": "#666", "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "24px", "fontWeight": "bold", "color": "#1a3e5c"}),
        ],
        style=CARD,
    )


@callback(
    Output("monthly-title", "children"),
    Output("monthly-subtitle", "children"),
    Output("monthly-member-filter", "options"),
    Output("monthly-kpis", "children"),
    Output("monthly-rides-line", "figure"),
    Output("monthly-bike-stack", "figure"),
    Output("monthly-dur-line", "figure"),
    Output("monthly-dist-line", "figure"),
    Input("monthly-member-filter", "value"),
)
def update_monthly(member_filter: str):
    df = _get_df()

    if df.is_empty():
        empty_fig = px.line(title="No data available – run the pipeline first")
        return "Monthly Citi Bike Metrics", "(no data)", [{"label": "All", "value": "All"}], [], empty_fig, empty_fig, empty_fig, empty_fig

    mn, mx = _date_range()
    title = "Monthly Citi Bike Metrics"
    subtitle = f"Data range: {mn} – {mx}"

    members = sorted(df["member_casual"].unique().to_list())
    options = [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in members]
    filtered = df if member_filter == "All" else df.filter(pl.col("member_casual") == member_filter)

    n_months = filtered["metric_month"].n_unique()
    avg_monthly_rides = filtered["ride_count"].sum() / n_months
    avg_dur = filtered.select(
        (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum() / pl.col("ride_count").sum()
    ).item()
    avg_dist = filtered.select(
        (pl.col("avg_ride_distance_km") * pl.col("ride_count")).sum() / pl.col("ride_count").sum()
    ).item()
    num_months = filtered["metric_month"].n_unique()

    kpis = [
        _kpi_card("Avg Monthly Rides", f"{avg_monthly_rides:,.0f}"),
        _kpi_card("Avg Duration", f"{avg_dur:.1f} min"),
        _kpi_card("Avg Distance", f"{avg_dist:.2f} km"),
        _kpi_card("Months Covered", str(num_months)),
    ]

    _layout = dict(plot_bgcolor="white", paper_bgcolor="white")

    # --- Monthly rides by membership ---
    rides_m = (
        filtered.group_by("metric_month", "member_casual")
        .agg(pl.col("ride_count").mean().alias("avg_rides"))
        .sort("metric_month")
    )
    fig_rides = px.line(
        rides_m.to_pandas(),
        x="metric_month",
        y="avg_rides",
        color="member_casual",
        markers=True,
        title="Avg Monthly Rides by Membership",
        labels={"metric_month": "Month", "avg_rides": "Avg Rides", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_rides.update_layout(**_layout)

    # --- Stacked bar by bike type ---
    bike_m = (
        filtered.group_by("metric_month", "rideable_type")
        .agg(pl.col("ride_count").mean().alias("avg_rides"))
        .sort("metric_month")
    )
    fig_bike = px.bar(
        bike_m.to_pandas(),
        x="metric_month",
        y="avg_rides",
        color="rideable_type",
        barmode="stack",
        title="Avg Rides by Bike Type per Month",
        labels={"metric_month": "Month", "avg_rides": "Avg Rides", "rideable_type": "Bike Type"},
        color_discrete_map={"classic_bike": "#59a14f", "electric_bike": "#e15759"},
    )
    fig_bike.update_layout(**_layout)

    # --- Avg duration trend ---
    dur_m = (
        filtered.group_by("metric_month", "member_casual")
        .agg(
            (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum().alias("w"),
            pl.col("ride_count").sum().alias("n"),
        )
        .with_columns((pl.col("w") / pl.col("n")).alias("avg_duration"))
        .sort("metric_month")
    )
    fig_dur = px.line(
        dur_m.to_pandas(),
        x="metric_month",
        y="avg_duration",
        color="member_casual",
        markers=True,
        title="Avg Ride Duration Over Time",
        labels={"metric_month": "Month", "avg_duration": "Avg Duration (min)", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_dur.update_layout(**_layout)

    # --- Avg distance trend ---
    dist_m = (
        filtered.group_by("metric_month", "member_casual")
        .agg(
            (pl.col("avg_ride_distance_km") * pl.col("ride_count")).sum().alias("w"),
            pl.col("ride_count").sum().alias("n"),
        )
        .with_columns((pl.col("w") / pl.col("n")).alias("avg_distance"))
        .sort("metric_month")
    )
    fig_dist = px.line(
        dist_m.to_pandas(),
        x="metric_month",
        y="avg_distance",
        color="member_casual",
        markers=True,
        title="Avg Ride Distance Over Time",
        labels={"metric_month": "Month", "avg_distance": "Avg Distance (km)", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_dist.update_layout(**_layout)

    return title, subtitle, options, kpis, fig_rides, fig_bike, fig_dur, fig_dist
