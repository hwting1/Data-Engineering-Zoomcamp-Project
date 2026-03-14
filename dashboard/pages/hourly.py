"""Hourly Usage Metrics page."""

import dash
from dash import html, dcc, callback, Output, Input
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from data import load_hourly_metrics

dash.register_page(__name__, name="Hourly Metrics", path="/")

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
        _df = pl.DataFrame(load_hourly_metrics())
    return _df


# --- Layout ----------------------------------------------------------------

layout = html.Div(
    [
        # --- Header row: subtitle + filter ---
        html.Div(
            [
                html.Div(
                    [
                        html.H3(id="hourly-title", style={"margin": "0"}),
                        html.P(
                            id="hourly-subtitle",
                            style={"margin": "0", "color": "#555", "fontSize": "13px"},
                        ),
                    ]
                ),
                html.Div(
                    [
                        html.Label("Membership:", style={"fontWeight": "bold", "fontSize": "13px"}),
                        dcc.Dropdown(
                            id="hourly-member-filter",
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
        html.Div(id="hourly-kpis", style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),
        # --- Charts row 1: hourly activity per membership + weekday/weekend split ---
        html.Div(
            [
                html.Div(dcc.Graph(id="hourly-activity-line"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="hourly-weekend-bar"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Charts row 2: bike type bar + duration/distance scatter ---
        html.Div(
            [
                html.Div(dcc.Graph(id="hourly-biketype-bar"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="hourly-duration-dist"), style={**GRAPH_BOX, "flex": "1"}),
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
    Output("hourly-title", "children"),
    Output("hourly-subtitle", "children"),
    Output("hourly-member-filter", "options"),
    Output("hourly-kpis", "children"),
    Output("hourly-activity-line", "figure"),
    Output("hourly-weekend-bar", "figure"),
    Output("hourly-biketype-bar", "figure"),
    Output("hourly-duration-dist", "figure"),
    Input("hourly-member-filter", "value"),
)
def update_hourly(member_filter: str):
    df = _get_df()

    if df.is_empty():
        empty_fig = px.line(title="No data available – run the pipeline first")
        return "Hourly Usage Metrics", "(no data)", [{"label": "All", "value": "All"}], [], empty_fig, empty_fig, empty_fig, empty_fig

    mn, mx = df["metric_date"].min(), df["metric_date"].max()
    title = "Hourly Usage Metrics"
    subtitle = f"Data range: {mn} – {mx}"

    members = sorted(df["member_casual"].unique().to_list())
    options = [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in members]
    filtered = df if member_filter == "All" else df.filter(pl.col("member_casual") == member_filter)

    n_days = filtered["metric_date"].n_unique()
    avg_daily_rides = filtered["ride_count"].sum() / n_days
    avg_dur = filtered.select(
        (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum() / pl.col("ride_count").sum()
    ).item()
    avg_dist = filtered.select(
        (pl.col("avg_ride_distance_km") * pl.col("ride_count")).sum() / pl.col("ride_count").sum()
    ).item()
    avg_spd = filtered.filter(pl.col("avg_speed_kmh").is_not_null()).select(
        (pl.col("avg_speed_kmh") * pl.col("ride_count")).sum() / pl.col("ride_count").sum()
    ).item()

    kpis = [
        _kpi_card("Avg Daily Rides", f"{avg_daily_rides:,.0f}"),
        _kpi_card("Avg Duration", f"{avg_dur:.1f} min"),
        _kpi_card("Avg Distance", f"{avg_dist:.2f} km"),
        _kpi_card("Avg Speed", f"{avg_spd:.1f} km/h"),
    ]

    # --- Hourly activity by membership type ---
    hourly_member = (
        filtered.group_by("started_hour", "member_casual")
        .agg(pl.col("ride_count").mean().alias("avg_rides"))
        .sort("started_hour")
    )
    fig_line = px.line(
        hourly_member.to_pandas(),
        x="started_hour",
        y="avg_rides",
        color="member_casual",
        markers=True,
        title="Avg Hourly Activity by Membership",
        labels={"started_hour": "Hour of Day", "avg_rides": "Avg Rides", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_line.update_layout(xaxis=dict(dtick=1), plot_bgcolor="white", paper_bgcolor="white")

    # --- Weekday vs Weekend ---
    we_agg = (
        filtered.group_by("is_weekend", "member_casual")
        .agg(pl.col("ride_count").mean().alias("avg_rides"))
    )
    we_agg = we_agg.with_columns(
        pl.when(pl.col("is_weekend")).then(pl.lit("Weekend")).otherwise(pl.lit("Weekday")).alias("day_type")
    )
    fig_weekend = px.bar(
        we_agg.to_pandas(),
        x="day_type",
        y="avg_rides",
        color="member_casual",
        barmode="group",
        title="Avg Weekday vs Weekend Rides",
        labels={"day_type": "", "avg_rides": "Avg Rides", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_weekend.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    # --- Bike type distribution ---
    bike_agg = (
        filtered.group_by("rideable_type", "member_casual")
        .agg(pl.col("ride_count").mean().alias("avg_rides"))
        .sort("rideable_type")
    )
    fig_bike = px.bar(
        bike_agg.to_pandas(),
        x="rideable_type",
        y="avg_rides",
        color="member_casual",
        barmode="group",
        title="Avg Rides by Bike Type & Membership",
        labels={"rideable_type": "Bike Type", "avg_rides": "Avg Rides", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_bike.update_layout(plot_bgcolor="white", paper_bgcolor="white")

    # --- Avg Duration by Hour ---
    dur_hour = (
        filtered.group_by("started_hour", "member_casual")
        .agg(
            (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum().alias("w_dur"),
            pl.col("ride_count").sum().alias("total"),
        )
        .with_columns((pl.col("w_dur") / pl.col("total")).alias("avg_duration"))
        .sort("started_hour")
    )
    fig_dur = px.line(
        dur_hour.to_pandas(),
        x="started_hour",
        y="avg_duration",
        color="member_casual",
        markers=True,
        title="Avg Ride Duration by Hour",
        labels={"started_hour": "Hour of Day", "avg_duration": "Avg Duration (min)", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_dur.update_layout(xaxis=dict(dtick=1), plot_bgcolor="white", paper_bgcolor="white")

    return title, subtitle, options, kpis, fig_line, fig_weekend, fig_bike, fig_dur
