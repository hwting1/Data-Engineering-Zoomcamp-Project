"""Weekly Citi Bike Metrics page."""

import dash
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dcc, html

from data import load_hourly_metrics, load_weekly_trend

dash.register_page(__name__, name="Weekly Metrics", path="/weekly", order=2)

_WEEKDAY_ORDER = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

GRAPH_BOX = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "8px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
}
CARD = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "18px 24px",
    "textAlign": "center",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "flex": "1",
    "minWidth": "180px",
}

_df: pl.DataFrame | None = None
_title: str = "Weekly Citi Bike Metrics"
_n_weeks: float = 1.0


def _get_df() -> pl.DataFrame:
    global _df, _title, _n_weeks
    if _df is None:
        _df = pl.DataFrame(load_weekly_trend())
        hourly = pl.DataFrame(load_hourly_metrics())
        if not hourly.is_empty():
            mn = hourly["metric_date"].min()
            mx = hourly["metric_date"].max()
            _title = f"Weekly Citi Bike Metrics  ({mn} – {mx})"
            _n_weeks = max(((mx - mn).days + 1) / 7, 1)
    return _df


# --- Layout ----------------------------------------------------------------

layout = html.Div(
    [
        html.H3(id="weekly-title", style={"margin": "0 0 4px 0"}),
        html.P(
            "Aggregated ride patterns across days of the week",
            style={"margin": "0 0 16px 0", "color": "#555", "fontSize": "13px"},
        ),
        html.Div(id="weekly-kpis", style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),
        html.Div(
            [
                html.Label("Membership:", style={"fontWeight": "bold", "fontSize": "13px"}),
                dcc.Dropdown(
                    id="weekly-bike-filter",
                    options=[{"label": "All", "value": "All"}],
                    value="All",
                    clearable=False,
                    style={"width": "180px"},
                ),
            ],
            style={"display": "flex", "gap": "8px", "alignItems": "center", "marginBottom": "16px"},
        ),
        # --- Row 1: Member distribution donut + bar by day ---
        html.H4("Members Type Distribution", style={"textAlign": "center", "margin": "8px 0"}),
        html.Div(
            [
                html.Div(dcc.Graph(id="weekly-member-donut"), style={**GRAPH_BOX, "flex": "2"}),
                html.Div(dcc.Graph(id="weekly-member-day-bar"), style={**GRAPH_BOX, "flex": "3"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "20px"},
        ),
        # --- Row 2: Bike type donut + bar by day ---
        html.H4("Bike Type Distribution", style={"textAlign": "center", "margin": "8px 0"}),
        html.Div(
            [
                html.Div(dcc.Graph(id="weekly-bike-donut"), style={**GRAPH_BOX, "flex": "2"}),
                html.Div(dcc.Graph(id="weekly-bike-day-bar"), style={**GRAPH_BOX, "flex": "3"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "20px"},
        ),
    ]
)


# --- Callbacks --------------------------------------------------------------


@callback(
    Output("weekly-title", "children"),
    Output("weekly-kpis", "children"),
    Output("weekly-bike-filter", "options"),
    Output("weekly-member-donut", "figure"),
    Output("weekly-member-day-bar", "figure"),
    Output("weekly-bike-donut", "figure"),
    Output("weekly-bike-day-bar", "figure"),
    Input("weekly-bike-filter", "value"),
)
def update_weekly(bike_filter: str):
    df = _get_df()
    n_weeks = _n_weeks

    if df.is_empty():
        empty_fig = px.bar(title="No data available – run the pipeline first")
        no_opts = [{"label": "All", "value": "All"}]
        return (_title, [], no_opts, empty_fig, empty_fig, empty_fig, empty_fig)

    members = sorted(df["member_casual"].unique().to_list())
    options = [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in members]
    filtered = df if bike_filter == "All" else df.filter(pl.col("member_casual") == bike_filter)

    avg_weekly_rides = filtered["ride_count"].sum() / n_weeks

    def _kpi_card(label: str, value: str) -> html.Div:
        return html.Div(
            [
                html.Div(label, style={"fontSize": "12px", "color": "#666", "marginBottom": "4px"}),
                html.Div(value, style={"fontSize": "24px", "fontWeight": "bold", "color": "#1a3e5c"}),
            ],
            style=CARD,
        )

    kpis = [
        _kpi_card("Avg Weekly Rides", f"{avg_weekly_rides:,.0f}"),
    ]

    _layout = dict(plot_bgcolor="white", paper_bgcolor="white")

    # --- Member donut ---
    mem_agg = (
        filtered.group_by("member_casual")
        .agg((pl.col("ride_count").sum() / n_weeks).alias("avg_rides"))
    )
    fig_mem_donut = px.pie(
        mem_agg.to_pandas(),
        names="member_casual",
        values="avg_rides",
        hole=0.4,
        color="member_casual",
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_mem_donut.update_traces(textinfo="percent+label")
    fig_mem_donut.update_layout(showlegend=False, **_layout, margin=dict(t=20, b=20))

    # --- Member bar by day ---
    mem_day = (
        filtered.group_by("weekday_name", "day_of_week", "member_casual")
        .agg((pl.col("ride_count").sum() / n_weeks).alias("avg_rides"))
        .sort("day_of_week")
    )
    fig_mem_day = px.bar(
        mem_day.to_pandas(),
        x="weekday_name",
        y="avg_rides",
        color="member_casual",
        barmode="stack",
        labels={"weekday_name": "", "avg_rides": "Avg Rides", "member_casual": "Membership"},
        category_orders={"weekday_name": _WEEKDAY_ORDER},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_mem_day.update_layout(**_layout, margin=dict(t=20, b=20))

    # --- Bike type donut ---
    bike_agg = (
        filtered.group_by("rideable_type")
        .agg((pl.col("ride_count").sum() / n_weeks).alias("avg_rides"))
    )
    fig_bike_donut = px.pie(
        bike_agg.to_pandas(),
        names="rideable_type",
        values="avg_rides",
        hole=0.4,
        color="rideable_type",
        color_discrete_map={"classic_bike": "#59a14f", "electric_bike": "#e15759"},
    )
    fig_bike_donut.update_traces(textinfo="percent+label")
    fig_bike_donut.update_layout(showlegend=False, **_layout, margin=dict(t=20, b=20))

    # --- Bike type bar by day ---
    bike_day = (
        filtered.group_by("weekday_name", "day_of_week", "rideable_type")
        .agg((pl.col("ride_count").sum() / n_weeks).alias("avg_rides"))
        .sort("day_of_week")
    )
    fig_bike_day = px.bar(
        bike_day.to_pandas(),
        x="weekday_name",
        y="avg_rides",
        color="rideable_type",
        barmode="group",
        labels={"weekday_name": "", "avg_rides": "Avg Rides", "rideable_type": "Bike Type"},
        category_orders={"weekday_name": _WEEKDAY_ORDER},
        color_discrete_map={"classic_bike": "#59a14f", "electric_bike": "#e15759"},
    )
    fig_bike_day.update_layout(**_layout, margin=dict(t=20, b=20))

    return (_title, kpis, options, fig_mem_donut, fig_mem_day, fig_bike_donut, fig_bike_day)
