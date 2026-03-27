"""Monthly Citi Bike Metrics page."""

import dash
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dcc, html, dash_table

from data import load_hourly_metrics, load_monthly_metrics, load_top_stations

dash.register_page(__name__, name="Monthly Metrics", path="/monthly", order=3)

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
TABLE_HEADER_STYLE = {
    "backgroundColor": "#4e79a7",
    "color": "white",
    "fontWeight": "bold",
    "textAlign": "center",
    "padding": "8px",
}

_df: pl.DataFrame | None = None
_subtitle: str = ""


def _get_df() -> pl.DataFrame:
    global _df, _subtitle
    if _df is None:
        _df = pl.DataFrame(load_monthly_metrics())
        hourly = pl.DataFrame(load_hourly_metrics())
        if not hourly.is_empty():
            mn = hourly["metric_date"].min()
            mx = hourly["metric_date"].max()
            _subtitle = f"Data range: {mn} – {mx}"
        elif not _df.is_empty():
            mn = _df["metric_month"].min()
            mx = _df["metric_month"].max()
            _subtitle = f"Data range: {mn} – {mx}"
    return _df


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

        # --- Row 3: Top 10 Departure Stations (bar + table) ---
        html.Div(
            [
                html.Div(dcc.Graph(id="monthly-top-start-bar"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(
                    [
                        html.H4("Top 10 Departure Stations", style={"margin": "0 0 8px 0", "textAlign": "center"}),
                        html.Div(id="monthly-top-start-table"),
                    ],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 4: Top 10 Arrival Stations (bar + table) ---
        html.Div(
            [
                html.Div(dcc.Graph(id="monthly-top-end-bar"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(
                    [
                        html.H4("Top 10 Arrival Stations", style={"margin": "0 0 8px 0", "textAlign": "center"}),
                        html.Div(id="monthly-top-end-table"),
                    ],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "16px"},
        ),
    ]
)


# --- Helpers ----------------------------------------------------------------


def _kpi_card(label: str, value: str) -> html.Div:
    return html.Div(
        [
            html.Div(label, style={"fontSize": "12px", "color": "#666", "marginBottom": "4px"}),
            html.Div(value, style={"fontSize": "24px", "fontWeight": "bold", "color": "#1a3e5c"}),
        ],
        style=CARD,
    )


def _station_table(rows: pl.DataFrame) -> dash_table.DataTable:
    """Build a numbered DataTable for top stations."""
    data = rows.with_row_index(name="#", offset=1).rename(
        {"station_name": "Station Name", "avg_trips": "Avg Monthly Trips"}
    ).with_columns(
        pl.col("Avg Monthly Trips").map_elements(lambda v: f"{v:,}", return_dtype=pl.Utf8)
    ).to_pandas().to_dict("records")

    return dash_table.DataTable(
        data=data,
        columns=[
            {"name": "#", "id": "#"},
            {"name": "Station Name", "id": "Station Name"},
            {"name": "Avg Monthly Trips", "id": "Avg Monthly Trips"},
        ],
        style_header=TABLE_HEADER_STYLE,
        style_cell={"textAlign": "left", "padding": "6px 10px", "fontSize": "13px"},
        style_cell_conditional=[
            {"if": {"column_id": "#"}, "width": "40px", "textAlign": "center"},
            {"if": {"column_id": "Avg Monthly Trips"}, "textAlign": "right", "width": "140px"},
        ],
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f9f9f9"},
        ],
        style_table={"overflowX": "auto"},
        page_action="none",
    )


# --- Callbacks --------------------------------------------------------------


@callback(
    Output("monthly-title", "children"),
    Output("monthly-subtitle", "children"),
    Output("monthly-member-filter", "options"),
    Output("monthly-kpis", "children"),
    Output("monthly-rides-line", "figure"),
    Output("monthly-bike-stack", "figure"),
    Output("monthly-top-start-bar", "figure"),
    Output("monthly-top-start-table", "children"),
    Output("monthly-top-end-bar", "figure"),
    Output("monthly-top-end-table", "children"),
    Input("monthly-member-filter", "value"),
)
def update_monthly(member_filter: str):
    df = _get_df()

    if df.is_empty():
        empty_fig = px.line(title="No data available – run the pipeline first")
        no_opts = [{"label": "All", "value": "All"}]
        return ("Monthly Citi Bike Metrics", "(no data)", no_opts, [],
                empty_fig, empty_fig, empty_fig, html.Div(), empty_fig, html.Div())

    title = "Monthly Citi Bike Metrics"

    members = sorted(df["member_casual"].unique().to_list())
    options = [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in members]
    filtered = df if member_filter == "All" else df.filter(pl.col("member_casual") == member_filter)

    n_months = filtered["metric_month"].n_unique()
    avg_monthly_rides = filtered["ride_count"].sum() / n_months

    kpis = [
        _kpi_card("Avg Monthly Rides", f"{avg_monthly_rides:,.0f}"),
        _kpi_card("Months Covered", str(n_months)),
    ]

    _layout = dict(plot_bgcolor="white", paper_bgcolor="white")

    # --- Monthly rides by membership ---
    rides_m = (
        filtered.group_by("metric_month", "member_casual")
        .agg(pl.col("ride_count").sum().alias("total_rides"))
        .sort("metric_month")
    )
    fig_rides = px.line(
        rides_m.to_pandas(),
        x="metric_month",
        y="total_rides",
        color="member_casual",
        markers=True,
        title="Monthly Rides by Membership",
        labels={"metric_month": "Month", "total_rides": "Total Rides", "member_casual": "Membership"},
        color_discrete_map={"member": "#4e79a7", "casual": "#f28e2b"},
    )
    fig_rides.update_layout(**_layout)

    # --- Stacked bar by bike type ---
    bike_m = (
        filtered.group_by("metric_month", "rideable_type")
        .agg(pl.col("ride_count").sum().alias("total_rides"))
        .sort("metric_month")
    )
    fig_bike = px.bar(
        bike_m.to_pandas(),
        x="metric_month",
        y="total_rides",
        color="rideable_type",
        barmode="stack",
        title="Rides by Bike Type per Month",
        labels={"metric_month": "Month", "total_rides": "Total Rides", "rideable_type": "Bike Type"},
        color_discrete_map={"classic_bike": "#59a14f", "electric_bike": "#e15759"},
    )
    fig_bike.update_layout(**_layout)



    # --- Top 10 Departure Stations ---
    start_rows = pl.DataFrame(load_top_stations("start", 10))
    if not start_rows.is_empty():
        fig_start = px.bar(
            start_rows.to_pandas(),
            x="station_name",
            y="avg_trips",
            title="Top 10 Departure Stations (Avg Monthly Trips)",
            labels={"station_name": "", "avg_trips": "Avg Monthly Trips"},
            color_discrete_sequence=["#4e79a7"],
        )
        fig_start.update_layout(**_layout, xaxis_tickangle=-45)
        tbl_start = _station_table(start_rows)
    else:
        fig_start = px.bar(title="No station data available")
        fig_start.update_layout(**_layout)
        tbl_start = html.Div("No data")

    # --- Top 10 Arrival Stations ---
    end_rows = pl.DataFrame(load_top_stations("end", 10))
    if not end_rows.is_empty():
        fig_end = px.bar(
            end_rows.to_pandas(),
            x="station_name",
            y="avg_trips",
            title="Top 10 Arrival Stations (Avg Monthly Trips)",
            labels={"station_name": "", "avg_trips": "Avg Monthly Trips"},
            color_discrete_sequence=["#f28e2b"],
        )
        fig_end.update_layout(**_layout, xaxis_tickangle=-45)
        tbl_end = _station_table(end_rows)
    else:
        fig_end = px.bar(title="No station data available")
        fig_end.update_layout(**_layout)
        tbl_end = html.Div("No data")

    return (title, _subtitle, options, kpis,
            fig_rides, fig_bike, fig_start, tbl_start, fig_end, tbl_end)
