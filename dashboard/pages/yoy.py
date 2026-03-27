"""Year-over-Year (2024 vs 2025) Comparison page."""

import dash
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html

from data import load_yoy_monthly, load_yoy_top_stations

dash.register_page(__name__, name="2024 vs 2025", path="/yoy", order=4)

CARD = {
    "backgroundColor": "white",
    "borderRadius": "8px",
    "padding": "18px 24px",
    "textAlign": "center",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.08)",
    "flex": "1",
    "minWidth": "160px",
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
STAT_ROW = {
    "display": "flex",
    "justifyContent": "center",
    "gap": "24px",
    "flexWrap": "wrap",
    "marginBottom": "20px",
}

YEAR_COLORS = {2024: "#4e79a7", 2025: "#f28e2b"}
MONTH_LABELS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

_df: pl.DataFrame | None = None
_start_stations: pl.DataFrame | None = None
_end_stations: pl.DataFrame | None = None


def _cast_year(sdf: pl.DataFrame) -> pl.DataFrame:
    if not sdf.is_empty():
        return sdf.with_columns(pl.col("year").cast(pl.Int64))
    return sdf


def _get_data():
    global _df, _start_stations, _end_stations
    if _df is None:
        _df = pl.DataFrame(load_yoy_monthly())
        if not _df.is_empty():
            _df = _df.with_columns(
                pl.col("year").cast(pl.Int64),
                pl.col("month").cast(pl.Int64),
            )
        _start_stations = _cast_year(
            pl.DataFrame(load_yoy_top_stations("start"))
        )
        _end_stations = _cast_year(
            pl.DataFrame(load_yoy_top_stations("end"))
        )
    return _df, _start_stations, _end_stations


# --- Layout ----------------------------------------------------------------

layout = html.Div(
    [
        html.H3("2024 vs 2025 Year-over-Year Comparison", style={"margin": "0 0 4px 0"}),
        html.P(
            id="yoy-subtitle",
            style={"margin": "0 0 12px 0", "color": "#555", "fontSize": "13px"},
        ),
        # --- Filter ---
        html.Div(
            [
                html.Label("Membership:", style={"fontWeight": "bold", "fontSize": "13px"}),
                dcc.Dropdown(
                    id="yoy-member-filter",
                    options=[{"label": "All", "value": "All"}],
                    value="All",
                    clearable=False,
                    style={"width": "160px"},
                ),
            ],
            style={"display": "flex", "gap": "8px", "alignItems": "center", "marginBottom": "16px"},
        ),
        # --- Statistics cards ---
        html.Div(id="yoy-stats", style=STAT_ROW),
        # --- Row 1: Monthly rides comparison + YoY change bar ---
        html.Div(
            [
                html.Div(dcc.Graph(id="yoy-rides-line"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="yoy-change-bar"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 2: Member type grouped bar + Bike type grouped bar ---
        html.Div(
            [
                html.Div(dcc.Graph(id="yoy-member-bar"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="yoy-bike-bar"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 3: Avg duration + Avg distance comparison ---
        html.Div(
            [
                html.Div(dcc.Graph(id="yoy-dur-line"), style={**GRAPH_BOX, "flex": "1"}),
                html.Div(dcc.Graph(id="yoy-dist-line"), style={**GRAPH_BOX, "flex": "1"}),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 4: Top 10 Departure Stations (2024 vs 2025 tables) ---
        html.H4("Top 10 Departure Stations", style={"textAlign": "center", "margin": "16px 0 8px 0"}),
        html.Div(
            [
                html.Div(
                    [html.H5("2024", style={"textAlign": "center", "margin": "0 0 6px 0"}),
                     html.Div(id="yoy-start-table-2024")],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
                html.Div(
                    [html.H5("2025", style={"textAlign": "center", "margin": "0 0 6px 0"}),
                     html.Div(id="yoy-start-table-2025")],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "16px", "marginBottom": "16px"},
        ),
        # --- Row 5: Top 10 Arrival Stations (2024 vs 2025 tables) ---
        html.H4("Top 10 Arrival Stations", style={"textAlign": "center", "margin": "16px 0 8px 0"}),
        html.Div(
            [
                html.Div(
                    [html.H5("2024", style={"textAlign": "center", "margin": "0 0 6px 0"}),
                     html.Div(id="yoy-end-table-2024")],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
                html.Div(
                    [html.H5("2025", style={"textAlign": "center", "margin": "0 0 6px 0"}),
                     html.Div(id="yoy-end-table-2025")],
                    style={**GRAPH_BOX, "flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "16px"},
        ),
    ]
)


# --- Helpers ----------------------------------------------------------------


def _stat_card(label: str, val_2024: str, val_2025: str, change_pct: str | None = None) -> html.Div:
    """A KPI card showing both years side by side."""
    change_color = "#2ecc71" if change_pct and change_pct.startswith("+") else "#e74c3c"
    children = [
        html.Div(label, style={"fontSize": "11px", "color": "#666", "marginBottom": "6px", "fontWeight": "bold"}),
        html.Div(
            [
                html.Div(
                    [html.Span("2024", style={"fontSize": "10px", "color": "#888"}),
                     html.Div(val_2024, style={"fontSize": "20px", "fontWeight": "bold", "color": YEAR_COLORS[2024]})],
                    style={"flex": "1"},
                ),
                html.Div(
                    [html.Span("2025", style={"fontSize": "10px", "color": "#888"}),
                     html.Div(val_2025, style={"fontSize": "20px", "fontWeight": "bold", "color": YEAR_COLORS[2025]})],
                    style={"flex": "1"},
                ),
            ],
            style={"display": "flex", "gap": "12px"},
        ),
    ]
    if change_pct is not None:
        children.append(
            html.Div(
                f"YoY: {change_pct}",
                style={"fontSize": "13px", "color": change_color,
                       "marginTop": "4px", "fontWeight": "600"},
            )
        )
    return html.Div(children, style=CARD)


def _pct_str(old: float, new: float) -> str:
    if old == 0:
        return "N/A"
    pct = (new - old) / old * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _top_station_table(sdf: pl.DataFrame, year: int, limit: int = 10) -> dash_table.DataTable | html.Div:
    sub = sdf.filter(pl.col("year") == year).sort("trip_count", descending=True).head(limit)
    if sub.is_empty():
        return html.Div("No data")
    data = (
        sub.with_row_index(name="#", offset=1)
        .select(
            pl.col("#"),
            pl.col("station_name").alias("Station"),
            pl.col("trip_count").map_elements(lambda v: f"{v:,}", return_dtype=pl.Utf8).alias("Trips"),
        )
        .to_pandas()
        .to_dict("records")
    )
    return dash_table.DataTable(
        data=data,
        columns=[{"name": "#", "id": "#"}, {"name": "Station", "id": "Station"}, {"name": "Trips", "id": "Trips"}],
        style_header=TABLE_HEADER_STYLE,
        style_cell={"textAlign": "left", "padding": "6px 10px", "fontSize": "13px"},
        style_cell_conditional=[
            {"if": {"column_id": "#"}, "width": "40px", "textAlign": "center"},
            {"if": {"column_id": "Trips"}, "textAlign": "right", "width": "100px"},
        ],
        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "#f9f9f9"}],
        style_table={"overflowX": "auto"},
        page_action="none",
    )


# --- Callbacks --------------------------------------------------------------


@callback(
    Output("yoy-subtitle", "children"),
    Output("yoy-member-filter", "options"),
    Output("yoy-stats", "children"),
    Output("yoy-rides-line", "figure"),
    Output("yoy-change-bar", "figure"),
    Output("yoy-member-bar", "figure"),
    Output("yoy-bike-bar", "figure"),
    Output("yoy-dur-line", "figure"),
    Output("yoy-dist-line", "figure"),
    Output("yoy-start-table-2024", "children"),
    Output("yoy-start-table-2025", "children"),
    Output("yoy-end-table-2024", "children"),
    Output("yoy-end-table-2025", "children"),
    Input("yoy-member-filter", "value"),
)
def update_yoy(member_filter: str):
    df, start_st, end_st = _get_data()

    _layout = dict(plot_bgcolor="white", paper_bgcolor="white")
    empty_fig = px.bar(title="No data available – run the pipeline first")
    empty_fig.update_layout(**_layout)
    no_div = html.Div("No data")

    if df.is_empty():
        no_opts = [{"label": "All", "value": "All"}]
        return ("(no data)", no_opts, [],
                empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_fig,
                no_div, no_div, no_div, no_div)

    # --- Filters ---
    members = sorted(df["member_casual"].unique().to_list())
    options = [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in members]
    filtered = df if member_filter == "All" else df.filter(pl.col("member_casual") == member_filter)

    # --- Per-year aggregates for statistics ---
    yearly = (
        filtered.group_by("year")
        .agg(
            pl.col("ride_count").sum().alias("total_rides"),
            (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum().alias("dur_w"),
            (pl.col("avg_ride_distance_km") * pl.col("ride_count")).sum().alias("dist_w"),
            pl.col("ride_count").sum().alias("total_n"),
            pl.col("month").n_unique().alias("months_covered"),
        )
        .with_columns(
            (pl.col("dur_w") / pl.col("total_n")).alias("avg_duration"),
            (pl.col("dist_w") / pl.col("total_n")).alias("avg_distance"),
        )
        .sort("year")
    )

    # Peak month rides per year
    peak_month = (
        filtered.group_by("year", "month")
        .agg(pl.col("ride_count").sum().alias("rides"))
        .sort("rides", descending=True)
        .group_by("year")
        .first()
        .sort("year")
    )

    def _yval(col: str, year: int) -> float:
        row = yearly.filter(pl.col("year") == year)
        return row[col].item() if not row.is_empty() else 0

    def _peak(year: int) -> int:
        row = peak_month.filter(pl.col("year") == year)
        return int(row["rides"].item()) if not row.is_empty() else 0

    t24, t25 = _yval("total_rides", 2024), _yval("total_rides", 2025)
    d24, d25 = _yval("avg_duration", 2024), _yval("avg_duration", 2025)
    di24, di25 = _yval("avg_distance", 2024), _yval("avg_distance", 2025)
    pk24, pk25 = _peak(2024), _peak(2025)
    mc24, mc25 = int(_yval("months_covered", 2024)), int(_yval("months_covered", 2025))

    subtitle = f"2024: {mc24} months of data  |  2025: {mc25} months of data"

    stats = [
        _stat_card("Total Rides", f"{t24:,.0f}", f"{t25:,.0f}", _pct_str(t24, t25)),
        _stat_card("Peak Month Rides", f"{pk24:,}", f"{pk25:,}", _pct_str(pk24, pk25)),
        _stat_card("Avg Duration (min)", f"{d24:.1f}", f"{d25:.1f}", _pct_str(d24, d25)),
        _stat_card("Avg Distance (km)", f"{di24:.2f}", f"{di25:.2f}", _pct_str(di24, di25)),
    ]

    # --- Monthly rides line: month on x-axis, one line per year ---
    month_rides = (
        filtered.group_by("year", "month")
        .agg(pl.col("ride_count").sum().alias("rides"))
        .sort("year", "month")
        .with_columns(
            pl.col("month")
            .map_elements(lambda m: MONTH_LABELS[m - 1], return_dtype=pl.Utf8)
            .alias("month_label")
        )
        .with_columns(pl.col("year").cast(pl.Utf8))
    )
    fig_rides = px.line(
        month_rides.to_pandas(),
        x="month_label",
        y="rides",
        color="year",
        markers=True,
        title="Monthly Rides: 2024 vs 2025",
        labels={"month_label": "Month", "rides": "Total Rides", "year": "Year"},
        color_discrete_map={"2024": YEAR_COLORS[2024], "2025": YEAR_COLORS[2025]},
        category_orders={"month_label": MONTH_LABELS},
    )
    fig_rides.update_layout(**_layout)

    # --- YoY % change bar per month ---
    pivot = (
        filtered.group_by("year", "month")
        .agg(pl.col("ride_count").sum().alias("rides"))
        .pivot(on="year", index="month", values="rides")
        .sort("month")
    )
    # Ensure columns exist
    if "2024" not in pivot.columns:
        pivot = pivot.with_columns(pl.lit(0).alias("2024"))
    if "2025" not in pivot.columns:
        pivot = pivot.with_columns(pl.lit(0).alias("2025"))
    pivot = pivot.with_columns(
        pl.when(pl.col("2024") > 0)
        .then((pl.col("2025") - pl.col("2024")) / pl.col("2024") * 100)
        .otherwise(None)
        .alias("yoy_pct"),
        pl.col("month").map_elements(lambda m: MONTH_LABELS[m - 1], return_dtype=pl.Utf8).alias("month_label"),
    )
    fig_change = px.bar(
        pivot.to_pandas(),
        x="month_label",
        y="yoy_pct",
        title="YoY Ride Count Change (%)",
        labels={"month_label": "Month", "yoy_pct": "Change (%)"},
        color_discrete_sequence=["#76b7b2"],
        category_orders={"month_label": MONTH_LABELS},
    )
    fig_change.update_layout(**_layout)
    fig_change.add_hline(y=0, line_dash="dash", line_color="gray")

    # --- Member type grouped bar: 2024 vs 2025 ---
    mem_yr = (
        filtered.group_by("year", "member_casual")
        .agg(pl.col("ride_count").sum().alias("rides"))
        .with_columns(pl.col("year").cast(pl.Utf8))
        .sort("year")
    )
    fig_member = px.bar(
        mem_yr.to_pandas(),
        x="member_casual",
        y="rides",
        color="year",
        barmode="group",
        title="Rides by Membership Type",
        labels={"member_casual": "Membership", "rides": "Total Rides", "year": "Year"},
        color_discrete_map={"2024": YEAR_COLORS[2024], "2025": YEAR_COLORS[2025]},
        category_orders={"year": ["2024", "2025"]},
    )
    fig_member.update_layout(**_layout)

    # --- Bike type grouped bar: 2024 vs 2025 ---
    bike_yr = (
        filtered.group_by("year", "rideable_type")
        .agg(pl.col("ride_count").sum().alias("rides"))
        .with_columns(pl.col("year").cast(pl.Utf8))
        .sort("year")
    )
    fig_bike = px.bar(
        bike_yr.to_pandas(),
        x="rideable_type",
        y="rides",
        color="year",
        barmode="group",
        title="Rides by Bike Type",
        labels={"rideable_type": "Bike Type", "rides": "Total Rides", "year": "Year"},
        color_discrete_map={"2024": YEAR_COLORS[2024], "2025": YEAR_COLORS[2025]},
        category_orders={"year": ["2024", "2025"]},
    )
    fig_bike.update_layout(**_layout)

    # --- Avg duration comparison by month ---
    dur_m = (
        filtered.group_by("year", "month")
        .agg(
            (pl.col("avg_ride_duration_minutes") * pl.col("ride_count")).sum().alias("w"),
            pl.col("ride_count").sum().alias("n"),
        )
        .with_columns(
            (pl.col("w") / pl.col("n")).alias("avg_duration"),
            pl.col("month").map_elements(lambda m: MONTH_LABELS[m - 1], return_dtype=pl.Utf8).alias("month_label"),
            pl.col("year").cast(pl.Utf8),
        )
        .sort("year", "month")
    )
    fig_dur = px.line(
        dur_m.to_pandas(),
        x="month_label",
        y="avg_duration",
        color="year",
        markers=True,
        title="Avg Ride Duration by Month",
        labels={"month_label": "Month", "avg_duration": "Avg Duration (min)", "year": "Year"},
        color_discrete_map={"2024": YEAR_COLORS[2024], "2025": YEAR_COLORS[2025]},
        category_orders={"month_label": MONTH_LABELS},
    )
    fig_dur.update_layout(**_layout)

    # --- Avg distance comparison by month ---
    dist_m = (
        filtered.group_by("year", "month")
        .agg(
            (pl.col("avg_ride_distance_km") * pl.col("ride_count")).sum().alias("w"),
            pl.col("ride_count").sum().alias("n"),
        )
        .with_columns(
            (pl.col("w") / pl.col("n")).alias("avg_distance"),
            pl.col("month").map_elements(lambda m: MONTH_LABELS[m - 1], return_dtype=pl.Utf8).alias("month_label"),
            pl.col("year").cast(pl.Utf8),
        )
        .sort("year", "month")
    )
    fig_dist = px.line(
        dist_m.to_pandas(),
        x="month_label",
        y="avg_distance",
        color="year",
        markers=True,
        title="Avg Ride Distance by Month",
        labels={"month_label": "Month", "avg_distance": "Avg Distance (km)", "year": "Year"},
        color_discrete_map={"2024": YEAR_COLORS[2024], "2025": YEAR_COLORS[2025]},
        category_orders={"month_label": MONTH_LABELS},
    )
    fig_dist.update_layout(**_layout)

    # --- Station tables ---
    tbl_s24 = _top_station_table(start_st, 2024) if not start_st.is_empty() else no_div
    tbl_s25 = _top_station_table(start_st, 2025) if not start_st.is_empty() else no_div
    tbl_e24 = _top_station_table(end_st, 2024) if not end_st.is_empty() else no_div
    tbl_e25 = _top_station_table(end_st, 2025) if not end_st.is_empty() else no_div

    return (subtitle, options, stats,
            fig_rides, fig_change, fig_member, fig_bike, fig_dur, fig_dist,
            tbl_s24, tbl_s25, tbl_e24, tbl_e25)
