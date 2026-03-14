"""Citi Bike Dashboard – multi-page Dash application."""

import dash
from dash import Dash, dcc, html

# Light blue background inspired by Looker Studio
BG_COLOR = "#f4f6f9"
NAV_COLOR = "#2c3e50"

app = Dash(
    __name__,
    use_pages=True,
    pages_folder="pages",
    suppress_callback_exceptions=True,
)

server = app.server

app.layout = html.Div(
    [
        html.Nav(
            [
                html.H2(
                    "NYC Citi Bike Trips Dashboard",
                    style={"margin": "0 32px 0 0", "fontSize": "22px", "fontWeight": "bold", "color": "#ffffff"},
                ),
                *[
                    dcc.Link(
                        page["name"],
                        href=page["path"],
                        style={
                            "marginRight": "20px",
                            "fontWeight": "600",
                            "textDecoration": "none",
                            "color": "#ffffff",
                            "fontSize": "14px",
                        },
                    )
                    for page in dash.page_registry.values()
                ],
            ],
            style={
                "display": "flex",
                "alignItems": "center",
                "padding": "14px 28px",
                "backgroundColor": NAV_COLOR,
                "borderBottom": "1px solid #1a252f",
            },
        ),
        html.Div(
            dash.page_container,
            style={"padding": "24px 28px", "backgroundColor": BG_COLOR, "minHeight": "100vh"},
        ),
    ],
    style={"fontFamily": "'Segoe UI', Roboto, Arial, sans-serif", "backgroundColor": BG_COLOR},
)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
