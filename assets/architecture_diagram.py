"""Generate the NYC Citi Bike Data Pipeline Architecture diagram.

Uses local icon files from the assets/ folder.
Run:  python assets/architecture_diagram.py
Output: architecture.png in the project root.
"""

import os
from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.gcp.analytics import BigQuery
from diagrams.gcp.storage import GCS
from diagrams.onprem.client import User
from diagrams.onprem.iac import Terraform
from diagrams.programming.language import Python
from diagrams.custom import Custom

# --- Resolve icon paths relative to this script ---
ASSETS = Path(__file__).resolve().parent
BRUIN_ICON = str(ASSETS / "bruin icon.png")
PLOTLY_ICON = str(ASSETS / "plotly icon.png")
ROOT = str(ASSETS.parent)

graph_attr = {
    "fontsize": "24",
    "fontname": "Sans-Serif",
    "bgcolor": "white",
    "pad": "0.6",
    "nodesep": "1.0",
    "ranksep": "1.5",
    "splines": "ortho",
}
node_attr = {
    "fontsize": "13",
    "fontname": "Sans-Serif",
}
edge_attr = {
    "fontsize": "11",
    "fontname": "Sans-Serif",
    "color": "#444444",
}

with Diagram(
    "NYC Citi Bike Data Pipeline Architecture",
    filename=os.path.join(ROOT, "architecture"),
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    # --- Infrastructure (top-left, like the reference) ---
    tf = Terraform("Terraform")

    # --- Data Source ---
    source = S3("Citi Bike\nTrip Data\n(S3)")

    # --- Ingestion ---
    ingestion = Python("Python\n(Polars)")

    # --- Cloud Storage ---
    gcs = GCS("Google Cloud\nStorage")

    # --- Bruin (transformation + orchestration, sits above BigQuery) ---
    bruin = Custom("Bruin", BRUIN_ICON)

    # --- BigQuery ---
    bq = BigQuery("Google\nBigQuery")

    # --- Dashboard ---
    dashboard = Custom("Plotly Dash\nDashboard", PLOTLY_ICON)

    # === Main data flow (left → right) ===
    source >> Edge(label="CSV.zip Files") >> ingestion
    ingestion >> Edge(label="Parquet files") >> gcs
    gcs >> bq
    bq >> dashboard

    # === Bruin orchestrates transforms inside BigQuery ===
    bruin >> Edge(style="dashed", color="#555555") >> bq

    # === Terraform provisions infra ===
    tf >> Edge(style="dotted", color="darkgreen") >> gcs
    tf >> Edge(style="dotted", color="darkgreen") >> bq

    # === Bruin orchestrates the full pipeline ===
    bruin >> Edge(style="dashed", color="#555555") >> ingestion
    bruin >> Edge(style="dashed", color="#555555") >> gcs
