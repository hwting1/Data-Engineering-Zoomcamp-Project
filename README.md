# NYC Citi Bike Data Pipeline

## Overview

This project builds an end-to-end data engineering pipeline for NYC Citi Bike trip data as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) capstone project.

Raw trip records are fetched monthly from the [Citi Bike public S3 bucket](https://s3.amazonaws.com/tripdata), landed in **Google Cloud Storage**, loaded and transformed in **BigQuery**, and surfaced through an interactive **Plotly Dash** dashboard. Infrastructure is managed with **Terraform** and the pipeline is orchestrated with **Bruin**.

**Tech stack at a glance:**

| Layer | Tool |
|---|---|
| Infrastructure as Code | Terraform |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Pipeline Orchestration | Bruin |
| Transformation | BigQuery SQL |
| Dashboard | Plotly Dash |

---

## Problem Description

New York City's Citi Bike programme generates millions of trip records every month. While the raw data is publicly available, it is split into separate monthly ZIP archives on S3 and contains no pre-computed analytics — making it hard to answer operational and strategic questions such as:

- **When do people ride?** Are there clear morning/evening rush-hour peaks, and do patterns differ between weekdays and weekends?
- **Who is riding?** How does behaviour differ between annual *members* and *casual* (pay-per-ride) users in terms of trip frequency, duration, and distance?
- **What type of bike do riders prefer?** Are classic bikes and electric bikes used in similar ways, or do trip profiles diverge?
- **How are usage trends evolving?** Is ridership growing month-over-month, and are average trip durations or distances changing over time?
- **Which stations are most active?** Which start and end stations drive the most traffic?

This pipeline answers those questions by automating the ingestion and transformation of trip data and presenting the results in a three-page dashboard:

1. **Hourly Metrics** — KPI cards (avg daily rides, avg duration, avg distance, avg speed) plus charts for hourly activity by membership, weekday vs. weekend split, bike-type distribution, and duration/distance scatter.
2. **Weekly Trend** — Aggregated ride patterns across days of the week, broken down by membership type and bike type, with avg duration and distance per weekday.
3. **Monthly Metrics** — Month-over-month ride counts, average duration & distance, top stations, and membership/bike-type breakdowns.

The pipeline runs on the **15th of every month**, processing the prior month's data so the dashboard always reflects the most recent complete month.