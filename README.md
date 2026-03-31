# NYC Citi Bike Data Pipeline

## Overview

This project builds an end-to-end data engineering pipeline for NYC Citi Bike trip data as part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) capstone project.

Raw trip records are fetched monthly from the [Citi Bike public S3 bucket](https://s3.amazonaws.com/tripdata/index.html), landed in **Google Cloud Storage**, loaded and transformed in **BigQuery**, and surfaced through an interactive **Plotly Dash** dashboard. Infrastructure is managed with **Terraform** and the pipeline is orchestrated with **Bruin**.

**Tech stack at a glance:**

| Layer | Tool |
|---|---|
| Infrastructure as Code | Terraform |
| Data Lake | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Pipeline Orchestration | Bruin |
| Transformation | BigQuery SQL |
| Dashboard | Plotly Dash |
| Build Automation | Makefile |
| CI | GitHub Actions |

### Pipeline Architecture
![image](./assets/architecture.png)

---

## Problem Description

New York City's Citi Bike programme generates millions of trip records every month. While the raw data is publicly available, it is split into separate monthly ZIP archives on S3 and contains no pre-computed analytics — making it hard to answer operational and strategic questions across different time scales.

### Intra-Day Patterns
Within a single day, ride demand is far from uniform. Are there clear morning and evening commuter peaks, and do members and casual riders follow fundamentally different demand curves? How do trip duration and distance shift across the hours — do riders travel farther at certain times? Understanding intra-day behaviour is essential for capacity planning and rebalancing docks at high-demand hours.

### Day-of-Week Patterns
Aggregated across the full dataset, certain weekdays consistently attract more rides than others. Do members and casual riders gravitate toward different days — mid-week commuters vs. weekend leisure riders? Does the mix of classic and electric bikes shift depending on the day? Identifying structural day-of-week patterns helps distinguish commuter demand from recreational demand.

### Monthly Trends and Station Activity
Ride volume is strongly seasonal, peaking in summer and troughing in winter, but the magnitude of those swings and how they differ by membership type and bike type are not obvious from raw data. Which stations sustain the highest throughput month after month, and do the top departure and arrival stations coincide? Understanding seasonality and station-level concentration is critical for infrastructure investment decisions.

### Year-over-Year Growth
Comparing 2024 to 2025 in full reveals which months, membership segments, and bike types drove aggregate growth. Was growth evenly distributed across the year, or concentrated in specific months? Did annual members and casual riders move in opposite directions? How rapidly is electric bike adoption displacing classic bikes? Isolating these year-over-year dynamics separates genuine demand growth from seasonal noise.

The pipeline runs on the **15th of every month**, processing the prior month's data so the analysis always reflects the most recent complete month.

---

## Dashboard Insights
The following findings are drawn from the interactive dashboard covering **92.8 million trips** across 2024 and 2025 (plus partial 2026 data).

### Page 1 — Hourly Metrics
- **Dual commuter peaks** are clearly visible for members: ride volume surges at **08:00** (5.3 M member rides) and again at **17:00** (7.1 M member rides) — the single busiest hour in the entire dataset.
- **Casual riders show a broad afternoon curve**, building through midday (13:00–16:00, ~1.1–1.3 M rides per hour) and peaking at **17:00 (1.4 M)** — without the sharp twin-spike shape seen for members, and with no distinct morning spike.
- **Trip duration diverges sharply by membership type.** Member average duration ranges ~9–12 min throughout the day (shortest at the pre-dawn 05:00 rush, longest at 17:00), while casual duration peaks at 13:00–14:00 (~21.4 min) — nearly twice as long.
- **Electric bikes lead across all hours**, accounting for roughly 68% of rides at every time slot. The advantage is most pronounced during commute hours, where members strongly prefer e-bikes.
- **Ride distance by hour** is more variable for members, dipping to ~1.8 km at midday (12:00–13:00) and reaching ~2.4 km at the 06:00 commute peak; casual riders maintain a steadier ~2.1–2.4 km throughout the day.
- Weekday trips average **12.0 min** vs. weekend trips **14.1 min**, driven by casual riders extending trip length on weekends.

### Page 2 — Weekly Trend
- **Members** are busiest mid-week: Tuesday (11.9 M) and Wednesday (11.9 M) record the highest ride counts; Sunday (8.6 M) is the quietest day for members.
- **Casual riders** peak on **Saturday** (3.3 M rides, avg 21.1 min / 2.33 km) — their longest and farthest trips of the week — and drop to a low on Monday (2.0 M).
- Overall, **Friday tops total ridership** (14.1 M), while **Sunday is the quietest day** across all riders (11.4 M), lower even than Monday.
- Average distance is slightly higher on weekends for both groups (members: 2.0–2.05 km Sat/Sun vs. ~1.94–1.98 km weekdays), consistent with leisure trip patterns.

### Page 3 — Monthly Metrics (2024 – 2026)
- **Strong seasonality** repeats every year: rides ramp up from March, peak in September–October, and trough in January–February.
- **September 2025** (5.27 M) is the highest single-month total in the dataset; **October 2024** (5.13 M) is the second highest.
- Winter troughs (~1.8–2.1 M in Jan–Feb) are less than **40%** of peak months.
- **2026 data (Jan–Feb):** Jan 2026 recorded 1.81 M rides and Feb 1.21 M, continuing the expected winter dip. Electric bikes already account for ~71% of member rides in these two months, reflecting the accelerating shift away from classic bikes.
- The top 10 departure stations are clustered in Midtown/Chelsea and along the waterfront: **W 21 St & 6 Ave** leads with ~13,200 avg monthly trips, followed by **Pier 61 at Chelsea Piers** (~11,500) and **Lafayette St & E 8 St** (~11,200). The top 10 arrival stations are identical to the top departure stations — the same 10 locations appear in both lists, confirming these as true two-way hubs.

### 2024 vs 2025 Year-over-Year Comparison
- Total rides grew from **44.2 M (2024)** to **45.6 M (2025)**, a net **+3.3%** increase — but the pattern was uneven across months.
- **Growth was concentrated in early 2025**: Jan +12.6%, Mar +18.9%, Apr +15.7%, Aug +11.3%.
- **H2 2025 saw a pullback**: Oct −8.1%, Nov −7.9%, Dec −9.5%, suggesting ridership normalisation after a strong first half.
- **Annual members drove all growth**: +5.6% (35.7 M → 37.7 M), while casual riders declined −6.6% (8.5 M → 7.9 M).
- **Electric bike uptake accelerated**: classic bike rides fell −9.9% (15.0 M → 13.5 M), while electric bike rides rose +10.1% (29.2 M → 32.1 M), pushing e-bikes to **70% of 2025 rides** (up from 66% in 2024).

The Dashboard has been deployed on [Plotly Cloud](https://c363df96-4c8c-4947-9c8f-0b1faeb922f2.plotly.app/)