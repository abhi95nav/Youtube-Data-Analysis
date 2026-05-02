# YouTube Trending Videos — End-to-End Data Analytics Pipeline

An end-to-end data engineering and analytics project built on **Databricks** and **Azure Data Lake Storage Gen2**. The pipeline ingests raw YouTube trending video data, transforms it through a **Medallion Architecture** (Bronze → Silver → Gold), and surfaces actionable insights via an interactive **AI/BI Dashboard**.

---

## Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌───────────┐
│  ADLS Gen2  │────▶│   BRONZE    │────▶│   SILVER    │────▶│      GOLD        │────▶│ DASHBOARD │
│  (Raw CSV/  │     │ Raw Delta + │     │ Star Schema │     │ Datamarts +      │     │ 6-Page    │
│   JSON)     │     │ Audit Cols  │     │ (Dims+Fact) │     │ Business Cases   │     │ Analytics │
└─────────────┘     └─────────────┘     └─────────────┘     └──────────────────┘     └───────────┘
```

## Data Lineage

```
╔═══════════════════════════════════════════════════════════════════════════════════════════════════════╗
║  SOURCE FILES (ADLS Gen2)                                                                         ║
╠═══════════════════════════════════════════════════════════════════════════════════════════════════════╣
║                                                                                                   ║
║   USvideos.csv ──────────────┐                                                                    ║
║   (~40K rows)                │                                                                    ║
║                              ▼                                                                    ║
║   ┌──────────────────────────────────────────┐                                                    ║
║   │  BRONZE                                  │                                                    ║
║   │  + audit columns per row                 │                                                    ║
║   │                                          │                                                    ║
║   │  raw_videos ─────────────────────────────┼──────┐                                             ║
║   │  raw_category ───────────────────────────┼──┐   │                                             ║
║   └──────────────────────────────────────────┘  │   │                                             ║
║                                                 │   │                                             ║
║   US_category_id.json ──────────────────────────┘   │                                             ║
║   (nested JSON)                                     │                                             ║
║                                                     ▼                                             ║
║   ┌─────────────────────────────────────────────────────────────────────────────────┐              ║
║   │  SILVER — Star Schema                                                           │              ║
║   │                                                                                 │              ║
║   │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │              ║
║   │  │  dim_date     │  │ dim_category │  │ dim_channel  │  │  dim_video   │        │              ║
║   │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘        │              ║
║   │         │                 │                  │                 │                 │              ║
║   │         │    ┌────────────┘                  │                 │                 │              ║
║   │         ▼    ▼                               ▼                 ▼                 │              ║
║   │  ┌─────────────────────────────────────────────────────────┐                    │              ║
║   │  │              fact_trending_videos                        │                    │              ║
║   │  └─────────────────────────────────────────────────────────┘                    │              ║
║   │         │                                                                       │              ║
║   │         ▼                                                                       │              ║
║   │  ┌──────────────┐     ┌───────────────────┐                                    │              ║
║   │  │   dim_tags    │◄───│  bridge_video_tags │                                    │              ║
║   │  └──────────────┘     └───────────────────┘                                    │              ║
║   └─────────────────────────────────────────────────────────────────────────────────┘              ║
║                    │                                                                              ║
║                    ▼                                                                              ║
║   ┌─────────────────────────────────────────────────────────────────────────────────┐              ║
║   │  GOLD — Datamarts                                                               │              ║
║   │                                                                                 │              ║
║   │  category_performance ──────▶ bc_category_scorecard ──────▶ Executive Overview  │              ║
║   │                                                             Content & Tags      │              ║
║   │                                                                                 │              ║
║   │  tag_trends ────────────────▶ bc_tag_strategy ────────────▶ Content & Tags      │              ║
║   │                                                                                 │              ║
║   │  trending_velocity ─────────▶ bc_viral_breakouts ─────────▶ Virality & Trending │              ║
║   │                                                                                 │              ║
║   │  weekend_vs_weekday ────────▶ bc_publishing_strategy ─────▶ Executive Overview  │              ║
║   │                                                                                 │              ║
║   │  channel_consistency ───┐                                                       │              ║
║   │  channel_daily_leaderboard┼─▶ bc_channel_power_rankings ──▶ Channel Intelligence│              ║
║   │                         │                                                       │              ║
║   │                         └──▶ bc_leaderboard_monopoly ─────▶ Trending Page       │              ║
║   │                                                             Diversity           │              ║
║   └─────────────────────────────────────────────────────────────────────────────────┘              ║
║                                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════════════════════════╝
```

**Lineage summary:** 2 source files → 2 bronze tables → 7 silver tables → 6 gold datamarts → 6 business case tables → 6 dashboard pages

---

## Data Source

| File | Format | Description |
| --- | --- | --- |
| `USvideos.csv` | CSV | ~40,000 rows of US YouTube trending video statistics (views, likes, dislikes, comments, tags, publish time) |
| `US_category_id.json` | JSON | Category ID → human-readable name mapping (nested `items[]` array) |

**Storage:** `abfss://employee@dataanlysisazuredatalake.dfs.core.windows.net/source_data/`

---

## Pipeline Notebooks

### 1. Bronze Layer — `bronze`

Raw ingestion with zero transformation. Reads source CSV and JSON files, appends audit metadata, and writes Delta tables.

**Audit columns added to every row:**

| Column | Purpose |
| --- | --- |
| `_bronze_ingested_at` | Processing timestamp |
| `_bronze_source_file` | Full ADLS path of the source file |
| `_bronze_batch_id` | UUID for batch-level traceability |
| `_bronze_is_valid` | Basic non-null check on primary key |

**Output tables:**
- `employeedatacatalog.bronze_youtube.raw_videos`
- `employeedatacatalog.bronze_youtube.raw_category`

---

### 2. Silver Layer — `silver`

Cleanses bronze data and models it into an analytics-ready **star schema**.

**Transformations applied:**
- Filter invalid rows (`_bronze_is_valid = false`)
- Parse `trending_date` from `yy.dd.MM` format to proper `DateType`
- Deduplicate on `(video_id, trending_date)`, keeping the highest-view row
- Derive engagement metrics: `engagement_rate`, `like_ratio`, `days_to_trend`
- Standardize booleans (`comments_disabled`, `ratings_disabled`)

**Star schema output:**

| Table | Type | Description |
| --- | --- | --- |
| `dim_date` | Dimension | Calendar attributes (day of week, weekend flag, quarter) |
| `dim_category` | Dimension | YouTube category lookup (exploded from nested JSON) |
| `dim_channel` | Dimension | Channel lifetime aggregates (total views, trending count) |
| `dim_video` | Dimension | Video metadata (title, publish time, description) |
| `dim_tags` | Dimension | 51,500+ unique tags extracted from pipe-delimited strings |
| `bridge_video_tags` | Bridge | Many-to-many mapping between videos and tags (126,000+ pairs) |
| `fact_trending_videos` | Fact | One row per video per trending day with all metrics |

**Schema:** `employeedatacatalog.silver_youtube`

---

### 3. Gold Layer — `gold`

Pre-aggregated datamarts — each answers a specific business question without requiring joins at query time.

| # | Table | Row Count | Business Question |
| --- | --- | --- | --- |
| 1 | `category_performance` | 16 | Which categories dominate the trending page? |
| 2 | `tag_trends` | 51,524 | Which tags drive the most views and engagement? |
| 3 | `trending_velocity` | 5,600 | Which videos grew the fastest and when did they peak? |
| 4 | `weekend_vs_weekday` | 7 | Does publishing day affect trending performance? |
| 5 | `channel_daily_leaderboard` | 2,050 | Who are the top 10 channels each day? |
| 6 | `channel_consistency` | 2,200 | Which channels consistently appear on trending? |

**Schema:** `employeedatacatalog.gold_youtube`

---

### 4. Gold Business Cases — `gold_business_cases`

Scenario-specific analytical tables derived from gold datamarts, designed to answer targeted business questions.

| # | Scenario | Output Table | Key Insight |
| --- | --- | --- | --- |
| 1 | Category Scorecard | `bc_category_scorecard` | Sentiment label, size tier, engagement grade, controversy index per category |
| 2 | Tag Strategy | `bc_tag_strategy` | Tag tiers with engagement efficiency and recommendations (Must Use / Hidden Gem / Saturated) |
| 3 | Viral Breakouts | `bc_viral_breakouts` | Top 100 most viral videos classified by peak timing (Early Peaker / Mid Peaker / Late Bloomer) |
| 4 | Publishing Strategy | `bc_publishing_strategy` | Day-of-week ranking by composite score (views + engagement + speed-to-trend) |
| 5 | Channel Power Rankings | `bc_channel_power_rankings` | Composite power score from consistency (40%) + leaderboard presence (30%) + engagement (30%) |
| 6 | Leaderboard Monopoly | `bc_leaderboard_monopoly` | Trending page diversity analysis and channel dominance detection |

---

## Dashboard

**YouTube Trending Analytics** — a 6-page interactive AI/BI dashboard built on the gold business case tables.

| Page | Focus |
| --- | --- |
| Executive Overview | High-level KPIs and trend summaries |
| Content & Tags | Category performance and tag strategy insights |
| Virality & Trending | Viral breakout patterns and velocity analysis |
| Channel Intelligence | Power rankings and consistency metrics |
| Trending Page Diversity | Monopoly detection and view concentration analysis |
| Global Filters | Cross-page filtering by category, sentiment, tag tier, and channel tier |

---

## Tech Stack

| Component | Technology |
| --- | --- |
| Cloud | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute | Databricks (Serverless) |
| Processing | Apache Spark (PySpark) |
| Table Format | Delta Lake |
| Catalog | Unity Catalog (`employeedatacatalog`) |
| Visualization | Databricks AI/BI Dashboards |
| Language | Python, SQL |

---

## Project Structure

```
Youtube-Data-Analysis/
├── README.md                  # This file
├── bronze                     # Notebook: Raw ingestion (CSV/JSON → Delta)
├── silver                     # Notebook: Cleansing & star schema modeling
├── gold                       # Notebook: Analytics-ready datamarts
├── gold_business_cases        # Notebook: Scenario-specific business tables
└── YouTube Trending Analytics # AI/BI Dashboard (6 pages)
```

---

## How to Run

1. **Prerequisites:** Access to the Databricks workspace with Unity Catalog enabled and ADLS Gen2 storage mounted.
2. **Execute notebooks in order:**
   - `bronze` — Ingests raw data into Delta tables
   - `silver` — Transforms into star schema
   - `gold` — Builds analytics datamarts
   - `gold_business_cases` — Produces business-specific scenario tables
3. **Open the dashboard** to explore interactive visualizations.

---

## Unity Catalog Schemas

| Layer | Schema | Table Count |
| --- | --- | --- |
| Bronze | `employeedatacatalog.bronze_youtube` | 2 |
| Silver | `employeedatacatalog.silver_youtube` | 7 |
| Gold | `employeedatacatalog.gold_youtube` | 12 |
