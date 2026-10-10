# Superstore Data Engineering Pipeline

An end-to-end data engineering project that transforms raw Superstore sales data into a clean, analytics-ready data warehouse using **Python, DuckDB, dbt, Apache Airflow, and Docker**.

The pipeline separates ingestion, cleansing, dimensional modeling, and orchestration into clear layers. The warehouse uses a **star schema** designed for sales and profitability analytics.

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Model](#data-model)
- [Repository Structure](#repository-structure)
- [Pipeline Workflow](#pipeline-workflow)
- [Data Quality](#data-quality)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Analytics Use Cases](#analytics-use-cases)
- [Current Status](#current-status)
- [Future Improvements](#future-improvements)

## Project Overview

The source Superstore dataset is a flat CSV file containing order, customer, product, shipping, location, and financial information. In a flat structure, analysts may need to repeat cleansing and transformation logic in every report.

This project builds a repeatable pipeline that:

- Loads the source CSV into an Operational Data Store (ODS).
- Standardizes column names, text fields, dates, and numeric data types.
- Creates dimensional models using dbt and SQL.
- Builds a star-schema warehouse for analytical queries.
- Orchestrates the workflow with an Apache Airflow DAG.
- Uses Docker to provide a reproducible runtime environment.
- Defines data-quality checks to help detect invalid records and broken relationships.

### Key result

The project proposal records a successful run of the `ods_staging_marts` Airflow DAG. The three pipeline tasks completed successfully, and the final `main_dwh.fact_orders` table contained **9,994 rows**.

## Architecture

```text
                  Superstore.csv
                        |
                        v
              Python + DuckDB loader
                        |
                        v
                ODS: ods.Superstore
                        |
                        v
               dbt staging transformation
                        |
                        v
               Staging: Stg_Superstore
                        |
                        v
                  dbt dimensional models
                        |
                        v
                  Star-schema warehouse
                    (main_dwh)
                        |
                        v
                SQL analytics / BI tools

      Apache Airflow orchestrates the pipeline
      Docker packages the execution environment
```

## Technology Stack

| Technology | Role |
|---|---|
| Python | Reads the source CSV and loads data into DuckDB |
| DuckDB | Embedded analytical database for ODS, staging, and warehouse layers |
| dbt | SQL transformations, model dependencies, dimensional modeling, tests, and documentation |
| Apache Airflow | Defines, runs, and monitors the pipeline DAG |
| Docker / Docker Compose | Packages services and supports a reproducible environment |
| SQL | Data cleansing, transformation, modeling, and analytics |

## Data Model

The warehouse follows a **Kimball-style star schema**. The central fact table stores order-line measures and foreign keys to descriptive dimensions.

### Fact table: `fact_orders`

**Grain:** one row per order line item (one product sold within an order), identified by the source `Row ID`.

Main columns include:

- `order_key` — surrogate key for the order line
- `order_id` — source order identifier
- `order_date_key` and `ship_date_key` — links to the date dimension
- `ship_mode_key`
- `customer_key`
- `category_key`
- `location_key`
- `product_key`
- `sales`, `quantity`, `discount`, and `profit`

### Dimension tables

| Dimension | Purpose |
|---|---|
| `dim_date` | Calendar attributes such as day, month, quarter, year, and weekend flag |
| `dim_customers` | Customer ID, customer name, and segment |
| `dim_products` | Product ID and product name |
| `dim_category` | Product category and sub-category |
| `dim_location` | Country, city, state, postal code, and region |
| `dim_ship_mode` | Shipping mode descriptions |

The date dimension is used twice in the fact table: once for the order date and once for the ship date. This is an example of a **role-playing dimension**.

### Surrogate keys

The design uses deterministic keys generated with `dbt_utils.generate_surrogate_key` for dimensions and the fact table, while `dim_date` uses a `YYYYMMDD` date key. Deterministic keys help keep identifiers consistent across rebuilds when the underlying business keys remain the same.

## Repository Structure

The structure below reflects the project layout described in the proposal. Adjust names and paths if your repository differs.

```text
.
├── dags/
│   └── ods_staging_marts.py
├── dbt_project/
│   ├── data/
│   │   └── raw/
│   │       └── Superstore.csv
│   ├── scripts/
│   │   └── load_to_ods.py
│   ├── models/
│   │   ├── staging/
│   │   │   ├── Stg_Superstore.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── dim_category.sql
│   │       ├── dim_ship_mode.sql
│   │       ├── dim_location.sql
│   │       ├── dim_date.sql
│   │       ├── fact_orders.sql
│   │       └── schema.yml
│   ├── tests/
│   │   ├── assert_fact_rowcount_matches_staging.sql
│   │   ├── assert_sales_and_profit_match.sql
│   │   ├── assert_ship_date_not_before_order_date.sql
│   │   └── assert_discount_and_quantity_valid.sql
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── dev.duckdb
├── Dockerfile
├── docker-compose.yml
└── README.md
```

> The database file `dev.duckdb` is generated at runtime in many setups. Avoid committing large or local-only database files unless you intentionally want to version them.

## Pipeline Workflow

### 1. Ingestion: CSV to ODS

The Python loader reads `Superstore.csv` and loads its contents into the DuckDB table `ods.Superstore`. The ODS keeps a copy of the source data for downstream transformations.

### 2. Staging: cleansing with dbt

The staging model standardizes the source data by:

- Renaming columns to consistent `snake_case` names.
- Parsing order and shipping dates.
- Casting sales, profit, discount, and quantity to appropriate numeric types.
- Trimming text values and standardizing selected categories to lowercase.
- Exposing a clean staging view for downstream models.

### 3. Marts: dimensional modeling

dbt builds the six dimensions and the `fact_orders` table. The fact model joins staging data to the dimensions and stores dimension keys alongside the sales measures.

### 4. Orchestration: Airflow

The `ods_staging_marts` DAG runs tasks in dependency order:

```text
load_to_ods  >>  dbt_staging  >>  dbt_marts
```

A downstream task should run only after its upstream task succeeds.

### 5. Quality checks and documentation

dbt supports generic tests declared in YAML and custom SQL tests. The project proposal recommends adding a dedicated `dbt test` task after the marts task so test failures are visible in the Airflow run status.

## Data Quality

The proposed checks cover:

- **Uniqueness and completeness:** ensure row identifiers and surrogate keys are unique and non-null.
- **Accepted values:** validate known values for shipping mode, segment, region, and category.
- **Relationships:** check that fact-table foreign keys match dimension keys.
- **Row-count reconciliation:** compare the number of fact rows with the staging row count.
- **Financial reconciliation:** compare sales and profit totals between staging and the fact table.
- **Business rules:** validate that shipping dates do not precede order dates, discounts are between 0 and 1, and quantity is positive.

Run tests from the dbt project directory after configuring the environment:

```bash
dbt deps
dbt test
```

Use `dbt build` when you want dbt to build selected models and run their associated tests in dependency order.

## Getting Started

### Prerequisites

- Python compatible with the project's pinned dependencies
- Docker and Docker Compose, if using the containerized setup
- A source file at `dbt_project/data/raw/Superstore.csv`
- The project's dbt profile configured for DuckDB

### 1. Clone the repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>
cd <YOUR_REPOSITORY_DIRECTORY>
```

Replace the placeholders with your actual GitHub URL and repository directory.

### 2. Configure the source data

Place the Superstore CSV at the path expected by the loader:

```text
dbt_project/data/raw/Superstore.csv
```

Make sure you have permission to use and redistribute the dataset before committing it to a public repository. If you do not want to publish the raw data, document where it can be obtained and keep it out of Git.

### 3. Configure dbt

Review `dbt_project.yml` and `profiles.yml` to ensure that the project name, profile name, database path, and schemas match your local setup.

The project uses the `dbt_utils` package for deterministic surrogate keys. The package should be declared in `packages.yml`, for example:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
```

Install packages from the dbt project directory:

```bash
dbt deps
```

### 4. Start the containerized environment

If your repository includes a working `Dockerfile` and `docker-compose.yml`, start the services from the directory containing the Compose file:

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
```

The exact startup process and Airflow URL depend on your Compose configuration. Consult your configuration and Airflow logs if a service does not start.

## Running the Pipeline

1. Start the environment using the setup method configured in your repository.
2. Open the Airflow UI using the URL and credentials configured for your environment.
3. Find the `ods_staging_marts` DAG.
4. Trigger the DAG.
5. Confirm that `load_to_ods`, `dbt_staging`, and `dbt_marts` succeed.
6. Run dbt tests if the test task is not yet part of the DAG.

You can also run dbt commands manually from the dbt project directory, provided your profile and DuckDB path are configured correctly:

```bash
dbt run
dbt test
dbt docs generate
```

These commands assume the project and dependencies are configured. The Python ingestion task must run before staging and marts can use the loaded ODS table.

## Analytics Use Cases

The star schema supports questions such as:

- Which product categories and sub-categories generate the most sales and profit?
- How do sales and profitability change by month, quarter, or year?
- Which customers and customer segments are most profitable?
- Which regions, states, and cities contribute the most revenue?
- How do discounts relate to profitability?
- How do shipping modes and the time between order and shipment vary?

For discount analysis, use an average or a sales-weighted calculation rather than summing discount percentages.

## Current Status

According to the project proposal:

- **Implemented and validated:** CSV-to-ODS loading, dbt staging, dimensional models, and the three-task Airflow DAG.
- **Recorded result:** `main_dwh.fact_orders` contains 9,994 rows after a successful DAG run.
- **Recommended follow-up work:** finalize and run the data-quality tests, generate dbt documentation, finish container packaging, and connect a BI tool or configure a recurring schedule.

Update this section as you verify additional features in your GitHub repository. The proposal describes some quality, documentation, and deployment tasks as planned or still to be finalized, so do not mark them complete unless they are implemented and tested.

## Future Improvements

- Add a `dbt test` task to the Airflow DAG and fail the run when quality checks fail.
- Generate and publish dbt documentation and lineage.
- Add automated tests to CI/CD.
- Configure a daily schedule and failure notifications.
- Connect Power BI, Tableau, or Metabase to the warehouse.
- Add load audit metadata and stronger source-schema validation.
- Evaluate incremental ingestion if the source begins receiving new records regularly.
- Consider exporting analytical tables to Parquet for downstream consumers.

## Author

**Ahmed Ali Hasaballah Zaki**

Data Engineering Portfolio Project

---

If you find this project useful, feel free to star the repository or share feedback.
