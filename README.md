# AHRF Primary Care Access Pipeline

**DataTalksClub Data Engineering Zoomcamp 2026 — Capstone Project**

A batch data pipeline analyzing primary care access gaps and underserved population density across U.S. counties using HRSA Area Health Resources Files (AHRF). Built with relevance to Federally Qualified Health Center (FQHC) operations in medically underserved areas.

---

## Problem Statement

FQHCs and health planners need county-level visibility into primary care supply relative to population need. This pipeline ingests three release years of AHRF data (2022–2023, 2023–2024, 2024–2025), transforms it into an analytical fact table, and surfaces access metrics in a dashboard; enabling comparison of provider density, uninsurance rates, and poverty concentration across counties over time.

This project was built with direct relevance to my work at a Federally Qualified Health Center serving the Columbia River Gorge region, including one of Oregon's largest migrant and seasonal farmworker populations. FQHCs like mine operate in federally designated Health Professional Shortage Areas and Medically Underserved Areas; the same designations derived from AHRF data. Understanding county-level primary care supply relative to population directly informs where safety net providers are needed most.

---

## Architecture

```
HRSA AHRF CSVs
     │
     ▼
[Python Ingestion]         ingest_ahrf.py
CSV → Filter → Join → Parquet
     │
     ▼
[GCS Raw Layer]            gs://ahrf-raw-dy/raw/
ahrf_2022_2023.parquet
ahrf_2023_2024.parquet
ahrf_2024_2025.parquet
     │
     ▼
[BQ Load]                  load_bq.py
GCS Parquet → BigQuery ahrf_raw
     │
     ▼
[dbt Transformation]
stg_ahrf (view) → fact_primary_care_access (view)
     │
     ▼
[Looker Studio Dashboard]
Primary Care Access by County
```

Orchestrated by **Kestra** on an annual schedule (Jan 1).
Infrastructure provisioned by **Terraform**.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Infrastructure | Terraform + GCP |
| Storage | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Orchestration | Kestra |
| Transformation | dbt (BigQuery adapter) |
| Dashboard | Looker Studio |
| Language | Python 3.12 |

---

## Data Source

**HRSA Area Health Resources Files (AHRF)**
- Source: https://data.hrsa.gov/topics/health-workforce/ahrf
- Format: County-level CSV files split by category (HP, HF, POP)
- Release years used: 2022–2023, 2023–2024, 2024–2025
- 18 variables selected across provider supply, facility availability, and population/SDOH

> Raw data files are not committed to this repo. Download directly from HRSA and place in the project root following the directory structure in `ingestion/ingest_ahrf.py`.

---

## Repository Structure

```
dzc-capstone/
├── ingestion/
│   ├── ingest_ahrf.py       # CSV → Parquet → GCS
│   └── load_bq.py           # GCS Parquet → BigQuery
├── terraform/
│   ├── main.tf              # GCS bucket + BQ dataset
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars     # not committed
├── kestra/
│   └── docker-compose.yml   # Kestra + Postgres
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   └── stg_ahrf.sql
│   │   └── marts/
│   │       └── fact_primary_care_access.sql
│   └── dbt_project.yml
└── .env                     # not committed
```

---

## Setup & Reproduction

### Prerequisites

- GCP project with BigQuery and GCS enabled
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Docker + Docker Compose
- Python 3.12
- Terraform
- dbt-bigquery (installed via pipx)

### 1. Clone and configure

```bash
git clone https://github.com/youngrend/dzc-capstone.git
cd dzc-capstone
cp .env.example .env
# fill in GCP_PROJECT_ID and GCS_BUCKET_NAME
```

### 2. Provision infrastructure

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# fill in project_id
terraform init
terraform apply
```

### 3. Download AHRF data

Download the following releases from https://data.hrsa.gov/topics/health-workforce/ahrf and unzip into the project root:
- AHRF 2022–2023 CSV
- AHRF 2023–2024 CSV
- AHRF 2024–2025 CSV

### 4. Start Kestra

```bash
cd kestra
docker compose up -d
```

Open http://localhost:8080, create the `ahrf_ingest` flow from `kestra/flows/ahrf_ingest.yml`, and execute manually.

### 5. Run dbt

```bash
cd dbt
dbt run
```

### 6. Dashboard

Connect Looker Studio to BigQuery → `ahrf_raw.fact_primary_care_access`.

---

## Variables Selected

| Category | Variable | Description |
|---|---|---|
| HP | primary_care_mds | Primary care MDs |
| HP | primary_care_dos | Primary care DOs |
| HP | nurse_practitioners | Nurse practitioners |
| HP | physician_assistants | Physician assistants |
| HP | dentists_private_practice | Dentists in private practice |
| HP | psychiatrists | Psychiatrists |
| HF | fqhcs | Federally Qualified Health Centers |
| HF | community_health_centers | Community health centers |
| HF | short_term_gen_hospitals | Short-term general hospitals |
| HF | hospital_beds | Hospital beds |
| HF | critical_access_hospitals | Critical access hospitals |
| HF | rural_health_clinics | Rural health clinics |
| POP | population | Total population estimate |
| POP | population_65plus | Population 65+ |
| POP | pct_below_poverty | % below federal poverty level |
| POP | pct_uninsured_under65 | % uninsured under 65 |
| POP | median_household_income | Median household income |
| POP | unemployment_rate | Unemployment rate 16+ |

---

## Known Issues

- **GEO columns missing**: State name, county name, and FIPS component columns (`fips_st`, `fips_cnty`, `st_name`, `cnty_name`) did not resolve during ingestion due to column casing inconsistency in the 2024–2025 AHRF release. Only `fips_st_cnty` is available. Fix planned: update `resolve_columns()` in `ingest_ahrf.py` to handle the 2024–2025 casing variant, or join to a FIPS lookup table in dbt.
- **Python 3.14 incompatibility**: dbt-bigquery is not compatible with Python 3.14 as of this writing. Install via pipx with Python 3.12 (`pipx install dbt-bigquery --python python3.12`).
- **Kestra namespace files API**: Kestra's namespace files UI and API had issues on the version used. Workaround: bind-mount the project directory into the Kestra container and reference scripts by absolute path.

---

## Dashboard

https://datastudio.google.com/s/kquC1pw8cDc
