# 🏥 Clinical Analytics & Knowledge Graph Lakehouse

An enterprise-grade, multi-tiered data lakehouse architecture designed for processing raw clinical lab reports (PDFs) into structured tabular analytics (Apache Iceberg) and semi-structured Knowledge Graphs (RDF Triplestore).

---

## 🏗️ 1. Data Pipeline Architecture

```text
                                  ┌───────────────────────────────┐
                                  │   Raw Lab Reports (PDFs)      │
                                  └───────────────┬───────────────┘
                                                  │
                                                  ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER: Object Storage (MinIO S3)                                                           │
│ Bucket: raw-lab-reports                                                                          │
└───────────────────────────────┬───────────────────────────────────┬──────────────────────────────┘
                                │                                   │
                                ▼                                   ▼
┌──────────────────────────────────────────┐     ┌─────────────────────────────────────────────────┐
│ SILVER LAYER: Staging State (PostgreSQL) │     │ GOLD GRAPH LAYER: Iceberg Triplestore           │
│ Database: document_lake                  │     │ Catalog: iceberg                                │
│ Table: staging_reports (JSONB)           │     │ Schema: knowledge_graph                         │
└────────────────────┬─────────────────────┘     │ Table: global_triplestore (Parquet)             │
                     │                           └─────────────────────────────────────────────────┘
                     ▼                                                     ▲
┌──────────────────────────────────────────┐                               │
│ GOLD TABULAR LAYER: Iceberg Lakehouse    │                               │
│ Catalog: iceberg                         │                               │
│ Schema: clinical_analytics               │                               │
│ Tables: patients, lab_observations       │───────────────────────────────┘
└──────────────────────────────────────────┘ (Serviced via Presto/Trino Query Engine)

```

---

## 🐳 2. Container & Service Infrastructure

```text
System Architecture Diagram
       [ Client / Host Browser ]
     :8080   :8181   :5432   :9000 / :9090
       │       │       │       │
┌──────┼───────┼───────┼───────┼──────────────────────────────────────────────────────────────┐
│      ▼       ▼       ▼       ▼                                                              │
│  ┌───────────────────────────────┐                                                          │
│  │    presto-network (bridge)    │                                                          │
│  └───────────────┬───────────────┘                                                          │
│                  │                                                                          │
│  ┌───────────────┼───────────────────────────────────────────────────────────────────────┐  │
│  │ SERVICES      │                                                                       │  │
│  │               ├──────────────► ┌───────────────────────────────────────────────────┐  │  │
│  │               │                │ presto-coordinator (SQL Query Engine)             │  │  │
│  │               │                └──────────────┬────────────────────────────────────┘  │  │
│  │               │                               │ Queries                               │  │
│  │               │                               ▼                                       │  │
│  │               ├──────────────► ┌───────────────────────────────────────────────────┐  │  │
│  │               │                │ rest (Iceberg REST Catalog)                       │  │  │
│  │               │                └──────────────┬────────────────────────────────────┘  │  │
│  │               │                               │ Metastore URI                         │  │
│  │               │                               ▼                                       │  │
│  │               ├──────────────► ┌───────────────────────────────────────────────────┐  │  │
│  │               │                │ postgres (Multi-Tenant State Engine)              │  │  │
│  │               │                │ ├── metastore_db (Catalog Metadata)               │  │  │
│  │               │                │ └── document_lake (Silver Staging / JSONB)        │  │  │
│  │               │                └───────────────────────────────────────────────────┘  │  │
│  │               │                                                                       │  │
│  │               ├──────────────► ┌───────────────────────────────────────────────────┐  │  │
│  │               │                │ minio (S3 Object Storage - Bronze & Gold Parquet) │  │  │
│  │               │                └──────────────▲────────────────────────────────────┘  │  │
│  │               │                               │ Init / Bucket                         │  │
│  │               ├──────────────► ┌──────────────┴────────────────────────────────────┐  │  │
│  │               │                │ mc (MinIO Client Init)                            │  │  │
│  │               │                └───────────────────────────────────────────────────┘  │  │
│  └───────────────┴───────────────────────────────────────────────────────────────────────┘  │
│                                                                                             │
│  VOLUMES: [ minio-data ]   [ catalog-data ]   [ postgres-data ]                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘

```

---

## 📊 3. Database & Table Schemas

### A. Silver Layer (PostgreSQL: `document_lake`)

* **Table:** `public.staging_reports`
* **Purpose:** Operational JSON document staging layer for state retention and change-data-capture.

| Column | Data Type | Key / Constraint | Description |
| --- | --- | --- | --- |
| `report_id` | `VARCHAR` | Primary Key | Unique lab report identifier (`LAB-XXXXX`) |
| `patient_id` | `VARCHAR` | Not Null | Patient MRN/UHID reference |
| `extracted_at` | `TIMESTAMP` | Default `NOW()` | Timestamp of document parsing execution |
| `s3_permanent_uri` | `VARCHAR` | Not Null | S3 URI pointing to Bronze raw PDF file |
| `raw_hierarchical_json` | `JSONB` | Not Null | Full parsed hierarchical JSON report representation |

---

### B. Gold Tabular Layer (Iceberg: `iceberg.clinical_analytics`)

#### **Table 1:** `patients`

* **Location:** `s3://warehouse/clinical_analytics/patients/`

| Column | Data Type | Description |
| --- | --- | --- |
| `patient_id` | `VARCHAR` | Unique Patient Identifier (UHID/MRN) |
| `patient_name` | `VARCHAR` | Patient Full Name |
| `gender` | `VARCHAR` | Gender Code (`M` / `F`) |
| `date_of_birth` | `DATE` | Patient Date of Birth |

#### **Table 2:** `lab_observations`

* **Location:** `s3://warehouse/clinical_analytics/lab_observations/`
* **Partitioning:** `ARRAY['loinc_code']`

| Column | Data Type | Description |
| --- | --- | --- |
| `report_id` | `VARCHAR` | Reference to Lab Report ID |
| `patient_id` | `VARCHAR` | Reference to Patient Identifier |
| `collected_datetime` | `TIMESTAMP` | Specimen collection timestamp |
| `test_name` | `VARCHAR` | Canonical lab test description |
| `loinc_code` | `VARCHAR` | Standardized LOINC metric mapping |
| `numeric_value` | `DOUBLE` | Observed metric value |
| `text_value` | `VARCHAR` | Qualitative metric observation |
| `units` | `VARCHAR` | Unit of measure (`gm%`, `10^3/µL`, etc.) |
| `reference_range_low` | `DOUBLE` | Bio-reference lower limit |
| `reference_range_high` | `DOUBLE` | Bio-reference upper limit |
| `bio_ref_interval` | `VARCHAR` | Raw reference interval text |
| `testing_method` | `VARCHAR` | Analytical measurement method |
| `interpretation` | `VARCHAR` | Diagnostic evaluation (`NORMAL` / `ABNORMAL`) |
| `interpretation_notes` | `VARCHAR` | Pipeline validation metadata |

---

### C. Gold Knowledge Graph Layer (Iceberg: `iceberg.knowledge_graph`)

* **Table:** `global_triplestore`
* **Purpose:** Schema-less RDF Triplestore model for graph-based queries and multi-modal entity linking.

| Column | Data Type | Description | Sample Node Value |
| --- | --- | --- | --- |
| `subject` | `VARCHAR` | Entity ID Node | `patient:PAT100982` / `visit:VIS99283` |
| `predicate` | `VARCHAR` | Directed Relationship Edge | `has_name`, `has_visit`, `has_observation` |
| `value` | `VARCHAR` | Connected Node ID or Literal | `visit:VIS99283` / `"HAEMOGLOBIN"` / `"13.5"` |

---

## 🛠️ 4. Prerequisites & System Requirements

* **Python:** `3.9+` (Tested on `3.11` / `3.12`)
* **Docker & Docker Compose:** MinIO, PostgreSQL, Iceberg REST Catalog, and Presto Coordinator.
* **Operating System:** macOS / Linux / WSL2

---

## 📦 5. Dependencies & Environment Setup

### 1. Initialize Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate

```

### 2. Install Required Packages

```bash
pip install --upgrade pip
pip install \
    boto3 \
    pdfplumber \
    pypdf \
    pandas \
    sqlalchemy \
    presto-python-client \
    trino \
    psycopg2-binary \
    pyvis \
    networkx

```

---

## 🚀 6. Execution Workflow

### Step 1: Upload Raw Files to Bronze Storage

Uploads local PDF reports from `historical_reports/` into the MinIO Bronze S3 bucket (`raw-lab-reports`):

```bash
python python-scripts/upload_to_minio.py

```

### Step 2: Run Tabular Lakehouse Pipeline (Silver & Gold)

Parses raw PDF objects directly from MinIO, stages raw JSONB into PostgreSQL `document_lake` (Silver), and commits tabular datasets into Iceberg `clinical_analytics` (Gold):

```bash
python python-scripts/global_upsert_pipeline_labreports.py

```

### Step 3: Run Knowledge Graph Pipeline

Parses raw PDF objects directly from MinIO and materializes Subject-Predicate-Value graph records into Iceberg `knowledge_graph.global_triplestore`:

```bash
python python-scripts/global_upsert_pipeline_triplets.py

```

### Step 4: Render Interactive Knowledge Graph Visualization

Generates an interactive HTML diagram (`knowledge_graph.html`) from the Iceberg triplestore:

```bash
python python-scripts/visualize_graph.py

```

---

## 🔍 7. Validation & Inspection Commands

### 1. Check Bronze Layer (MinIO Storage)

```bash
# Authenticate MinIO Client inside container
docker exec -it minio mc alias set local http://localhost:9000 minio minio123

# List files
docker exec -it minio mc ls local/raw-lab-reports/reports/

```

### 2. Check Silver Layer (PostgreSQL Staging)

```bash
docker exec -it postgres psql -U user -d document_lake -c "
SELECT 
    report_id, 
    patient_id, 
    extracted_at, 
    s3_permanent_uri 
FROM staging_reports;"

```

### 3. Check Gold Tabular Layer (Presto / Iceberg Analytics)

```bash
docker exec -it presto-coordinator presto-cli --server http://localhost:8080 --catalog iceberg --schema clinical_analytics --execute "
SELECT 
    report_id, 
    collected_datetime, 
    test_name, 
    loinc_code, 
    numeric_value, 
    units, 
    interpretation 
FROM lab_observations 
LIMIT 10;"

```

### 4. Check Gold Knowledge Graph Layer (Iceberg Triplestore)

```bash
docker exec -it presto-coordinator presto-cli --server http://localhost:8080 --catalog iceberg --schema knowledge_graph --execute "
SELECT subject, predicate, value 
FROM global_triplestore 
LIMIT 15;"

```

---

## 🧹 8. Troubleshooting & Common Fixes

| Issue / Error | Cause | Resolution |
| --- | --- | --- |
| `mc: Access Denied` | MinIO CLI client alias unauthenticated. | Run `docker exec -it minio mc alias set local http://localhost:9000 minio minio123`. |
| `NoSuchBucketException (404)` | Target bucket missing when Presto executes DDL. | Ensure `self.minio_client.create_bucket()` runs before Presto DDL queries. |
| `Catalog/Schema must be specified` | Query string lacks fully qualified catalog paths. | Always use `iceberg.schema.table` in SQLAlchemy/Trino query strings. |
| `f-string expression cannot include backslash` | Backslashes inside f-string brackets on Python <3.12. | Pre-sanitize single quotes (`.replace("'", "''")`) in an explicit local variable prior to string interpolation. |