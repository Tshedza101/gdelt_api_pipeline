# gdelt_api_pipeline
a real-time, fault-tolerant ETL pipeline built to ingest global news data from the GDELT v2 API into a local columnar Data Lakehouse. 

# Real-Time GDELT Global Event Data Lakehouse Pipeline

An automated, fault-tolerant ETL pipeline built in Python to ingest, parse, and transform real-time global news event data from the **GDELT Project v2 API** into a local Parquet-based data lakehouse using **DuckDB** and **APScheduler**.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat&logo=python)
![DuckDB](https://img.shields.io/badge/DuckDB-0.9%2B-yellow?style=flat&logo=duckdb)
![Data Architecture](https://img.shields.io/badge/Architecture-Medallion-green)

---

## Architecture Overview

1. **Extraction**: The pipeline polls the GDELT v2 live manifest (`lastupdate.txt`) every 15 minutes to discover the latest compressed TSV batch export (`.export.CSV.zip`).
2. **Raw Storage (Landing)**: Downloads and extracts raw TSV payloads into `gdelt_lakehouse/raw/`.
3. **Ingestion & Transformation**: Uses **DuckDB** to execute high-performance string cleaning, strict column casting, null-padding, and custom escape handling directly on uncompressed feeds without relying on volatile automatic dialect sniffing.
4. **Processed Lakehouse (Bronze)**: Converts cleaned tabular data into columnar **Apache Parquet** files stored in `gdelt_lakehouse/processed/` for optimized analytical querying.

---

## Technical Features & Resilience

* **Schema Assurance**: Enforces strict GDELT v2 specifications across all 61 attributes (e.g., `GLOBALEVENTID`, `Actor1Code`, `GoldsteinScale`, `ActionGeo_Lat`, `SOURCEURL`).
* **Dialect Hardening**: Overrides DuckDB automatic CSV sniffing using explicit tab delimitation (`\t`), empty quoting parameters (`quote=''`), and error ignoring to gracefully handle unescaped HTML characters or malformed line endings in GDELT data.
* **Non-Blocking Scheduling**: Powered by `APScheduler` for 15-minute sync intervals, including an immediate trigger execution on startup.
* **Cross-Platform Path Hygiene**: Sanitized path handling compatible with Windows and Unix filesystems.

---

## Project Structure

```text
├── gdelt_lakehouse/
│   ├── raw/         # Temporary landing zone for raw GDELT TSV extracts
│   └── processed/   # Partitioned/stored analytical Parquet files
├── gdelt_api.py # Main orchestration script
├── requirements.txt # Project dependencies
├── .gitignore
└── README.md
RequirementsPython 3.9+Recommended: Virtual Environment (venv or conda)Quickstart1. Clone the RepositoryBashgit clone [https://github.com/your-username/gdelt-lakehouse-pipeline.git](https://github.com/your-username/gdelt-lakehouse-pipeline.git)
cd gdelt-lakehouse-pipeline
2. Set Up Virtual Environment & Install DependenciesBashpython -m venv gdelt_pipe
# On Windows:
gdelt_pipe\Scripts\activate
# On Linux/macOS:
source gdelt_pipe/bin/activate

pip install -r requirements.txt
3. Run the PipelineBashpython gdelt_api.py
Sample Data SchemaThe pipeline parses and standardizes raw feeds to the following 61-column GDELT v2 schema:Column CategoryFieldsIdentifiers & TimeGLOBALEVENTID, SQLDATE, MonthYear, Year, FractionDate, DATEADDEDActor DemographicsActor1Code, Actor1Name, Actor1CountryCode, Actor1Religion1Code, Actor1Type1Code (and corresponding Actor2 attributes)Event MetricsEventCode, QuadClass, GoldsteinScale, NumMentions, NumSources, AvgToneGeographyActor1Geo_FullName, Actor2Geo_FullName, ActionGeo_Lat, ActionGeo_Long
