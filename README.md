# snowflake-tpch-analytics


This project is a hands-on exploration of Snowflake as a **modern cloud data platform**, covering database setup, security, data pipelines transformations, data quality, and Snowpark-based analytics.

The goal is to understand Snowflake **beyond SQL**, from architecture and governance to programmatic data processing.

---

## Project Objectives

* Understand Snowflake architecture beyond basic querying
* Design a layered data platform using **Medallion Architecture**
* Build ingestion, transformation, and analytics pipelines natively in Snowflake
* Apply **data quality**, **security**, and **governance** best practices
* Explore **Snowpark Python** for programmatic data processing

---

## Architecture Overview

**Architecture pattern:** Medallion (Staging → Silver → Gold)

* **Staging layer**: raw ingestion and schema alignment
* **Silver layer**: cleaned, conformed, business-ready data
* **Gold layer**: analytics- and reporting-focused tables

**Approach:**

* ELT using Snowflake compute
* Clear separation between ingestion, transformation, analytics, and governance
* Modular, script-based implementation for reproducibility



![Diagrams](./e.excalidraw.svg)

---

## Repository Structure

```bash
.
├── 01_db_setup
│   ├── 0101_setup_db_n_rbac.sql
│   ├── 0102_layer_staging.sql
│   └── 01_test.ipynb
├── 02_architecture_n_pipeline
│   ├── 0201_layer_silver_n_gold.sql
│   ├── 0202_pipe.sql
│   ├── 0203_stream.sql
│   ├── 0204_sp_analytics.sql
│   ├── 0205_sp_reports.sql
│   └── 02_test.ipynb
├── 03_quality_check
│   ├── 03_data_quality_check.sql
│   └── 03_test.ipynb
├── 04_infosec
│   ├── 0401_masking_policies.sql
│   ├── 0402_data_sharing.sql
│   └── 04_test.ipynb
├── 05_snowspark_udfs
│   ├── 0501_snowpark.py
│   ├── 0502_udfs.sql
│   ├── 05_test.ipynb
│   ├── connections.toml
│   └── environment.yml
└── README.md
```

Each module includes SQL scripts and/or notebooks used to validate logic and results.

---

## Result

- Video Link: TBU
- See the result temporarily in pictures captured in each section (aka each folder)

---

## Snowflake Features Explored

This project covers a wide range of Snowflake platform features:

* Virtual Warehouses & compute–storage separation
* Role-Based Access Control (RBAC)
* Medallion architecture implementation
* Snowpipe for continuous data ingestion
* Streams for incremental data processing
* Stored Procedures (SQL & Snowpark)
* Snowpark Python API
* User-Defined Functions (UDFs)
* Data masking policies for sensitive data
* Secure data sharing

---

## Testing & Validation

* Each layer is validated using:

    * Row counts and null checks
    * Business rule verification
    * Sample data inspection via notebooks
    * SQL logic is tested incrementally before promotion to downstream layers

---

## Security & Governance Focus

Security and governance are treated as **first-class concerns**, not afterthoughts:

* RBAC applied at database, schema, and object levels
* Column-level masking for sensitive attributes
* Controlled access patterns for analytics and reporting
* Secure data sharing configuration for external consumers


---

## Key Learnings

* Snowflake should be treated as a **data platform**, not only a SQL engine
* Native Snowflake features can replace many external tools when used correctly
* Governance and security must be designed early in the data lifecycle
* Snowpark enables advanced transformations while keeping compute inside Snowflake
* Clear layering and modular design improve maintainability and scalability

---

### Improvements


- **Event-driven ingestion with Snowpipe**
  
  Enhance the ingestion architecture by configuring **Snowpipe with Google Cloud Pub/Sub** to enable fully event-driven, near–real-time data loading from GCS, replacing the current task-based ingestion setup.

- **CI/CD & version control for data pipelines**
  
  Introduce a **CI/CD workflow** to manage versioning and deployment of SQL scripts, Snowpark code, and infrastructure objects (tasks, streams, procedures).  
  This includes automated testing, environment promotion (dev → prod), and controlled releases for data pipelines.



---
### Resources
- Learn more about Git Integration [in the link](https://www.youtube.com/watch?v=4GOa1eUccmQ&t=27s)
