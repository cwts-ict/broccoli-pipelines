---
title: Broccoli Architecture Decision record
template: >-
    # {Use case}
    ## Context and problem statement
    ## Considered options
    ### {option}
    - pro: {pro}
    - con: {con}
    ## Decision outcome: {outcome}
    ### Lock-in/exit potential
---

# Data storage file format

## Context and problem statement

We need some kind of file format to store our data in.

## Considered options

### Parquet

- pro: Apache License 2.0
- pro: Good compression
- pro: Partial reading
- pro: Widely used in data lake and data lakehouse architectures

## Decision outcome: Parquet (preliminary)

Parquet is a solid option for the long-term, as it is open and flexible.

### Lock-in/exit potential

Migrating away from parquet at a late stage would be difficult as it will be the core of our data storage, although this would be the case for almost any file storage format. We mitigate this by keeping parquet strictly table-oriented: columns of text, numeric, date, boolean, etc. no deeply nested structs or serialised objects. Migration to CSV, JSON, or most database table formats then stays straightforward.

# Data table format

## Context and problem statement

Ultimately we will be using tables to store and retrieve data. We need a system that provides us an easy way to store, modify and query tables. We could use a data lake, lakehouse or a DBMS.

## Considered options

### DuckLake
- pro: Lightweight, open, simple
- pro: Modular storage layer and catalog database
- pro: MIT License
- pro: Owned by DuckDB Foundation
- con: New, stable release in May 2026
### Apache Iceberg
- pro: Industry-standard open table format, broad ecosystem support
- pro: Mature, battle-tested at large scale
- con: Heavyweight; requires additional catalog infrastructure (e.g. Nessie, Hive Metastore, Polar)
- con: Designed for large teams and concurrent writers, more operational complexity than we need
### Delta Lake
- pro: Mature, widely adopted (Databricks ecosystem)
- pro: ACID transactions, time travel
- con: Tightly associated with the Databricks/Spark ecosystem
- con: More moving parts than needed for our scale
### Postgres
- pro: Battle-tested, mature, excellent tooling and ecosystem
- pro: Full SQL with strong consistency
- con: Operational burden (hosting, backups, upgrades) on us
### Google BigQuery
- pro: Fully managed, little operational burden
- pro: Excellent analytical performance at scale
- con: Vendor lock-in to Google Cloud
- con: Recurring cost scales with usage; incompatible with on-prem/SURF hosting plans
- con: Closed source
- con: Dependence on US infrastructure
### Plain DuckDB database
- pro: Simplest possible setup; a single `.duckdb` file per database
- pro: No catalog infrastructure at all
- con: Single-writer only; poor fit for concurrent pipelines
- con: No built-in table versioning or snapshotting

## Decision outcome: DuckLake (preliminary)

We will move ahead with DuckLake as a preliminary option. It gives us the flexibility to store the index in a few databases (For now DuckDB, might need to switch to Postgres for concurrent writes) and it works with plain parquet files.

Chosen over Iceberg and Delta Lake because those bring catalog infrastructure and operational complexity that is disproportionate to our scale; over plain DuckDB files because we do want catalog-managed versioning and multi-writer support without losing the open parquet storage layer; over Postgres/BigQuery because we prefer data in open files rather than inside a managed DBMS.

# Compute Platform

## Context and problem statement

Where do the pipelines run? We need compute for ingestion jobs, transformations and (periodic) full pipeline runs, without significant operational overhead.

## Considered options

### SURF Research Cloud
- pro: Dutch academic infrastructure
- pro: European/Dutch jurisdiction
- pro: No procurement or payment beyond existing SURF membership
- pro: Simple workspace/catalog model suited to project-sized deployments
- con: Smaller ecosystem and fewer managed services than the big clouds
### Generic cloud providers (AWS, Azure, GCP)
- pro: Vast managed-service ecosystem (managed Airflow, object storage, serverless)
- pro: Effectively unlimited elasticity and global availability
- con: Pay-per-use costs are hard to predict and limit
- con: Vendor lock-in through proprietary managed services
- con: Data sovereignty and GDPR/DPA overhead

## Decision outcome: SURF Research Cloud (preliminary)

SURF Research Cloud is the working choice: cost-effective, sovereign, and friction-free within existing SURF membership. The pipeline stack (marimo + Python + DuckDB) is deliberately cloud-agnostic, so moving to a generic provider later is a deployment change, not a rewrite.

### Lock-in/exit potential

Low. Nothing in the stack depends on Research Cloud-specific services; the platform provides VMs and storage, which are commodities. Exit is mainly an operational (redeployment) effort.

# Data Storage Platform

## Context and problem statement

Where do the parquet files and DuckLake catalog live? We need durable, shared storage accessible from the compute platform, without significant operational overhead.

## Considered options

### SURF Object Store
- pro: S3-compatible, so any S3-speaking tool (DuckDB HTTPFS, DuckLake) works directly
- pro: Dutch academic infrastructure
- pro: Data sovereignty
- pro: Pairs naturally with SURF Research Cloud compute
- con: Smaller ecosystem
- con: Performance and SLA guarantees weaker than hyperscalers

### Generic cloud object stores (AWS S3, GCS, Azure Blob)
- pro: Best-in-class durability, performance and ecosystem integration
- pro: Rich surrounding services (lifecycle policies, event triggers, query services)
- con: Egress and request costs add up unpredictably
- con: Vendor lock-in risk is low for plain S3 but high for surrounding services
- con: Data sovereignty and GDPR/DPA overhead; procurement process required

## Decision outcome: SURF Object Store (preliminary)

SURF Object Store is the working choice, primarily because it is S3-compatible, the de facto interchange standard for object storage, while keeping data sovereign and co-located with SURF Research Cloud compute.

### Lock-in/exit potential

Low to moderate. Because the store is S3-compatible and only holds plain parquet files plus a DuckLake catalog, migrating to AWS S3, GCS, Azure Blob or any other S3-compatible store is a bulk copy plus a catalog connection string change.

# Pipeline Orchestration

## Context and problem statement

How do pipelines get scheduled, triggered and monitored? Pipelines must run reproducibly and periodically without manual intervention, and failures must be visible.

## Options

### Marimo notebooks
- pro: First class Python/SQL/DuckDB/DuckLake support
- pro: Focus on reproducibility
- pro: Easy migration
- pro: Apache License 2.0
- con: Not designed as a pipeline unit, scheduling/orchestration has to come from elsewhere
### Jupyter notebooks
- pro: Ubiquitous; every data person knows it
- pro: Huge extension ecosystem
- con: Hidden state and out-of-order execution undermine reproducibility
- con: Not designed as a pipeline unit, scheduling/orchestration has to come from elsewhere
### Apache Airflow
- pro: Industry-standard scheduler; mature, huge community
- pro: First-class DAG definition, retries, backfills, monitoring UI
- con: Heavy operational footprint (webserver, scheduler, workers, metadata DB)
- con: DAG code and business logic tend to diverge; pipeline logic lives inside the scheduler's conventions
- con: Task-oriented DAG model rather than data-oriented; lineage and dataset dependencies are second-class
### OpenMetadata
- pro: Combines orchestration with metadata/lineage/data quality, one platform for governance
- pro: Native support for many workflow engines and connectors
- con: Ambitious scope: running it well means operating a metadata platform, not just a scheduler
- con: Orchestration features are younger and less battle-tested than Airflow's
- con: Java/elastic stack underneath adds operational weight
### Dagster
- pro: Modern orchestration with strong asset/data-aware model (fits table-oriented thinking)
- pro: Excellent local development experience and testing story
- con: Smaller community than Airflow; opinionated concepts to learn
- con: Another Python framework to operate
### Prefect
- pro: Lightweight, Pythonic; low ceremony to get started
- pro: Hybrid execution model works well for small teams
- con: Key features tied to Prefect Cloud; self-hosted server is less polished
- con: Less suited to complex dependency graphs than Airflow/Dagster
### Plain cron / systemd timers
- pro: Zero infrastructure; good enough for a handful of nightly jobs
- con: No dependency management, retries, visibility, or history
- con: Does not scale past the trivial-pipeline stage

## Decision outcome: Marimo notebooks (preliminary)

For the current demo stage, marimo notebooks serve as the pipeline unit of execution: each notebook is a runnable, schedulable artifact with built-in reproducibility (no hidden state) and native DuckDB/DuckLake support, with a lightweight external trigger (cron or similar) for scheduling. This is a pragmatic choice for exploration, not a settled architectural position.

On their own merits, Airflow and OpenMetadata are the serious contenders for a production orchestrator: Airflow for mature scheduling at modest operational cost, OpenMetadata if metadata management and lineage become first-class requirements rather than nice-to-haves. Dagster's asset-oriented model also fits the table-oriented data architecture well.

### Lock-in/exit potential

Low. Marimo notebooks are plain Python files, versionable, importable and executable outside marimo, so pipeline logic can be lifted into any orchestrator without rewriting.

# Pipeline Programming Language

## Context and problem statement

In which language/environment do we implement the pipelines that ingest, transform and serve our data?

## Options

### Python
- pro: Wide (academic) use
- pro: Wide library support
- pro: Easy onboarding
- pro: Python Software Foundation License
### R
- pro: Strong presence in bibliometrics/scientometrics research
- pro: Familiar to much of the target (research) audience
- pro: Good reproducibility tooling (renv)
- con: Weaker fit with the DuckDB/DuckLake tooling ecosystem than Python
- con: Smaller general-purpose library ecosystem for API ingestion and pipeline engineering
### dbt (with dlt as ingestion layer)
- pro: Declarative, SQL-first transformations; lineage and testing built in
- pro: dlt handles the messy ingestion side (APIs, pagination, incremental loads)
- pro: Well-established pairing; large community and documentation
- con: Adds framework machinery (project structure, manifests, run orchestration)
- con: Python still needed alongside for custom logic
### SQLMesh
- pro: SQL-first transformations like dbt, with virtual/environments and true data diffing (no full rebuilds to validate changes)
- pro: First-class DuckDB/DuckLake support; engine-agnostic SQL transpilation reduces engine lock-in
- pro: Built-in incremental model evaluation and column-level lineage
- con: Smaller community and ecosystem than dbt
- con: Adds framework machinery alongside Python, same as dbt
### Bash / CLI tooling
- pro: Zero framework overhead; every tool is a subprocess
- con: No typing, testing, or dependency management
### DuckDB (as processing engine)
- pro: Open source column-oriented DBMS
- pro: Very broad data storage format support
  - Parquet, JSON, CSV, XLSX, ...
  - Ducklake, HTTPFS, Iceberg, Delta
- pro: Also suitable for some source ingestion
- pro: High performance (depending on compute)
- pro: MIT License
- pro: Owned by DuckDB Foundation

## Decision outcome: Python/DuckDB (preliminary)

Python is a given as the pipeline language; DuckDB is the data processing engine within it. The intended division of labour:

- **Python**: Orchestration, API ingestion, glue code
- **DuckDB**: All table-level processing, columnar format ingestion, reading/writing parquet and DuckLake
- **marimo**: Bridge: notebooks that embed Python and SQL/DuckDB cells

dbt+dlt and SQLMesh remain options to test as alternative transformation layers instead of or on top of DuckDB; either could replace hand-written Python transformation code without changing the storage or engine decisions. SQLMesh is maybe the more interesting of the two for this stack because its virtual environments and data diffing align well with the snapshot/versioning approach in the data layer.

### Lock-in/exit potential

Python carries no real lock-in.

DuckDB does carry some some lock-in: it is a single implementation with single-writer limitations, and much of its appeal lies in SQL extensions beyond ANSI (its own syntax, functions and integration with DuckLake/HTTPFS). Migrating away would mean rewriting that bespoke SQL for another engine. This is mitigated partly by keeping DuckDB as an engine rather than a storage format.

# Dashboard / Application Layer

## Context and problem statement

Research outputs (reports, indicators, exploratory views) need to reach users, researchers, librarians, management, as something interactive or at least browsable. We need to decide how dashboards/applications are built and served, on top of the DuckLake data layer. Requirements are expected to include: low-friction development by data people (not frontend engineers), interactivity (filtering, drill-down), and sharing within the institute and possibly with external partners.

## Considered options

### marimo apps / marimo hub
- pro: Same tool for exploration and delivery; notebooks already run as read-only interactive apps
- pro: Reactive execution model; Altair/Plotly render natively
- pro: No additional framework to learn or maintain
- con: Marimo apps have limited styling/layout control; looks like a notebook, not a product, possibly solved by marimo hub
- con: No user authentication/authorisation
### Streamlit
- pro: The de facto Python quick-dashboard standard; huge community, fast iteration
- pro: Simple auth and deployment options; many hosting options documented
- con: Rerun-the-script execution model is inefficient for data-heavy apps
- con: Generic Layout/styling
### Shiny for Python
- pro: Reactive programming model well suited to linked, filter-driven dashboards
- con: Smaller Python ecosystem than Streamlit
### Plotly Dash
- pro: Production-grade; fine-grained layout control; enterprise-friendly
- con: Callback boilerplate, slower to build, more code per interaction
### Panel / HoloViz
- pro: Flexible; supports multiple plotting libraries; good for data-app dashboards
- con: Smaller community, API surface larger than needed here
### BI / dashboarding platforms (Apache Superset, Metabase, Grafana, Lightdash)
- pro: No-code/low-code dashboard building; fast for standard charts and tables
- pro: Built-in auth, permissions, sharing and scheduling
- pro: Superset is Apache-licensed and self-hostable; Metabase/Lightdash also open source options
- pro: SQL Lab / SQL IDE features encourage reuse of curated queries over the DuckLake layer
- con: Dashboards are constrained to the tool's paradigm; custom analysis flows don't fit
- con: Operational footprint: Superset needs a metadata DB, cache and a web service running
- con: Generic BI look and feel
### Static reports (Quarto / exported notebooks)
- pro: Zero server; trivially shareable and archivable; reproducible by construction
- pro: Good fit for periodic reports (e.g. publisher reports)
- con: Not interactive beyond what HTML widgets allow; no live filtering against the data
### Custom web app (React/Next.js + API)
- pro: Full control over UX, auth, branding; scales to a real product
- con: Requires frontend engineering capacity
- con: Much higher development and maintenance cost

## Decision outcome: marimo apps (preliminary)

For the demo stage, marimo apps are a pragmatic choice: they reuse the existing stack and its DuckDB/DuckLake integration, so a dashboard is essentially a curated notebook over the data layer.

If dashboards become a product with real users, the serious contenders to revisit are Streamlit (fastest path to a polished app) and Shiny for Python (best reactive model for filter-driven research dashboards). A BI platform such as Superset is the contender if self-service (users building their own views and charts against the data layer) becomes a requirement, since auth/permissions and dashboard authoring come out of the box. A custom web app might be a hard requirement for the UKBsis successor application.

### Lock-in/exit potential

Low. The valuable logic lives in the data layer, not in the dashboard framework. Chart code (Altair/Plotly specs) is portable across all Python dashboard options, so switching frameworks is a rewrite of the presentation layer only.
