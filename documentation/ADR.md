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

## Decision outcome: Parquet

Parquet seems to be a solid option for the long-term, as it is open and flexible

### Lock-in/exit potential

Migrating away from parquet at a late stage would be difficult as it will be the core of our data storage. Although this would be the case for almost any file storage format. One way to mitigate this somewhat is to lean into the table-oriented character of parquet and to not store to many objects but to view each parquet file as a table with only text, numeric, date, etc. data. This would allow us to migrate the file storage to a plethora of other formats like CSV, JSON, etc. or even to most database table formats with relative ease.

# Data table format

## Context and problem statement

Ultimately we will be using tables to store and retrieve data. We need a system that provides us an easy way to store, modify and query tables. We could use a data lake, lakehouse or a DBMS.

## Considered options

### DuckLake
- pro: Lightweight, simple
- pro: Modular storage layer and catalog database
- pro: MIT License
- pro: Owned by DuckDB Foundation
- con: New, stable release in May 2026
### Apache Iceberg
### Delta lake
### Postgres
### Google Bigquery

## Decision outcome: DuckLake (preliminary)

We will move ahead with DuckLake as a preliminary option. It gives us the flexibility to store the index in a few databases (DuckDB, SQLite, Postgres) and it works with plain parquet files.

### Lock-in/exit potential

# Compute Platform
## Decision outcome: Surf Research Cloud (preliminary)

# Data Storage Platform
## Decision outcome: Surf Object Store (preliminary)

# Pipeline Orchestration
## Options
### Marimo notebooks
- pro: First class Python/SQL/DuckDB/DuckLake support
- pro: Focus on reproducibility
- pro: Easy migration
- pro: Apache License 2.0
### Jupyter notebooks
### Apache Airflow
### OpenMetadata
## Decision outcome: Marimo notebooks (preliminary)

# Pipeline Programming Language
## Options
### Python
- pro: Wide (academic) use
- pro: Wide library support
- pro: Easy onboarding
- pro: Python Software Foundation License

### DuckDB
- pro: Open source column-oriented DBMS
- pro: Very broad data storage format support
  - Parquet, JSON, CSV, XLSX, ...
  - Ducklake, HTTPFS, Iceberg, Delta
- pro: High performance (depending on compute)
- pro: MIT License
- pro: Owned by DuckDB Foundation

## Decision outcome: Python/DuckDB (preliminary)
