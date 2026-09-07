import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import json
    import os
    from dotenv import load_dotenv
    load_dotenv()
    return mo, os


@app.cell
def _(os):
    from obstore.store import S3Store

    s3_broccoli_alpha = S3Store.from_url("s3://broccoli-alpha/",
        endpoint="https://objectstore.surf.nl",
        access_key_id=os.getenv('SURF_ACCESS_KEY_ID'),
        secret_access_key=os.getenv('SURF_SECRET_ACCESS_KEY')
    )

    s3_openalex = S3Store.from_url("s3://openalex/data/",
        skip_signature=True
    )
    return


@app.cell
def _(mo, os):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE SECRET surf (
            TYPE s3,
            PROVIDER config,
            ENDPOINT 'objectstore.surf.nl',
            KEY_ID '{os.getenv("SURF_ACCESS_KEY_ID")}',
            SECRET '{os.getenv("SURF_SECRET_ACCESS_KEY")}',
            REGION 'default',
            URL_STYLE 'path'
        );

        CREATE OR REPLACE SECRET openalex (
            TYPE s3, SCOPE 's3://openalex'
        );
        """,
        output=False
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE SECRET broccoli_ducklake (
            TYPE ducklake,
            METADATA_PATH 'broccoli.ducklake',
            DATA_PATH 's3://broccoli-alpha/lake/'
        );

        ATTACH 'ducklake:broccoli_ducklake' as db;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Ingestion
    """)
    return


@app.cell
def _():
    INGEST_PATH = "s3://broccoli-alpha/data/openalex/ingestion/works"
    return (INGEST_PATH,)


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS db.meta;
        CREATE TABLE IF NOT EXISTS db.meta.ingest_log (
            data_source VARCHAR,
            file VARCHAR,
            ingest_date DATETIME
        );
        """
    )
    return


@app.cell
def _(mo):
    PATHS = mo.sql(
        f"""
        SELECT file from glob('s3://openalex/data/parquet/works/*/*.parquet')
        EXCEPT
        SELECT file from db.meta.ingest_log
        WHERE data_source = 'openalex_parquet_public'
        """,
        output=False
    )
    return (PATHS,)


@app.cell
def _(INGEST_PATH, PATHS, mo):
    import random

    def oax_ingest(batch, batch_number):
        if len(batch) == 0: return
        mo.sql(f"""
            COPY (
                SELECT 
                    id, 
                    doi,
                    title,
                    publication_date, 
                    type,
                    open_access.oa_status AS oa_status
                FROM read_parquet({batch})
                WHERE list_contains(flatten(list_transform(authorships, a -> a.countries)), 'NL')
            ) TO '{INGEST_PATH}/{batch_number}_data.parquet' (FORMAT PARQUET);

            INSERT INTO db.meta.ingest_log
            SELECT 'openalex_parquet_public', unnest({batch}) as file, now() as ingest_date;
        """)

    batchsize = 10
    REMAINING = PATHS['path'].to_list()
    random.shuffle(REMAINING)
    for i in mo.status.progress_bar(range(0, len(REMAINING), batchsize), show_eta=True, show_rate=True):
        oax_ingest(REMAINING[i:i+batchsize], int((i/batchsize)+1))
    return


@app.cell
def _(mo):
    # Pull Dutch works (authorship institution country == NL) into a DuckDB table
    _df = mo.sql(
        f"""
        SELECT * FROM db.meta.ingest_log WHERE data_source = 'openalex_parquet_public'
        """
    )
    return


@app.cell(hide_code=True)
def _(INGEST_PATH, mo):
    _df = mo.sql(
        f"""
        SELECT count() from glob('{INGEST_PATH}/*_data.parquet');
        """
    )
    return


@app.cell(hide_code=True)
def hdr_bronze(mo):
    mo.md(r"""
    ## Bronze — raw, with schema & preliminary data checks
    """)
    return


@app.cell
def _(INGEST_PATH, mo):
    _counts = mo.sql(
        f"""
        SELECT
          count(*)                                         AS n_works,
          count(DISTINCT id)                               AS n_unique_works,
          count(*) FILTER (WHERE publication_date IS NULL) AS null_year,
          count(*) FILTER (WHERE oa_status IS NULL)        AS null_status,
          count(*) FILTER (WHERE type IS NULL)             AS null_type,
          min(year(publication_date))                      AS min_year,
          max(year(publication_date))                      AS max_year,
          count(DISTINCT year(publication_date))           AS n_years,
          count(DISTINCT oa_status)                        AS n_oa_types
        FROM read_parquet('{INGEST_PATH}/*_data.parquet')
        """
    )
    return


@app.cell
def _(INGEST_PATH, mo):
    _schema = mo.sql(
        f"""
        SELECT column_name, column_type
        FROM (DESCRIBE SELECT * FROM read_parquet('{INGEST_PATH}/*_data.parquet'))
        """
    )
    return


@app.cell
def _(INGEST_PATH, mo):
    _df = mo.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS db.openalex_bronze;

        CREATE OR REPLACE TABLE db.openalex_bronze.works AS
        SELECT * FROM read_parquet('{INGEST_PATH}/*_data.parquet')
        """
    )
    return


@app.cell(hide_code=True)
def hdr_silver(mo):
    mo.md(r"""
    ## Silver — cleaned & asserted
    """)
    return


@app.cell
def _(mo):
    silver_assert = mo.sql(
        f"""
        SELECT
          count()                            AS n_works,
          count(DISTINCT id)                  AS n_unique_ids,
          count() FILTER (WHERE doi IS NULL) AS null_doi,
          count() FILTER (WHERE publication_date IS NULL) AS null_date,
          count() FILTER (WHERE publication_date > today()) AS invalid_date,
          count() FILTER (WHERE oa_status NOT IN ('gold','hybrid','green','bronze','diamond','closed')) AS bad_status,
          count() FILTER (WHERE oa_status is NULL) AS oa_status_null,
          (count() = count(DISTINCT id))     AS ids_unique
        FROM db.openalex_bronze.works
        """
    )
    return (silver_assert,)


@app.cell
def _(silver_assert):
    # If any assertion fails, raise so the cell shows the problem clearly.
    def data_assert(assertion, message, critical=False):
        if not assertion and critical:
            print(f"ERROR: {message}")
            return False
        if not assertion and not critical:
            print(f"WARN:  {message}")
        return True

    checks = [
        data_assert(silver_assert["null_doi"][0] == 0, 
                    f"null doi present: {silver_assert["null_doi"][0]}",
                    critical=True),
        data_assert(silver_assert["null_date"][0] == 0, 
                    f"null publication_date present: {silver_assert["null_date"][0]}",
                    critical=False),
        data_assert(silver_assert["invalid_date"][0] == 0, 
                    f"invalid publication_date present: {silver_assert["invalid_date"][0]}",
                    critical=False),
        data_assert(silver_assert["bad_status"][0] == 0, 
                    f"unexpected oa_status values: {silver_assert["bad_status"][0]}",
                    critical=True),
        data_assert(silver_assert["oa_status_null"][0] == 0,
                    f"null oa_status present: {silver_assert["oa_status_null"][0]}",
                    critical=False),
        data_assert(bool(silver_assert["ids_unique"][0]),
                    f"duplicate ids in set",
                    critical=True)
    ]

    if False in checks:
        raise AssertionError("Pipeline cannot continue")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        -- Create the silver set
        CREATE SCHEMA IF NOT EXISTS db.openalex_silver;
        CREATE OR REPLACE TABLE db.openalex_silver.works AS
        SELECT
          replace(id, 'https://openalex.org/', '') as id,
          doi,
          title,
          year(publication_date) 		AS publication_year,
          publication_date,
          COALESCE(type, 'unknown')     AS type,
          COALESCE(oa_status, 'closed') AS oa_status,
          COALESCE(oa_status != 'closed', oa_status IS NOT NULL) AS is_oa
        FROM db.openalex_bronze.works
        WHERE id IS NOT NULL
          AND doi IS NOT NULL
          AND publication_date IS NOT NULL
          AND publication_date <= today();
        """
    )
    return


@app.cell
def _(mo):
    # Preview.
    _df = mo.sql(
        f"""
        SELECT publication_year::VARCHAR, oa_status, count(*) AS n 
        FROM db.openalex_silver.works
        GROUP BY 1,2 
        ORDER BY 1,2
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Gold — data ready for the dashboard
    """)
    return


@app.cell
def gold(mo):
    gold_oa = mo.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS db.openalex_gold;
        CREATE OR REPLACE TABLE db.openalex_gold.oa_status_per_year AS
        SELECT
          publication_year AS pub_year,
          oa_status,
          count(*) AS n_works
        FROM db.openalex_silver.works
        GROUP BY publication_year, oa_status
        ORDER BY publication_year, oa_status
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT * FROM db.openalex_gold.oa_status_per_year;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Chart — Dutch open-access output
    """)
    return


@app.cell
def _(mo):
    oa_by_year = mo.sql(
        f"""
        SELECT pub_year, oa_status, n_works
        FROM db.openalex_gold.oa_status_per_year
        WHERE pub_year BETWEEN 1990 and year(today())
        """,
        output=False
    )
    return (oa_by_year,)


@app.cell
def _():
    import altair as alt

    return (alt,)


@app.cell(hide_code=True)
def _(alt, oa_by_year):
    _chart = (
        alt.Chart(oa_by_year)
        .mark_bar()
        .encode(
            x=alt.X(field='pub_year', type='nominal'),
            y=alt.Y(field='n_works', type='quantitative', stack=True, aggregate='mean'),
            color=alt.Color(field='oa_status', type='nominal'),
            tooltip=[
                alt.Tooltip(field='pub_year', format=',.0f'),
                alt.Tooltip(field='n_works', aggregate='mean', format=',.0f'),
                alt.Tooltip(field='oa_status')
            ]
        )
        .properties(
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    _chart
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
