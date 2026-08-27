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
def _():
    INGEST_PATH = "s3://broccoli-alpha/data/openalex/ingestion/works"
    BRONZE_PATH = "s3://broccoli-alpha/data/openalex/bronze/works.parquet"
    SILVER_PATH = "s3://broccoli-alpha/data/openalex/silver/works.parquet"
    GOLD_PATH =   "s3://broccoli-alpha/data/openalex/gold/oa_by_year_type.parquet"
    return BRONZE_PATH, GOLD_PATH, INGEST_PATH, SILVER_PATH


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Ingestion
    """)
    return


@app.cell(disabled=True)
def _(INGEST_PATH, mo):
    _ = mo.sql(
        f"""
        COPY (select NULL as path limit 0) to '{INGEST_PATH}/ingest.parquet';
        """
    )
    return


@app.cell
def _(INGEST_PATH, mo):
    PATHS = mo.sql(
        f"""
        SELECT file as path from glob('s3://openalex/data/parquet/works/*/*.parquet')
        EXCEPT
        SELECT path from '{INGEST_PATH}/ingest.parquet'
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

            COPY (
                SELECT path FROM '{INGEST_PATH}/ingest.parquet'
                UNION
                SELECT unnest({batch}) as path
            ) to '{INGEST_PATH}/ingest.parquet';
        """)

    batchsize = 10
    REMAINING = PATHS['path'].to_list()
    random.shuffle(REMAINING)
    for i in mo.status.progress_bar(range(0, len(REMAINING), batchsize), show_eta=True, show_rate=True):
        oax_ingest(REMAINING[i:i+batchsize], int((i/batchsize)+1))
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        select filename, *
        from 's3://broccoli-alpha/data/openalex/ingestion/works/*.parquet'
        limit 10
        """
    )
    return


@app.cell(hide_code=True)
def _(INGEST_PATH, mo):
    # Pull Dutch works (authorship institution country == NL) into a DuckDB table
    _df = mo.sql(
        f"""
        SELECT year(publication_date), oa_status, count(*) AS n 
        FROM '{INGEST_PATH}/*.parquet'
        GROUP BY 1, 2 
        ORDER BY 1, 2
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
          count(*)                                      AS n_works,
          count(DISTINCT id)                            AS n_unique_works,
          count(*) FILTER (WHERE publication_year IS NULL) AS null_year,
          count(*) FILTER (WHERE oa_status IS NULL)        AS null_status,
          count(*) FILTER (WHERE type IS NULL)             AS null_type,
          min(publication_year)                         AS min_year,
          max(publication_year)                         AS max_year,
          count(DISTINCT publication_year)              AS n_years,
          count(DISTINCT oa_status)                     AS n_oa_types
        FROM read_parquet('{INGEST_PATH}/*.parquet')
        """
    )
    return


@app.cell
def _(INGEST_PATH, mo):
    _schema = mo.sql(
        f"""
        SELECT column_name, column_type
        FROM (DESCRIBE SELECT * FROM read_parquet('{INGEST_PATH}/*.parquet'))
        """
    )
    return


@app.cell
def _(BRONZE_PATH, INGEST_PATH, mo):
    # --- Store bronze (validated pass-through of the raw slice) ---
    _bronze_write = mo.sql(
        f"""
        COPY (SELECT * FROM read_parquet('{INGEST_PATH}/*.parquet'))
        TO '{BRONZE_PATH}' (FORMAT PARQUET, FILE_SIZE_BYTES )
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
def silver(BRONZE_PATH, mo):
    silver_assert = mo.sql(
        f"""
        -- Clean: drop rows missing core fields, keep unique work ids.
        CREATE OR REPLACE TABLE silver_works AS
        SELECT
          id,
          COALESCE(doi, '')             AS doi,
          COALESCE(title, '')           AS title,
          publication_year,
          COALESCE(publication_date, make_date(publication_year, 1, 1)) AS publication_date,
          COALESCE(type, 'unknown')     AS type,
          COALESCE(oa_status, 'closed') AS oa_status,
          COALESCE(is_oa, oa_status != 'closed') AS is_oa
        FROM read_parquet('{BRONZE_PATH}')
        WHERE id IS NOT NULL
          AND publication_year IS NOT NULL;

        -- Drop duplicate work ids, keeping the first occurrence.
        DELETE FROM silver_works
        WHERE rowid NOT IN (
            SELECT min(rowid) FROM silver_works GROUP BY id
        );

        SELECT
          count(*)                            AS n_works,
          count(DISTINCT id)                  AS n_unique_ids,
          count(*) FILTER (WHERE publication_year IS NULL) AS null_year,
          count(*) FILTER (WHERE oa_status NOT IN ('gold','hybrid','green','bronze','diamond','closed')) AS bad_status,
          count(*) FILTER (WHERE is_oa AND oa_status = 'closed') AS oa_closed_contradiction,
          (count(*) = count(DISTINCT id))     AS ids_unique
        FROM silver_works
        """
    )
    return (silver_assert,)


@app.cell
def _(silver_assert):
    # If any assertion fails, raise so the cell shows the problem clearly.
    assert silver_assert["null_year"][0] == 0, f"null publication_year present: {silver_assert}"
    assert silver_assert["bad_status"][0] == 0, f"unexpected oa_status values: {silver_assert}"
    assert silver_assert["oa_closed_contradiction"][0] == 0, f"is_oa but oa_status=closed: {silver_assert}"
    assert bool(silver_assert["ids_unique"][0]), f"duplicate ids in silver: {silver_assert}"
    return


@app.cell
def _(SILVER_PATH, mo, silver_works):
    # Persist silver.
    _df = mo.sql(
        f"""
        COPY silver_works TO '{SILVER_PATH}' (FORMAT PARQUET)
        """
    )
    return


@app.cell
def _(mo, silver_works):
    # Preview.
    _df = mo.sql(
        f"""
        SELECT publication_year, oa_status, count(*) AS n FROM silver_works GROUP BY 1,2 ORDER BY 1,2
        """
    )
    return


@app.cell(hide_code=True)
def hdr_gold(mo):
    mo.md(r"""
    ## Gold — data ready for the dashboard
    """)
    return


@app.cell
def gold(SILVER_PATH, mo):
    gold_oa = mo.sql(
        f"""
        SELECT
          publication_year AS year,
          oa_status AS oa_type,
          count(*) AS n_works
        FROM read_parquet('{SILVER_PATH}')
        GROUP BY publication_year, oa_status
        ORDER BY publication_year, oa_status
        """
    )
    return (gold_oa,)


@app.cell
def _(GOLD_PATH, gold_oa, mo):
    _df = mo.sql(
        f"""
        COPY gold_oa TO '{GOLD_PATH}' (FORMAT PARQUET)
        """
    )
    return


@app.cell
def _(gold_oa):
    # Show the table so the dashboard data is visible in the notebook.
    gold_oa
    return


@app.cell(hide_code=True)
def hdr_chart(mo):
    mo.md(r"""
    ## Chart — Dutch open-access output
    """)
    return


@app.cell
def _(GOLD_PATH, mo):
    oa_by_year = mo.sql(
        f"""
        SELECT * FROM read_parquet('{GOLD_PATH}') ORDER BY year, oa_type
        """
    )
    return (oa_by_year,)


@app.cell
def chart(mo, oa_by_year):
    # Single chart: open-access publications from the Netherlands, per year, by OA type.
    import polars as pl
    import plotly.graph_objects as go

    # Build one stacked bar trace per OA type.
    oa_fig = go.Figure()
    for oa_type in sorted(oa_by_year["oa_type"].unique().to_list()):
        sub = oa_by_year.filter(pl.col("oa_type") == oa_type)
        oa_fig.add_trace(go.Bar(
            x=sub["year"].to_list(), y=sub["n_works"].to_list(), name=oa_type,
        ))
    oa_fig.update_layout(
        barmode="stack",
        xaxis_title="Publication year",
        yaxis_title="# publications",
        title="Open-access publications from the Netherlands (demo slice)",
    )

    mo.ui.plotly(oa_fig)
    return


if __name__ == "__main__":
    app.run()
