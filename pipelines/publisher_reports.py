import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
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
    return


@app.cell
def surf_secret(mo, os):
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
        """,
        output=False
    )
    return


@app.cell
def _():
    RAW_PATH    = "s3://broccoli-alpha/data/publisher_reports/raw"
    INGEST_PATH = "s3://broccoli-alpha/data/publisher_reports/ingest"
    BRONZE_PATH = "s3://broccoli-alpha/data/publisher_reports/bronze"
    SILVER_PATH = "s3://broccoli-alpha/data/publisher_reports/silver"
    GOLD_PATH   = "s3://broccoli-alpha/data/publisher_reports/gold"
    return BRONZE_PATH, INGEST_PATH, RAW_PATH, SILVER_PATH


@app.cell
def pub_config():
    # --- Publisher report read configuration -------------------------------
    # Each file has a different sheet name, header row (via `range`) and column names.
    # `key_col` is the column used to drop null-padded/blank rows from big ranges.
    PUB_CFG = {
        "BRI": dict(file="2020 BRI - Brill Open Access Article List 202012 dd20210119.xlsx",
                    sheet="Sheet1", range=None, key="DOI"),
        "CUP": dict(file="2020 CUP - UKB Read and Publish Report 2020 Complete.xlsx",
                    sheet="Articles", range="B10:AA9999", key="DOI"),
        "ELS": dict(file="2020 ELS - November 2020 Report.xlsx",
                    sheet="NOV2020", range=None, key="DOI"),
        "EME": dict(file="2020 EME - Emerald Open Access Article List 202009 dd20201023.xlsx",
                    sheet="Report", range=None, key="DOI"),
        "IOS": dict(file="2020 IOS - Open Access Article List 202012 dd20211008.xlsx",
                    sheet="ned-surf-2019", range=None, key="DOI"),
        "KAR": dict(file="2020 KAR - articles 2020 Jan-December - UKB Complete.xlsx",
                    sheet="NL", range="A2:Q9999", key="DOI"),
        "LWW": dict(file="2020 LWW- Dutch OA Published Article Data 2020.xlsx",
                    sheets=["Q1 2020","Q2 2020","Q3 2020","Q4 2020"], range="A5:G9999", key="Article Title"),
        "OUP": dict(file="2020 OUP - Oxford Open Access Article List 202012 dd20210104.xlsx",
                    sheet="Charges for 000082430564", range="A2:S9999", key="Doi"),
        "RSC": dict(file="2020 RSC - Open Access Article List 202012 dd20210111.xlsx",
                    sheet="Master", range=None, key="MANUSCRIPT_ID"),
        "SPR": dict(file="2020 SPR - Compact Report for UKB December 2020.xlsx",
                    sheet="OA Approved YTD2020 published", range="A5:V9999", key="DOI"),
        "TAF": dict(file="2020 TAF - Taylor Open Access Article List 202012 dd20210119.xlsx",
                    sheet="Sheet1", range=None, key="DOI"),
        "THI": dict(file="2020 THI - Thieme 2020 article List Dutch institutions.xlsx",
                    sheet="article list NL", range=None, key="Article DOI"),
    }

    # --- Unified bronze schema: map each publisher's columns onto this core ------
    BRONZE_FIELDS = ["doi","title","journal","author","institution","license",
                     "oa_status","publication_date","article_type"]
    # column per publisher per bronze field (None => NULL). Uses exact post-ingest column names.
    BRONZE_COL = {
        "BRI": {"doi":"DOI","title":"Article Title","journal":"Journal Title","author":"Author",
                "institution":"Institution","license":"license","oa_status":None,
                "publication_date":"publication date","article_type":"Article Type"},
        "CUP": {"doi":"DOI","title":"Article Title","journal":"Journal Title","author":"Affiliated Author",
                "institution":"GRID Name","license":"OA Licence Type","oa_status":"Current OA Status",
                "publication_date":"Online Publication Date","article_type":"Article Type"},
        "ELS": {"doi":"DOI","title":"full article name","journal":"Journal title","author":"authors name",
                "institution":"Institute Name","license":"license type","oa_status":"access type",
                "publication_date":"journey complete date","article_type":"article type"},
        "EME": {"doi":"DOI","title":"Article Title","journal":"Journal Name","author":"CorrespondingAuthor",
                "institution":"S1 Affiliation","license":"OA License Applied","oa_status":None,
                "publication_date":"Date Published Online","article_type":None},
        "IOS": {"doi":"DOI","title":"Title","journal":"journal","author":"__author__",
                "institution":"Institution","license":None,"oa_status":"Status",
                "publication_date":None,"article_type":"Type Article"},
        "KAR": {"doi":"DOI","title":"Article Title","journal":"Journal","author":"Corr. Author",
                "institution":"UKB Member Institution","license":"License","oa_status":"Journal Type (OA/Hybrid)",
                "publication_date":"Online Publication Date","article_type":"Article Type"},
        "LWW": {"doi":None,"title":"Article Title","journal":"Journal Name","author":"Corresponding Author",
                "institution":"Institution","license":None,"oa_status":"Journal Name",
                "publication_date":"Pub Date","article_type":None},
        "OUP": {"doi":"Doi","title":"Manuscript Title","journal":"Journal Name","author":None,
                "institution":None,"license":"Licence","oa_status":"Type",
                "publication_date":"Order Date","article_type":None},
        "RSC": {"doi":"RESOURCE_ID","title":"MANUSCRIPT_TITLE","journal":"PRODUCT NAME","author":"MASTER NAME",
                "institution":"RELATED INSTITUTION","license":"OA LICENCE TYPE","oa_status":"IS OA?",
                "publication_date":"E-ISSUE PUBLICATION DATE","article_type":"MANUSCRIPT_CODE"},
        "SPR": {"doi":"DOI","title":"Article Title","journal":"Journal Title","author":"Corresponding Author Name",
                "institution":"Institution","license":"Licence Type","oa_status":None,
                "publication_date":"Online Date","article_type":"Article Type"},
        "TAF": {"doi":"DOI","title":"Title","journal":"Journal title","author":"Author",
                "institution":"Institution","license":None,"oa_status":"OA Status on Taylor & Francis Online",
                "publication_date":"Date published online","article_type":"Manuscript Type"},
        "THI": {"doi":"Article DOI","title":"Article Title","journal":"Journal Title","author":"Author",
                "institution":"Affiliation Name","license":"Type of CC License","oa_status":"Open Access",
                "publication_date":"Online Publication Date (online)","article_type":None},
    }

    # IOS author = GivenName + SurName
    def qexpr(pub, col):
        """Return a SQL expression selecting `col` from publisher's ingest parquet."""
        if col is None: return "NULL"
        if col == "__author__":
            return '(coalesce("GivenName","") || ' ' || coalesce("SurName",""))'
        return f'"{col}"'


    return BRONZE_COL, BRONZE_FIELDS, PUB_CFG


@app.cell(hide_code=True)
def hdr_ingest(mo):
    mo.md(r"""
    ## Ingestion — raw capture (one parquet per publisher)
    """)
    return


@app.cell
def ingest(INGEST_PATH, PUB_CFG, RAW_PATH, mo):
    # Read each messy publisher workbook into a raw ingest parquet.
    # The large `range` pads rows with nulls; we keep only non-blank rows on the key column.
    _LWW_QTRS = ["Q1 2020","Q2 2020","Q3 2020","Q4 2020"]

    for pub, _cfg in PUB_CFG.items():
        _f = f"{RAW_PATH}/{_cfg['file']}"
        if pub == "LWW":
            _parts = " UNION ALL ".join(
                f"SELECT * FROM read_xlsx('{_f}', sheet='{q}', range='{_cfg['range']}', all_varchar=true)"
                for q in _cfg["sheets"]
            )
            _full = f"SELECT '{pub}' AS publisher, '{_cfg['file']}' AS source_file, * FROM ({_parts})"
            _key = '"Article Title"'
        else:
            _rng = f", range='{_cfg['range']}'" if _cfg["range"] else ""
            _full = (f"SELECT '{pub}' AS publisher, '{_cfg['file']}' AS source_file, * "
                     f"FROM read_xlsx('{_f}', sheet='{_cfg['sheet']}'{_rng}, all_varchar=true)")
            _key = f'"{_cfg["key"]}"'
        mo.sql(f"CREATE OR REPLACE TABLE pr_ingest_{pub} AS "
               f"{_full} WHERE {_key} IS NOT NULL AND TRIM({_key}) != ''", output=False)
        mo.sql(f"COPY pr_ingest_{pub} TO '{INGEST_PATH}/{pub}.parquet' (FORMAT PARQUET)", output=False)

    # Summary of ingest row counts (read back from S3 parquets).
    _ingest_counts = mo.sql(
        f"""
        SELECT replace(filename, '.parquet', '') AS source, count(*) AS n_rows
        FROM read_parquet('{INGEST_PATH}/*.parquet', filename=true)
        GROUP BY 1 ORDER BY 1
        """
    )
    return


@app.cell(hide_code=True)
def hdr_bronze(mo):
    mo.md(r"""
    ## Bronze — unified schema with checks
    """)
    return


@app.cell
def bronze(
    BRONZE_COL,
    BRONZE_FIELDS,
    BRONZE_PATH,
    INGEST_PATH,
    PUB_CFG,
    mo,
    pr_bronze,
):
    # Map each publisher's raw ingest onto a common bronze schema and union.
    # Missing fields per publisher are NULL (see BRONZE_COL config).
    def _q(_pub, _col):
        if _col is None: return "NULL"
        if _col == "__author__":
            # join GivenName + SurName; use single-quoted literals ("" would be a bad identifier)
            return f'(coalesce("GivenName", {chr(39)}{chr(39)}) || {chr(39)} {chr(39)} || coalesce("SurName", {chr(39)}{chr(39)}))'
        return f'"{_col}"'

    _uni = []
    for _pub in PUB_CFG:
        _cols = ", ".join(f"{_q(_pub, BRONZE_COL[_pub][fn])} AS {fn}" for fn in BRONZE_FIELDS)
        _uni.append(f"    SELECT '{_pub}' AS publisher, '{_pub}.parquet' AS source_file, {_cols}"
                    f"    FROM read_parquet('{INGEST_PATH}/{_pub}.parquet')")

    mo.sql(f"CREATE OR REPLACE TABLE pr_bronze AS " + " UNION ALL ".join(_uni), output=False)
    mo.sql(f"COPY pr_bronze TO '{BRONZE_PATH}/works.parquet' (FORMAT PARQUET)", output=False)

    # Bronze checks: total rows + row count per publisher.
    _bronze_checks = mo.sql(
        f"SELECT publisher, count(*) AS n_rows FROM read_parquet('{BRONZE_PATH}/works.parquet') GROUP BY publisher ORDER BY publisher"
    )
    return


@app.cell(hide_code=True)
def hdr_silver(mo):
    mo.md(r"""
    ## Silver — cleaned & asserted
    """)
    return


@app.cell
def silver(BRONZE_PATH, SILVER_PATH, mo):
    # Silver: clean & assert the unified bronze.
    # - drop rows with neither DOI nor title
    # - drop duplicate DOIs (case-insensitive), keeping the first occurrence
    # - doi-less rows are keyed by md5(publisher|source_file|title) so they are not clobbered
    # - normalize blank fields ("", " ") to NULL
    _clean = mo.sql(f"""
    CREATE OR REPLACE TABLE pr_silver AS
    WITH src AS (
        SELECT
            NULLIF(NULLIF(TRIM(doi), ''), ' ')               AS doi,
            NULLIF(NULLIF(TRIM(title), ''), ' ')             AS title,
            NULLIF(TRIM(journal), '')                        AS journal,
            NULLIF(TRIM(author), '')                         AS author,
            NULLIF(TRIM(institution), '')                    AS institution,
            NULLIF(NULLIF(TRIM(license), ''), ' ')           AS license,
            NULLIF(NULLIF(TRIM(oa_status), ''), ' ')         AS oa_status,
            NULLIF(TRIM(publication_date), '')               AS publication_date,
            NULLIF(TRIM(article_type), '')                   AS article_type,
            publisher, source_file
        FROM read_parquet('{BRONZE_PATH}/works.parquet')
        WHERE (doi IS NOT NULL AND TRIM(doi) != '') OR (title IS NOT NULL AND TRIM(title) != '')
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY CASE
                       WHEN doi IS NOT NULL AND doi != '' THEN LOWER(doi)
                       ELSE md5(COALESCE(publisher,'')||'|'||COALESCE(source_file,'')||'|'||COALESCE(title,''))
                   END
                   ORDER BY publisher, source_file
               ) AS rn
        FROM src
    )
    SELECT doi, title, journal, author, institution, license, oa_status,
           publication_date, article_type, publisher, source_file
    FROM ranked WHERE rn = 1
    """, output=False)

    # --- Assertions ---
    _assert = mo.sql(f"""
    SELECT
      count(*) AS n_works,
      count(*) FILTER (WHERE doi IS NOT NULL AND doi != '') AS n_with_doi,
      count(*) FILTER (WHERE (doi IS NULL OR doi = '') AND (title IS NULL OR title = '')) AS no_identifier,
      count(*) FILTER (WHERE title IS NULL AND (doi IS NOT NULL AND doi != '')) AS titleless_with_doi
    FROM pr_silver
    """)

    assert _assert["no_identifier"][0] == 0, f"rows lacking both doi and title: {_assert}"
    # among rows that have a doi, DOIs must be unique
    _dupes = mo.sql("""
    SELECT count(*) AS n FROM (
        SELECT LOWER(doi) d FROM pr_silver WHERE doi IS NOT NULL AND doi != ''
        GROUP BY LOWER(doi) HAVING count(*) > 1
    ) t
    """)
    assert _dupes["n"][0] == 0, f"duplicate dois remain: {_dupes}"

    # --- Persist silver ---
    mo.sql(f"COPY pr_silver TO '{SILVER_PATH}/works.parquet' (FORMAT PARQUET)", output=False)

    # --- Preview ---
    _silver_preview = mo.sql("SELECT publisher, count(*) AS n FROM pr_silver GROUP BY publisher ORDER BY publisher")
    return


if __name__ == "__main__":
    app.run()
