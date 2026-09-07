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
def hdr_ingest(mo):
    mo.md(r"""
    ## Ingestion
    """)
    return


@app.cell
def _():
    RAW_PATH    = "s3://broccoli-alpha/data/publisher_reports/raw"
    INGEST_PATH = "s3://broccoli-alpha/data/publisher_reports/ingest"
    return INGEST_PATH, RAW_PATH


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
def ingest_bri(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Brill 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'BRI' AS publisher,
                   '2020 BRI - Brill Open Access Article List 202012 dd20210119.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 BRI - Brill Open Access Article List 202012 dd20210119.xlsx',
                           sheet = 'Sheet1',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/BRI.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/BRI.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_cup(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Cambridge University Press 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'CUP' AS publisher,
                   '2020 CUP - UKB Read and Publish Report 2020 Complete.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 CUP - UKB Read and Publish Report 2020 Complete.xlsx',
                           sheet = 'Articles', range = 'B10:AA9999',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/CUP.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/CUP.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_els(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Elsevier November 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'ELS' AS publisher,
                   '2020 ELS - November 2020 Report.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 ELS - November 2020 Report.xlsx',
                           sheet = 'NOV2020',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/ELS.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/ELS.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_eme(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Emerald 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'EME' AS publisher,
                   '2020 EME - Emerald Open Access Article List 202009 dd20201023.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 EME - Emerald Open Access Article List 202009 dd20201023.xlsx',
                           sheet = 'Report',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/EME.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/EME.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_ios(INGEST_PATH, RAW_PATH, mo):
    # Ingest: IOS Press 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'IOS' AS publisher,
                   '2020 IOS - Open Access Article List 202012 dd20211008.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 IOS - Open Access Article List 202012 dd20211008.xlsx',
                           sheet = 'ned-surf-2019',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/IOS.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/IOS.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_kar(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Karger 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'KAR' AS publisher,
                   '2020 KAR - articles 2020 Jan-December - UKB Complete.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 KAR - articles 2020 Jan-December - UKB Complete.xlsx',
                           sheet = 'NL', range = 'A2:Q9999',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/KAR.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/KAR.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_lww(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Lippincott Williams & Wilkins 2020 report (4 quarter sheets) -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'LWW' AS publisher,
                   '2020 LWW- Dutch OA Published Article Data 2020.xlsx' AS source_file,
                   *
            FROM (
                SELECT * FROM read_xlsx('{RAW_PATH}/2020 LWW- Dutch OA Published Article Data 2020.xlsx',
                                        sheet = 'Q1 2020', range = 'A5:G9999', all_varchar = true)
                UNION ALL
                SELECT * FROM read_xlsx('{RAW_PATH}/2020 LWW- Dutch OA Published Article Data 2020.xlsx',
                                        sheet = 'Q2 2020', range = 'A5:G9999', all_varchar = true)
                UNION ALL
                SELECT * FROM read_xlsx('{RAW_PATH}/2020 LWW- Dutch OA Published Article Data 2020.xlsx',
                                        sheet = 'Q3 2020', range = 'A5:G9999', all_varchar = true)
                UNION ALL
                SELECT * FROM read_xlsx('{RAW_PATH}/2020 LWW- Dutch OA Published Article Data 2020.xlsx',
                                        sheet = 'Q4 2020', range = 'A5:G9999', all_varchar = true)
            ) src
            WHERE "Article Title" IS NOT NULL AND TRIM("Article Title") != ''
        ) TO '{INGEST_PATH}/LWW.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/LWW.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_oup(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Oxford University Press 2020 charges report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'OUP' AS publisher,
                   '2020 OUP - Oxford Open Access Article List 202012 dd20210104.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 OUP - Oxford Open Access Article List 202012 dd20210104.xlsx',
                           sheet = 'Charges for 000082430564', range = 'A2:S9999',
                           all_varchar = true)
            WHERE "Doi" IS NOT NULL AND TRIM("Doi") != ''
        ) TO '{INGEST_PATH}/OUP.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/OUP.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_rsc(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Royal Society of Chemistry 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'RSC' AS publisher,
                   '2020 RSC - Open Access Article List 202012 dd20210111.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 RSC - Open Access Article List 202012 dd20210111.xlsx',
                           sheet = 'Master',
                           all_varchar = true)
            WHERE "MANUSCRIPT_ID" IS NOT NULL AND TRIM("MANUSCRIPT_ID") != ''
        ) TO '{INGEST_PATH}/RSC.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/RSC.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_spr(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Springer 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'SPR' AS publisher,
                   '2020 SPR - Compact Report for UKB December 2020.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 SPR - Compact Report for UKB December 2020.xlsx',
                           sheet = 'OA Approved YTD2020 published', range = 'A5:V9999',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/SPR.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/SPR.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_taf(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Taylor & Francis 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'TAF' AS publisher,
                   '2020 TAF - Taylor Open Access Article List 202012 dd20210119.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 TAF - Taylor Open Access Article List 202012 dd20210119.xlsx',
                           sheet = 'Sheet1',
                           all_varchar = true)
            WHERE "DOI" IS NOT NULL AND TRIM("DOI") != ''
        ) TO '{INGEST_PATH}/TAF.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/TAF.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def ingest_thi(INGEST_PATH, RAW_PATH, mo):
    # Ingest: Thieme 2020 report -> ingest parquet
    _df = mo.sql(
        f"""
        COPY (
            SELECT 'THI' AS publisher,
                   '2020 THI - Thieme 2020 article List Dutch institutions.xlsx' AS source_file,
                   *
            FROM read_xlsx('{RAW_PATH}/2020 THI - Thieme 2020 article List Dutch institutions.xlsx',
                           sheet = 'article list NL',
                           all_varchar = true)
            WHERE "Article DOI" IS NOT NULL AND TRIM("Article DOI") != ''
        ) TO '{INGEST_PATH}/THI.parquet' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true);

        INSERT INTO db.meta.ingest_log
        SELECT 'publisher_reports' as data_source, source_file as file, now() as ingest_date
        FROM '{INGEST_PATH}/THI.parquet'
        LIMIT 1
        """,
        output=False
    )
    return


@app.cell
def _(INGEST_PATH, mo):
    _df = mo.sql(
        f"""
        SELECT file
        FROM glob('{INGEST_PATH}/*.parquet')
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
def _(mo):
    _df = mo.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS db.publisher_reports_bronze;

        CREATE OR REPLACE TABLE db.publisher_reports_bronze.works
        (
            doi VARCHAR,
            title VARCHAR,
            journal VARCHAR,
            author VARCHAR,
            institution VARCHAR,
            licence VARCHAR,
            oa_status VARCHAR,
            publication_date VARCHAR,
            article_type VARCHAR,
            publisher VARCHAR
        );

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Article Title" as title,
            "Journal Title" as journal,
            "Author" as author,
            "Institution" as institution,
            "license" as license,
            NULL as oa_status,
            "publication date" as publication_date,
            "Article Type" as article_type,
            'BRI' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/BRI.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Article Title" as title,
            "Journal Title" as journal,
            "Affiliated Author" as author,
            "GRID Name" as institution,
            "OA Licence Type" as license,
            "Current OA Status" oa_status,
            "Online Publication Date" as publication_date,
            "Article Type" as article_type,
            'CUP' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/CUP.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "full article name" as title,
            "Journal title" as journal,
            "authors name" as author,
            "Institute Name" as institution,
            "license type" as license,
            "access type" as oa_status,
            "journey complete date" as publication_date,
            "article type" as article_type,
            'ELS' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/ELS.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Article Title" as title,
            "Journal Name" as journal,
            "CorrespondingAuthor" as author,
            "S1 Affiliation" as institution,
            "OA License Applied" as license,
            NULL as oa_status,
            "Date Published Online" as publication_date,
            NULL as article_type,
            'EME' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/EME.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Title" as title,
            "journal" as journal,
            concat_ws(' ', "GivenName", "Surname") as author,
            "Institution" as institution,
            NULL as license,
            NULL as oa_status,
            "Status" as publication_date,
            "Type Article" as article_type,
            'IOS' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/IOS.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Article Title" as title,
            "Journal" as journal,
            "Corr. Author" as author,
            "UKB Member Institution" as institution,
            "License" as license,
            "Journal Type (OA/Hybrid)" as oa_status,
            "Online Publication Date" as publication_date,
            "Article Type" as article_type,
            'KAR' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/KAR.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            NULL as doi,
            "Article Title" as title,
            regexp_extract("Journal Name", '^(.*? OA) - (.*?) *(\(...\))(\(....-....\))', 2, 'i') as journal,
            "Corresponding Author" as author,
            "Institution" as institution,
            NULL as license,
            regexp_extract("Journal Name", '^(.*? OA) - (.*?) *(\(...\))(\(....-....\))', 1, 'i') as oa_status,
            "Pub Date" as publication_date,
            NULL as article_type,
            'LWW' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/LWW.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "Doi" as doi,
            "Manuscript Title" as title,
            "Journal Name" as journal,
            NULL as author,
            NULL as institution,
            "Licence" as license,
            "Type" as oa_status,
            "Order Date" as publication_date,
            NULL as article_type,
            'OUP' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/OUP.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            '10.1039/' || "RESOURCE_ID" as doi,
            "MANUSCRIPT_TITLE" as title,
            "PRODUCT NAME" as journal,
            "MASTER NAME" as author,
            "RELATED INSTITUTION" as institution,
            "OA LICENCE TYPE" as license,
            "IS OA?" as oa_status,
            "E-ISSUE PUBLICATION DATE" as publication_date,
            "MANUSCRIPT_CODE" as article_type,
            'RSC' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/RSC.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Article Title" as title,
            "Journal Title" as journal,
            "Corresponding Author Name" as author,
            "Institution" as institution,
            "Licence Type" as license,
            NULL as oa_status,
            "Online Date" as publication_date,
            "Article Type" as article_type,
            'SPR' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/SPR.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "DOI" as doi,
            "Title" as title,
            "Journal title" as journal,
            "Author" as author,
            "Institution" as institution,
            NULL as license,
            "OA Status on Taylor & Francis Online" as oa_status,
            "Date published online" as publication_date,
            "Manuscript Type" as article_type,
            'TAF' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/TAF.parquet');

        INSERT INTO db.publisher_reports_bronze.works
        SELECT
            "Article DOI" as doi,
            "Article Title" as title,
            "Journal Title" as journal,
            "Author" as author,
            "Affiliation Name" as institution,
            "Type of CC License" as license,
            "Open Access" as oa_status,
            "Online Publication Date (online)" as publication_date,
            NULL as article_type,
            'THI' as publisher
        FROM read_parquet('s3://broccoli-alpha/data/publisher_reports/ingest/THI.parquet');
        """
    )
    return


@app.cell
def _(mo):
    # Bronze checks: total rows + row count per publisher.
    _bronze_checks = mo.sql(
        f"""
        SELECT publisher, count(*) AS n_rows
        FROM db.publisher_reports_bronze.works
        GROUP BY publisher
        ORDER BY publisher
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
    _df = mo.sql(
        f"""
        describe db.publisher_reports_bronze.works
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE MACRO try_cast_date(datestr) AS
        CASE
            WHEN datestr is NULL
                THEN NULL
                WHEN try_cast(datestr as DATE) IS NOT NULL
                THEN try_cast(datestr as DATE)
            WHEN try_cast(datestr as integer) < 100000
                THEN date_add(DATE '1900-01-01', INTERVAL (cast(datestr as integer)-2) DAYS)::DATE
            WHEN regexp_matches(datestr::STRING, '([0-9][0-9][0-9][0-9][-/][0-9][0-9][-/][0-9][0-9])')
                THEN try_cast(regexp_extract(datestr::STRING, '([0-9][0-9][0-9][0-9][-/][0-9][0-9][-/][0-9][0-9])', 1) as DATE)
            WHEN regexp_matches(datestr::STRING, '([0-9][0-9][-/][0-9][0-9][-/][0-9][0-9][0-9][0-9])')
                THEN try_cast(regexp_extract(datestr::STRING, '([0-9][0-9][-/][0-9][0-9][-/][0-9][0-9][0-9][0-9])', 1) as DATE)
                ELSE try_cast(datestr as date)
        end;

        CREATE OR REPLACE MACRO try_cast_oa_status(oa_status) AS
        CASE
            when regexp_matches(oa_status, '(hybrid)', 'i')
                then 'hybrid'
            when regexp_matches(oa_status, '(gold|green|bronze|diamond|closed)', 'i')
                then lower(regexp_extract(oa_status, '(gold|green|bronze|diamond|closed)', 1, 'i'))
            when regexp_matches(oa_status, '^(yes|open|openaccess|open access)$', 'i')
                then 'open'
            when regexp_matches(oa_status, '(non oa|not oa|subscription|not open)', 'i')
                or regexp_matches(oa_status, '^(no)$', 'i')
                then 'closed'
        END;

        CREATE OR REPLACE MACRO try_cast_work_type(work_type) AS
        CASE
            when lower(work_type) in
                ('fla','originalpaper','article','research-article','sco','research article','paper','research article','research-article','crp','research','case-report','case report','brief report','osp','short communication','pro','brief-report','target article','feature article','methods-article','case report')
                then 'article'
            when lower(work_type) in
                ('report')
                then 'report'
            when lower(work_type) in
                ('rev','reviewpaper','review','review article','ssu','review-article','review-article','review','systematic-review','critical review','meta-analysis','tutorial review','review article')
                then 'review'
        END;


        CREATE OR REPLACE MACRO clean_doi(doi) AS
            regexp_replace(regexp_replace(doi, 'https?://doi.org/', ''), '\s', '');

        CREATE OR REPLACE MACRO validate_doi(doi) AS
        	regexp_matches(clean_doi(doi), '^10\.\d{{4,9}}/[-\._;()/:A-z0-9]+$')
        """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS db.publisher_reports_silver;

        CREATE OR REPLACE TABLE db.publisher_reports_silver.works
        (
            doi VARCHAR,
            title VARCHAR,
            licence VARCHAR,
            oa_status VARCHAR,
            publication_year INTEGER,
            publication_date DATE,
            work_type VARCHAR
        );

        INSERT INTO db.publisher_reports_silver.works
        SELECT DISTINCT
        	clean_doi(doi) as doi,
        	title,
        	licence,
        	try_cast_oa_status(oa_status) as oa_status,
        	year(try_cast_date(publication_date)) as publication_year,
        	try_cast_date(publication_date) as publication_date,
        	try_cast_work_type(article_type) as work_type
        from db.publisher_reports_bronze.works
        where validate_doi(doi)
        QUALIFY row_number() over (partition by doi order by publication_date desc) = 1;
        """
    )
    return


@app.cell
def _(mo):
    silver_assert = mo.sql(
        f"""
        SELECT
          count()                            AS n_works,
          count() FILTER (WHERE doi IS NULL) AS null_doi,
          count(DISTINCT doi) FILTER (WHERE doi IS NOT NULL) AS n_unique_dois,
          count() FILTER (WHERE publication_date IS NULL) AS null_date,
          count() FILTER (WHERE publication_date > today()) AS invalid_date,
          count() FILTER (WHERE oa_status NOT IN ('open','gold','hybrid','green','bronze','diamond','closed')) AS bad_status,
          count() FILTER (WHERE oa_status is NULL) AS oa_status_null,
          count() FILTER (WHERE work_type is NULL) AS work_type_null,
          (count(doi) = count(DISTINCT doi))     AS ids_unique
        FROM db.publisher_reports_silver.works
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
                    critical=True),
        data_assert(silver_assert["work_type_null"][0] == 0,
                    f"null work_type present: {silver_assert["work_type_null"][0]}",
                    critical=False)
    ]

    if False in checks:
        raise AssertionError("Pipeline cannot continue")
    else:
        print("No critical errors")
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        SELECT publication_year::VARCHAR, oa_status, count(*) AS n 
        FROM db.publisher_reports_silver.works
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
def _(mo):
    oa_status_2020 = mo.sql(
        f"""
        SELECT oa_status, count(*) AS n 
        FROM db.publisher_reports_silver.works
        WHERE publication_year = 2020 and oa_status IS NOT NULL
        GROUP BY oa_status
        ORDER BY n
        """
    )
    return (oa_status_2020,)


@app.cell
def _(oa_status_2020):
    import altair as alt
    _chart = (
        alt.Chart(oa_status_2020)
        .mark_arc(innerRadius=0)
        .encode(
            color=alt.Color(field='oa_status', type='nominal'),
            theta=alt.Theta(field='n', type='quantitative', aggregate='sum'),
            tooltip=[
                alt.Tooltip(field='n', aggregate='sum'),
                alt.Tooltip(field='n', aggregate='sum', format=',.0f'),
                alt.Tooltip(field='oa_status')
            ],
            order='n'
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


if __name__ == "__main__":
    app.run()
