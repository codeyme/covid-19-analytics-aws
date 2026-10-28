# COVID-19 Analytics on AWS

This starter project builds the state-level daily analytics described in the project guidance:

1. Read raw files already uploaded to S3 Bronze.
2. Standardize state cases and COVID Tracking Project testing into Silver Parquet.
3. Build a date dimension, state dimension, and daily case/testing facts in Gold Parquet.
4. Register the Parquet tables in Athena, then optionally export to Redshift.

The bucket and paths default to the ones in your current `bronze.py`. Set environment variables to use another bucket or layout. The code assumes your bucket paths are `s3://BUCKET/bronze/...`, `silver/...`, and `gold/...` (no `/covid/` prefix).

## Files

- `bronze.py` reads selected Bronze CSVs and checks their headers. It does not upload files. `read_bronze()` returns a dictionary keyed by source name.
- `silver.py` calls `read_bronze()` for `cases`, `testing`, and `states`, then parses dates, maps state names/codes, casts measures, and writes partitioned Parquet.
- `gold.py` builds dimensions and facts. Negative changes in cumulative totals are clipped to zero; the first observed cumulative value is used as that day's new count.
- `redshift_export.py` writes unpartitioned Parquet with all columns physically present, which is suitable for the Redshift COPY examples.
- `sql/athena.sql` creates Athena tables and includes sample validation/analysis queries.
- `sql/redshift.sql` creates Redshift tables and COPY commands.

## Prerequisites

- Run with PySpark on a runtime that has S3A support and AWS credentials, such as a suitably configured EMR cluster.
- The runtime role/user needs read access to Bronze and write access to Silver, Gold, and `redshift_export/`.
- Upload the source files to these paths (or adjust the paths in `config.py`):

```text
s3://covid-19-analytics-prakriti/bronze/nytimes/us_states.csv
s3://covid-19-analytics-prakriti/bronze/nytimes/us_county.csv
s3://covid-19-analytics-prakriti/bronze/covid_tracking/states_daily.csv
s3://covid-19-analytics-prakriti/bronze/covid_tracking/us_daily.csv
s3://covid-19-analytics-prakriti/bronze/covid_tracking/us.csv
s3://covid-19-analytics-prakriti/bronze/static/states_abv.csv
s3://covid-19-analytics-prakriti/bronze/static/CountryCodeQS.csv
s3://covid-19-analytics-prakriti/bronze/static/County_Population.csv
```

Your `states_abv.csv` headers are `State` and `Abbreviation`; the Silver code uses those actual headers.

## Run

On a Spark cluster with this folder copied to the driver:

```bash
spark-submit --deploy-mode client silver.py
spark-submit --deploy-mode client gold.py
```

Then run `sql/athena.sql` in Athena. It registers the tables and discovers the partition folders. Run the quick checks before proceeding.

For Redshift, run `redshift_export.py`, replace `<ACCOUNT_ID>` and `<RedshiftCopyRole>` in `sql/redshift.sql`, create the tables, and execute the COPY statements. The Redshift role must have read access to the export prefix.

If using a different bucket:

```bash
export COVID_BUCKET='your-bucket-name'
export AWS_REGION='us-east-2'
```

## Scope notes

This version implements the guidance's state-level cases and testing tables. County data, hospital-bed records, country codes, and population files remain in Bronze for possible extensions. In particular, the hospital-bed file in your folder is newline-delimited JSON, not a standard GeoJSON feature collection; it is not needed for this pipeline.

`inferSchema` is disabled so CSV input types stay predictable; this code explicitly parses dates and casts the measures it uses. Rows with unknown state mappings or invalid dates stop the run so the source issue is visible instead of being silently dropped.
