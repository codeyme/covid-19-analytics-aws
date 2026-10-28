-- Create target schema and tables. COPY column order matches redshift_export.py.
CREATE SCHEMA IF NOT EXISTS covid_gold;

CREATE TABLE IF NOT EXISTS covid_gold.dim_date (
  date_id int NOT NULL,
  full_date date,
  year smallint,
  month smallint,
  day smallint,
  dow smallint,
  is_weekend boolean
) SORTKEY(date_id);

CREATE TABLE IF NOT EXISTS covid_gold.dim_state (
  state_code varchar(2) NOT NULL,
  state_name varchar(64)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS covid_gold.fact_cases_state_daily (
  date_id int NOT NULL,
  state_code varchar(2) NOT NULL,
  cases_cum bigint,
  deaths_cum bigint,
  new_cases bigint,
  new_deaths bigint
) DISTKEY(state_code) SORTKEY(date_id, state_code);

CREATE TABLE IF NOT EXISTS covid_gold.fact_testing_state_daily (
  date_id int NOT NULL,
  state_code varchar(2) NOT NULL,
  tests_total_cum bigint,
  tests_pos_cum bigint,
  tests_neg_cum bigint,
  new_tests bigint,
  positivity_rate double precision
) DISTKEY(state_code) SORTKEY(date_id, state_code);

-- Replace the role ARN with the IAM role attached to your Redshift cluster.
-- COPY paths point to redshift_export/, whose Parquet files include all columns.
COPY covid_gold.dim_date
FROM 's3://covid-19-analytics-prakriti/redshift_export/dim_date/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<RedshiftCopyRole>' FORMAT AS PARQUET;

COPY covid_gold.dim_state
FROM 's3://covid-19-analytics-prakriti/redshift_export/dim_state/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<RedshiftCopyRole>' FORMAT AS PARQUET;

COPY covid_gold.fact_cases_state_daily
FROM 's3://covid-19-analytics-prakriti/redshift_export/fact_cases_state_daily/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<RedshiftCopyRole>' FORMAT AS PARQUET;

COPY covid_gold.fact_testing_state_daily
FROM 's3://covid-19-analytics-prakriti/redshift_export/fact_testing_state_daily/'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/<RedshiftCopyRole>' FORMAT AS PARQUET;

ANALYZE;
