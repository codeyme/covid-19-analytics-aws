-- Run in Athena after silver.py and gold.py complete. Replace the bucket if needed.
CREATE DATABASE IF NOT EXISTS covid_silver_db;
CREATE DATABASE IF NOT EXISTS covid_gold_db;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_silver_db.cases_standardized (
  full_date date,
  state_name string,
  cases_cum bigint,
  deaths_cum bigint
)
PARTITIONED BY (state_code string, year int, month int, day int)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/silver/cases_standardized/';
MSCK REPAIR TABLE covid_silver_db.cases_standardized;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_silver_db.testing_standardized (
  full_date date,
  state_name string,
  tests_total_cum bigint,
  tests_pos_cum bigint,
  tests_neg_cum bigint
)
PARTITIONED BY (state_code string, year int, month int, day int)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/silver/testing_standardized/';
MSCK REPAIR TABLE covid_silver_db.testing_standardized;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dim_date (
  date_id int,
  full_date date,
  year int,
  month int,
  day int,
  dow int,
  is_weekend boolean
)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/gold/dim_date/';

CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dim_state (
  state_code string,
  state_name string
)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/gold/dim_state/';

-- state_code is a partition column, so it belongs in PARTITIONED BY, not the
-- regular column list. The Parquet files contain the other fact columns.
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_cases_state_daily (
  date_id int,
  cases_cum bigint,
  deaths_cum bigint,
  new_cases bigint,
  new_deaths bigint
)
PARTITIONED BY (state_code string)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/gold/fact_cases_state_daily/';
MSCK REPAIR TABLE covid_gold_db.fact_cases_state_daily;

CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_testing_state_daily (
  date_id int,
  tests_total_cum bigint,
  tests_pos_cum bigint,
  tests_neg_cum bigint,
  new_tests bigint,
  positivity_rate double
)
PARTITIONED BY (state_code string)
STORED AS PARQUET
LOCATION 's3://covid-19-analytics-prakriti/gold/fact_testing_state_daily/';
MSCK REPAIR TABLE covid_gold_db.fact_testing_state_daily;

-- Quick checks
SELECT MIN(full_date), MAX(full_date), COUNT(*)
FROM covid_silver_db.cases_standardized;

SELECT state_code, COUNT(*)
FROM covid_gold_db.fact_cases_state_daily
GROUP BY state_code
ORDER BY COUNT(*) DESC;

-- Top five states on a selected day
SELECT s.state_name, f.new_cases
FROM covid_gold_db.fact_cases_state_daily f
JOIN covid_gold_db.dim_state s ON s.state_code = f.state_code
WHERE f.date_id = 20200515
ORDER BY f.new_cases DESC
LIMIT 5;

-- Daily cases, tests, and cumulative positivity for California
SELECT d.full_date, c.new_cases, t.new_tests,
       t.tests_pos_cum * 1.0 / NULLIF(t.tests_total_cum, 0) AS positivity_rate
FROM covid_gold_db.fact_cases_state_daily c
JOIN covid_gold_db.fact_testing_state_daily t
  ON t.date_id = c.date_id AND t.state_code = c.state_code
JOIN covid_gold_db.dim_date d ON d.date_id = c.date_id
WHERE c.state_code = 'CA'
ORDER BY d.full_date;
