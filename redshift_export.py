"""Create unpartitioned Parquet files with every column physically present for COPY."""
from pyspark.sql import SparkSession

from config import GOLD_ROOT, REDSHIFT_EXPORT_ROOT


def main() -> None:
    spark = SparkSession.builder.appName("covid-redshift-export").getOrCreate()
    columns = {
        "dim_date": ["date_id", "full_date", "year", "month", "day", "dow", "is_weekend"],
        "dim_state": ["state_code", "state_name"],
        "fact_cases_state_daily": ["date_id", "state_code", "cases_cum", "deaths_cum", "new_cases", "new_deaths"],
        "fact_testing_state_daily": ["date_id", "state_code", "tests_total_cum", "tests_pos_cum", "tests_neg_cum", "new_tests", "positivity_rate"],
    }
    for name, ordered_columns in columns.items():
        df = spark.read.parquet(f"{GOLD_ROOT}/{name}").select(*ordered_columns)
        # Gold facts store state_code in partition paths; selecting it here writes it
        # into each Parquet row so Redshift COPY receives the full Redshift table row.
        df.write.mode("overwrite").parquet(f"{REDSHIFT_EXPORT_ROOT}/{name}")
    print(f"Redshift-ready Parquet written under {REDSHIFT_EXPORT_ROOT}")
    spark.stop()


if __name__ == "__main__":
    main()
