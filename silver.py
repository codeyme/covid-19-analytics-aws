"""Standardize state cases and testing from Bronze CSVs into partitioned Parquet."""
from pyspark.sql import DataFrame, SparkSession, functions as F
from bronze import read_bronze
from config import SILVER_ROOT


def _state_lookup(raw_states) -> DataFrame:
    # Actual file headers are State and Abbreviation (not name and abbr).
    return (raw_states.select(
                F.upper(F.trim(F.col("Abbreviation"))).alias("state_code"),
                F.initcap(F.trim(F.col("State"))).alias("state_name"),
            ).dropDuplicates(["state_code"]))


def _check_rows(df: DataFrame, dataset: str) -> None:
    bad = df.filter(F.col("full_date").isNull() | F.col("state_code").isNull()).limit(1).count()
    if bad:
        raise ValueError(
            f"{dataset} has a row with an unparseable date or state not found in states_abv.csv"
        )


def standardize_cases(raw, states) -> DataFrame:
    out = (raw
        .withColumn("full_date", F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("state_name_key", F.initcap(F.trim(F.col("state"))))
        .join(states, F.col("state_name_key") == F.col("state_name"), "left")
        .withColumn("cases_cum", F.col("cases").cast("long"))
        .withColumn("deaths_cum", F.col("deaths").cast("long"))
        .select("full_date", "state_code", "state_name", "cases_cum", "deaths_cum")
        .dropDuplicates(["full_date", "state_code"]))
    _check_rows(out, "us_states.csv")
    return (out.withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date")))


def standardize_testing(raw: DataFrame, states: DataFrame) -> DataFrame:
    out = (raw
        .withColumn("full_date", F.to_date(F.col("date"), "yyyyMMdd"))
        .withColumn("state_code", F.upper(F.trim(F.col("state"))))
        .join(states, "state_code", "left")
        .withColumn("tests_total_cum", F.col("totalTestResults").cast("long"))
        .withColumn("tests_pos_cum", F.col("positive").cast("long"))
        .withColumn("tests_neg_cum", F.col("negative").cast("long"))
        .select("full_date", "state_code", "state_name", "tests_total_cum",
                "tests_pos_cum", "tests_neg_cum")
        .dropDuplicates(["full_date", "state_code"]))
    _check_rows(out, "states_daily.csv")
    return (out.withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date")))


def main() -> None:
    spark = SparkSession.builder.appName("covid-bronze-to-silver").config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com").config("spark.hadoop.fs.s3a.access.key", "AKIA2GTEWWXXUX6J4U7B").config("spark.hadoop.fs.s3a.secret.key", "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").enableHiveSupport().getOrCreate()
    # read_bronze returns a dictionary; use only the three inputs needed here.
    bronze = read_bronze(spark, ["cases", "testing", "states"])
    states = _state_lookup(bronze["states"])
    cases = standardize_cases(bronze["cases"], states)
    testing = standardize_testing(bronze["testing"], states)

    cases.write.mode("overwrite").partitionBy("state_code", "year", "month", "day").parquet(f"{SILVER_ROOT}/cases_standardized")
    testing.write.mode("overwrite").partitionBy("state_code", "year", "month", "day").parquet(f"{SILVER_ROOT}/testing_standardized")

    print(f"Silver datasets written under {SILVER_ROOT}")
    spark.stop()


if __name__ == "__main__":
    main()
