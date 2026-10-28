"""Build the date/state dimensions and daily state facts from Silver Parquet."""
from pyspark.sql import SparkSession, functions as F, Window
from config import GOLD_ROOT, SILVER_ROOT


def main() -> None:
    spark = SparkSession.builder.appName("covid-silver-to-gold").getOrCreate()
    cases = spark.read.parquet(f"{SILVER_ROOT}/cases_standardized")
    tests = spark.read.parquet(f"{SILVER_ROOT}/testing_standardized")

    all_dates = (cases.select("full_date").unionByName(tests.select("full_date"))
                 .where(F.col("full_date").isNotNull()).distinct())
    dim_date = (all_dates
        .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("dow", F.dayofweek("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select("date_id", "full_date", "year", "month", "day", "dow", "is_weekend"))

    dim_state = (cases.select("state_code", "state_name")
                 .unionByName(tests.select("state_code", "state_name"))
                 .distinct())

    cw = Window.partitionBy("state_code").orderBy("full_date")

    fact_cases = (cases
        .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("new_cases", F.greatest(F.col("cases_cum") - F.coalesce(F.lag("cases_cum").over(cw), F.lit(0)), F.lit(0)))
        .withColumn("new_deaths", F.greatest(F.col("deaths_cum") - F.coalesce(F.lag("deaths_cum").over(cw), F.lit(0)), F.lit(0)))
        .select("date_id", "state_code", "cases_cum", "deaths_cum", "new_cases", "new_deaths"))

    tw = Window.partitionBy("state_code").orderBy("full_date")

    fact_testing = (tests
        .withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("new_tests", F.greatest(F.col("tests_total_cum") - F.coalesce(F.lag("tests_total_cum").over(tw), F.lit(0)), F.lit(0)))
        .withColumn("positivity_rate", F.when(F.col("tests_total_cum") > 0,
            F.col("tests_pos_cum").cast("double") / F.col("tests_total_cum")))
        .select("date_id", "state_code", "tests_total_cum", "tests_pos_cum",
                "tests_neg_cum", "new_tests", "positivity_rate"))


    dim_date.write.mode('overwrite').parquet(f'{GOLD_ROOT}/dim_date')
    dim_state.write.mode('overwrite').parquet(f'{GOLD_ROOT}/dim_state')
    fact_cases.write.mode("overwrite").partitionBy("state_code").parquet(f"{GOLD_ROOT}/fact_cases_state_daily")
    fact_testing.write.mode("overwrite").partitionBy("state_code").parquet(f"{GOLD_ROOT}/fact_testing_state_daily")
    print(f"Gold datasets written under {GOLD_ROOT}")
    spark.stop()


if __name__ == "__main__":
    main()
