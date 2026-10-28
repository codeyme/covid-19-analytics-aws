"""Read and lightly validate the raw CSVs already landed in the Bronze S3 area.

Bronze keeps source values as strings. Parsing, standardizing, and writing
Parquet belongs in silver.py.
"""
from collections.abc import Sequence
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from config import source_path


SOURCES = {
    "cases": ("nytimes/us_states.csv", {"date", "state", "cases", "deaths"}),
    "county_cases": ("nytimes/us_county.csv", {"date", "county", "state", "cases", "deaths"}),
    "testing": ("covid_tracking/states_daily.csv", {"date", "state", "positive", "negative", "totalTestResults"}),
    "us_daily": ("covid_tracking/us_daily.csv", {"date", "states"}),
    "us_summary": ("covid_tracking/us.csv", {"positive", "negative"}),
    "states": ("static/states_abv.csv", {"State", "Abbreviation"}),
    "country_codes": ("static/CountryCodeQS.csv", {"Country", "Alpha-2 code"}),
    "county_population": ("static/County_Population.csv", {"County", "State", "Population Estimate 2018"}),
}


def read_bronze(
    spark: SparkSession, source_names: Optional[Sequence[str]] = None
) -> dict[str, DataFrame]:
    """Load selected Bronze CSVs and return them by name.

    Example: ``frames = read_bronze(spark, ["cases", "testing", "states"])``
    """
    selected = list(source_names) if source_names is not None else list(SOURCES)
    unknown = set(selected).difference(SOURCES)
    if unknown:
        raise ValueError(f"Unknown Bronze source names: {sorted(unknown)}")

    result = {}
    for name in selected:
        relative_path, required_columns = SOURCES[name]
        df = (spark.read.option("header", True)
              .option("inferSchema", False)
              .option("mode", "FAILFAST")
              .csv(source_path(relative_path)))
        missing = required_columns.difference(df.columns)
        if missing:
            raise ValueError(
                f"{relative_path} is missing expected columns: {sorted(missing)}"
            )
        result[name] = df
    return result
