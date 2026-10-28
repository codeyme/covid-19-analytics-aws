"""Shared configuration for the COVID-19 analytics pipeline."""
import os


AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
BUCKET = os.getenv("COVID_BUCKET", "covid-19-analytics-prakriti")
# s3://covid-19-analytics-prakriti/bronze/covid_tracking/states_daily.csv
BRONZE_ROOT = os.getenv("COVID_BRONZE_ROOT", f"s3a://{BUCKET}/bronze")
SILVER_ROOT = os.getenv("COVID_SILVER_ROOT", f"s3a://{BUCKET}/silver")
GOLD_ROOT = os.getenv("COVID_GOLD_ROOT", f"s3a://{BUCKET}/gold")
REDSHIFT_EXPORT_ROOT = os.getenv(
    "COVID_REDSHIFT_EXPORT_ROOT", f"s3a://{BUCKET}/redshift_export"
)


def source_path(*parts: str) -> str:
    return "/".join([BRONZE_ROOT.rstrip("/"), *(p.strip("/") for p in parts)])
