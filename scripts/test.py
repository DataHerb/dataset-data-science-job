import pandas as pd

df_all = pd.read_csv(
    "dataset/indeed_job_listing.csv",
    dtype={"id": str},
    parse_dates=["published_at"],
)

df_all.isna().any()