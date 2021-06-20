import json
import os

import click
import numpy as np
import pandas as pd
import xmltodict
from loguru import logger
from stackoverflow_jobs.filters import Description, Remote, Role
from stackoverflow_jobs.query import Query


# Download data
def download_as_dict():
    """Download data from stackoverflow job listings

    The return will be list of dictionaries.
    """

    # We build a query that gets data science jobs
    q = Query()
    q = Query() + Role([Role.Type.DATASCIENCE])

    logger.debug("Downloading from Stackoverflow")
    data = q.execute()
    logger.debug("Downloaded from stackoverflow")

    # Convert the xml data to list of dictionaries
    data_obj = xmltodict.parse(data)
    ds_jobs = data_obj["rss"]["channel"]["item"]

    logger.debug(f"Downloaded {len(ds_jobs)} results of data science jobs")

    return ds_jobs


def convert_dict_data_to_dataframe(ds_jobs):
    """convert the dictionaries to a pandas dataframe"""

    df_ds_jobs = pd.DataFrame(ds_jobs)
    df_ds_jobs["location"] = df_ds_jobs.location.apply(
        lambda x: None if pd.isna(x) else x.get("#text")
    )
    df_ds_jobs["stackoverflow_id"] = df_ds_jobs.guid.apply(
        lambda x: None if pd.isna(x) else x.get("#text")
    )
    df_ds_jobs.drop("guid", axis=1, inplace=True)
    df_ds_jobs["author"] = df_ds_jobs["a10:author"].apply(
        lambda x: None if pd.isna(x) else x.get("a10:name")
    )
    df_ds_jobs.drop("a10:author", axis=1, inplace=True)

    df_ds_jobs["location_country"] = df_ds_jobs.location.apply(
        lambda x: None if pd.isna(x) else x.split(",")[-1].strip()
    )
    df_ds_jobs["location_city"] = df_ds_jobs.location.apply(
        lambda x: None if pd.isna(x) else x.split(",")[0].strip()
    )

    # Deal with datetime
    df_ds_jobs["updated_at"] = pd.to_datetime(df_ds_jobs["a10:updated"])
    df_ds_jobs.drop("a10:updated", axis=1, inplace=True)
    df_ds_jobs.rename(columns={"pubDate": "published_at"}, inplace=True)
    df_ds_jobs["published_at"] = pd.to_datetime(df_ds_jobs["published_at"])

    return df_ds_jobs


def save(dataframe, target):
    """Save dataframe as file

    If existing file specified, we will load the existing files first then append the new data.
    """

    if not os.path.isfile(target):
        df_all = pd.DataFrame()
    else:
        logger.debug("Load existing file")
        df_all = pd.read_csv(
            target,
            dtype={"stackoverflow_id": str},
            parse_dates=["updated_at", "published_at"],
        )
        df_all = df_all.loc[~df_all.stackoverflow_id.isin(dataframe.stackoverflow_id)]
        logger.debug("Excluded ids that are in the current dataset")

    df_all = pd.concat([df_all, dataframe])

    logger.debug(f"Saving to csv: {target}")
    df_all.to_csv(target, index=False)
    logger.debug(f"Saved to csv: {target}")


@click.command()
@click.argument("target", default="dataset/stackoverflow_job_listing.csv")
def stackoverflow(target):

    data = download_as_dict()

    df = convert_dict_data_to_dataframe(data)

    save(df, target)


if __name__ == "__main__":

    stackoverflow()
