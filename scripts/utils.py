from loguru import logger
import os
import pandas as pd


def save(dataframe, target, unique_id="id", parse_dates=None):
    """Save dataframe as file

    If existing file specified, we will load the existing files first then append the new data.
    """
    if parse_dates is None:
        parse_dates = ["updated_at", "published_at"]

    if not os.path.isfile(target):
        df_all = pd.DataFrame()
    else:
        logger.debug("Load existing file")
        df_all = pd.read_csv(
            target,
            dtype={unique_id: str},
            parse_dates=parse_dates,
        )
        df_all = df_all.loc[~df_all[unique_id].isin(dataframe[unique_id])]
        logger.debug("Excluded ids that are in the current dataset")

    df_all = pd.concat([df_all, dataframe])

    logger.debug(f"Saving to csv: {target}")
    df_all.to_csv(target, index=False)
    logger.debug(f"Saved to csv: {target}")
