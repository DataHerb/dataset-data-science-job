import click
from jobs_scraper import JobsScraper
import datetime
import re
from loguru import logger
from utils import save


@click.command()
@click.argument("target", default="dataset/indeed_job_listing.csv")
def indeed(target):

    # Download
    scraper = JobsScraper(
        country="DE", position="Data Scientist", location="", pages=3
    )
    df = scraper.scrape()

    # Furnish data
    df["published_at"] = datetime.date.today()
    df.rename(
        columns={
            "summary": "description"
        },
        inplace=True
    )
    # re_id = re.compile(r"jk=(.*)&fccid")
    df["id"] = df.url

    # Save
    save(df, target, parse_dates=["published_at"])


if __name__ == "__main__":
    indeed()