"""Retrieve the risk image from a STAC server."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import click
import requests
from dateutil.relativedelta import relativedelta

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.risk_utils import (
    RISK_ASSET_NAME,
    RISK_SRC_INPE,
    get_last_risk_file_info,
    write_expiration_date,
    write_log,
)

logger = get_logger(__name__, sys.stdout)


@click.command()
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--stac-api-url",
    required=False,
    type=str,
    default="",
    help="STAC api url.",
)
@click.option(
    "--collection",
    required=False,
    type=str,
    default="",
    help="Collection name.",
)
@click.option(
    "--save-dir",
    required=True,
    type=click.Path(exists=True, resolve_path=True, dir_okay=True),
    help="Directory to save the downloaded image.",
)
@click.option(
    "--days-until-expiration",
    required=True,
    type=int,
    help="Number of days until the data expires.",
)
@click.option("--begin", required=True, type=str, help="YYYY-mm-dd")
@click.option("--end", required=True, type=str, help="YYYY-mm-dd")
def main(
    db_url: str,
    stac_api_url: str,
    collection: str,
    save_dir: str,
    days_until_expiration: int,
    begin: str,
    end: str,
):
    """Retrieve the risk image from a STAC server."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    stac_api_url = os.getenv("AMS_STAC_API_URL", stac_api_url)
    logger.debug(stac_api_url)
    assert stac_api_url

    collection = os.getenv("AMS_STAC_COLLECTION", collection)
    logger.debug(collection)
    assert collection

    begin = datetime.strptime(begin, "%Y-%m-%d")
    logger.debug(begin)

    end = datetime.strptime(end, "%Y-%m-%d")
    logger.debug(end)

    download_risk_file(
        db_url=db_url,
        stac_api_url=stac_api_url,
        collection=collection,
        save_dir=Path(save_dir),
        days_until_expiration=days_until_expiration,
        beg=begin,
        end=end,
    )


def _get_items(endpoint: str, params: dict = None):
    logger.debug(endpoint)
    logger.debug(params)

    response = requests.get(endpoint, params=params, timeout=120)
    if response.status_code == 200:
        return response.json()

    logger.error(response.text)
    return None


def _download_asset(url: str, download_path: Path):
    logger.info("downloading %s", url)
    response = requests.get(url, stream=True, timeout=120)
    if response.status_code == 200:
        with open(download_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return 1, "Risk file downloaded successfully."
    return 0, response.text


def download_risk_file(
    db_url: str,
    stac_api_url: str,
    days_until_expiration: int,
    collection: str,
    save_dir: Path,
    beg: datetime,
    end: datetime,
):
    db = DatabaseFacade.from_url(db_url=db_url)

    items = _get_items(
        endpoint=f"{stac_api_url}/search",
        params={
            "collection_id": collection,
            "beg": beg.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        },
    )

    candidates = {}
    last_date = None
    for item in items:
        properties = item["properties"]
        cur_date = datetime.strptime(properties["datetime"], "%Y-%m-%dT%H:%M:%SZ")

        for name, values in item["assets"].items():
            if name.lower() != RISK_ASSET_NAME.lower():
                continue

            candidates[cur_date] = values
            last_date = cur_date if last_date is None else max(last_date, cur_date)

    last_risk_file, last_risk_file_date = get_last_risk_file_info(
        db=db, src=RISK_SRC_INPE
    )

    if (
        last_risk_file
        and last_date.date() <= last_risk_file_date
        or len(candidates) == 0
    ):
        msg = "There is no new file"
        write_log(
            db=db,
            msg=msg,
            status=0,
            file_date=None,
            file_name="",
            is_new=False,
            src=RISK_SRC_INPE,
        )
        return

    # trying to download the risk file
    values = candidates[last_date]

    url = values["href"]

    download_dir = save_dir / RISK_ASSET_NAME
    download_dir.mkdir(exist_ok=True, parents=True)

    file_name = download_dir / Path(url).name

    status, msg = _download_asset(url=url, download_path=file_name)
    logger.debug(msg)

    if not status:
        write_log(
            db=db,
            msg=msg,
            status=0,
            file_date=None,
            file_name="",
            is_new=False,
            src=RISK_SRC_INPE,
        )
        return

    file_date = last_date
    file_expiration_date = file_date + relativedelta(days=days_until_expiration)

    write_log(
        db=db,
        msg=msg,
        status=status,
        file_date=file_date,
        file_name=file_name,
        is_new=last_date.date() <= file_expiration_date.date(),
        src=RISK_SRC_INPE,
    )

    if status == 1:
        write_expiration_date(
            db=db,
            status=status,
            file_name=file_name,
            file_date=file_date,
            file_expiration_date=file_expiration_date,
            src=RISK_SRC_INPE,
        )
