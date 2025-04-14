"""Retrieve the risk image from an ftp server."""

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
def main(
    db_url: str,
    stac_api_url: str,
    collection: str,
    save_dir: str,
    days_until_expiration: int,
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

    download_risk_file(
        db_url=db_url,
        stac_api_url=stac_api_url,
        collection=collection,
        save_dir=Path(save_dir),
        days_until_expiration=days_until_expiration,
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
):
    db = DatabaseFacade.from_url(db_url=db_url)

    # looking for stac risk file
    now = datetime.now()
    beg = now - relativedelta(days=days_until_expiration - 1)
    end = now

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

        cur_date = properties["datetime"]
        last_date = cur_date if last_date is None else min(last_date, cur_date)

        for name, values in item["assets"].items():
            if name.lower() != RISK_ASSET_NAME.lower():
                continue
            candidates[cur_date] = values

    if len(candidates) == 0:
        msg = "There is no new file"
        write_log(
            db=db, msg=msg, status=0, file_date=None, file_name="", is_new=False, src=""
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

    file_date = last_date
    file_expiration_date = cur_date + relativedelta(days=days_until_expiration)

    write_log(
        db=db,
        msg=msg,
        status=status,
        file_date=file_date,
        file_name=file_name,
        is_new=datetime.now().date() <= file_expiration_date.date(),
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
