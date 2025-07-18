"""Retrieve the risk image from an ftp server."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import click
from dateutil.relativedelta import relativedelta

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.ftp_utils import FtpFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.risk_utils import (
    RISK_SRC_IBAMA,
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
    "--ftp-url",
    required=False,
    type=str,
    default="",
    help="FTP server url (ftp://<username>:<password>@<host>:<port>/<path>).",
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
def main(db_url: str, ftp_url: str, save_dir: str, days_until_expiration: int):
    """Retrieve the risk image from as ftp server."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    ftp_url = os.getenv("AMS_FTP_URL") if not ftp_url else ftp_url
    logger.debug(ftp_url)
    assert ftp_url

    asyncio.run(
        download_risk_file(
            db_url=db_url,
            ftp_url=ftp_url,
            save_dir=Path(save_dir),
            days_until_expiration=days_until_expiration,
        )
    )


async def _download_risk_file(
    db: DatabaseFacade,
    ftp: FtpFacade,
    remote_risk_file: dict,
    output_file: Path,
):
    last_risk_file, last_risk_file_date = get_last_risk_file_info(
        db=db, src=RISK_SRC_IBAMA
    )

    if last_risk_file and remote_risk_file["date"] <= last_risk_file_date:
        return 0, "There is no new file"

    src_file = remote_risk_file["path"]
    dst_file = output_file

    status, msg = await ftp.download(src_file=src_file, dst_file=dst_file)

    if not status:
        return 0, msg

    if dst_file.exists() and dst_file.lstat().st_size == remote_risk_file["size"]:
        return 1, "Risk file downloaded successfully."

    return 0, "The file was downloaded, but is invalid."


async def download_risk_file(
    ftp_url: str, db_url: str, save_dir: Path, days_until_expiration: int
):
    """Download the new risk file if available."""
    db = DatabaseFacade.from_url(db_url=db_url)
    ftp = FtpFacade.from_url(ftp_url=ftp_url)

    files = await ftp.list()

    if len(files) == 0:
        status = 0
        msg = "There is no file on ftp server."
        write_log(
            db=db,
            msg=msg,
            status=status,
            file_date=None,
            file_name="",
            is_new=False,
            src="",
        )
        return

    output_file = (
        save_dir / f"weekly_ibama_1km_{datetime.now().strftime('%d_%m_%Y')}.tif"
    )

    remote_risk_file = max(files, key=lambda _: _["date"])

    status, msg = await _download_risk_file(
        db=db,
        ftp=ftp,
        remote_risk_file=remote_risk_file,
        output_file=output_file,
    )

    logger.info(msg)

    file_date = remote_risk_file["date"]
    file_expiration_date = file_date + relativedelta(days=days_until_expiration)

    write_log(
        db=db,
        msg=msg,
        status=status,
        file_date=remote_risk_file["date"],
        file_name=output_file,
        is_new=datetime.now().date() <= file_expiration_date,
        src=RISK_SRC_IBAMA,
    )

    if status == 1:
        write_expiration_date(
            db=db,
            status=status,
            file_name=output_file,
            file_date=file_date,
            file_expiration_date=file_expiration_date,
            src=RISK_SRC_IBAMA,
        )
