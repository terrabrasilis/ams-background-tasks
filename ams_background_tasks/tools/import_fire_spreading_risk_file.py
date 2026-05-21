"""Import the fire spreading risk file from save_dir."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)

_FIRE_SPREADING_RISK_DB_SCHEMA = "fire_spreading_risk"
_FIRE_SPREADING_RISK_DB_FILE_TABLE_NAME = "risk_file"
_FIRE_SPREADING_RISK_DB_FILE_TABLE = (
    f"{_FIRE_SPREADING_RISK_DB_SCHEMA}.{_FIRE_SPREADING_RISK_DB_FILE_TABLE_NAME}"
)


def _path_to_pathlib(ctx, param, value):
    _ = ctx  # no warn
    _ = param  # no warn
    return Path(value)


@click.command()
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--save-dir",
    required=True,
    type=click.Path(exists=True, resolve_path=True, dir_okay=True),
    callback=_path_to_pathlib,
    help="Directory where downloaded images are saved",
)
def main(db_url: str, save_dir: Path):
    """Import the fire spreading risk image from save_dir."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    zip_files = list(save_dir.glob("fire_spreading_risk_*.zip"))

    for zip_file in zip_files:
        sql = f"""
            SELECT file_name
            FROM {_FIRE_SPREADING_RISK_DB_FILE_TABLE}
            WHERE file_name='{zip_file.name}'
        """
        res = db.fetchall(sql)

        if len(res):
            logger.info("file %s already exists", zip_file)
            continue

        dt_fmt = "%Y-%m-%d %H:%M:%S"

        parts = zip_file.stem.split("_")
        assert len(parts) == 9

        print(f"{parts[3]}-{parts[4]}-{parts[5]} {parts[6]}:{parts[7]}:{parts[8]}")

        file_date = datetime.strptime(
            f"{parts[3]}-{parts[4]}-{parts[5]} {parts[6]}:{parts[7]}:{parts[8]}", dt_fmt
        )

        sql = f"""
            INSERT INTO {_FIRE_SPREADING_RISK_DB_FILE_TABLE} (file_name, process_status, process_message, file_date, is_new)
            VALUES('{zip_file.name}', 1, 'imported successfully', '{file_date.strftime(dt_fmt)}', {True});
        """

        db.execute(sql)

    db.commit()
