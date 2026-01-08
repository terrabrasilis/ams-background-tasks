"""Retrieve the fire spreading risk file."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import click
import requests

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


_FIRE_SPREADING_RISK_URL = "https://maps.csr.ufmg.br/geodownload/"
_FIRE_SPREADING_RISK_HEADERS = {
    "host": "maps.csr.ufmg.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "referer": "https://maps.csr.ufmg.br/geodownload/?workspace=CSR&store=tif__projetos_csr__Fip_cerrado__cerrado__cerrado_fire_map__cerrado_fire_map&license_agreement=true",
}
_FIRE_SPREADING_RISK_QUERYSTRING = {
    "workspace": "CSR",
    "store": "tif__projetos_csr__Fip_cerrado__cerrado__cerrado_fire_map__cerrado_fire_map",
    "license_agreement": "true",
}


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
    help="Directory to save the downloaded image.",
)
def main(db_url: str, save_dir: Path):
    """Retrieve the fire spreading risk image."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    now = datetime.now()

    file_prefix = str(
        save_dir / f"fire_spreading_risk_{now.strftime('%Y_%m_%d_%H_%M_%S')}"
    )

    output_file = Path(file_prefix + ".zip")
    log_file = Path(file_prefix + ".log")

    response = requests.get(
        _FIRE_SPREADING_RISK_URL,
        headers=_FIRE_SPREADING_RISK_HEADERS,
        params=_FIRE_SPREADING_RISK_QUERYSTRING,
        timeout=None,
    )

    msg = ""
    status = 0

    if not response.ok:
        msg = f"download failed with HTTP status {response.status_code}"
    else:
        try:
            with open(output_file, "wb") as f:
                f.write(response.content)
            msg = "file downloaded successfully"
            status = 1
        except OSError as e:
            msg = f"download succeeded, but failed to save file: {e}"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(msg)

    dt = now.strftime("%Y-%m-%d %H:%M:%S")

    msg = msg.replace("'", '"')

    sql = f"""
        INSERT INTO fire_spreading_risk.risk_file (file_name, process_status, process_message, file_date, is_new)
        VALUES('{output_file.name}', {status}, '{msg}', '{dt}', {True});
    """

    db.execute(sql)

    db.commit()
