"""Retrieve the fire spreading risk file."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click
import requests

from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


_FIRE_SPREADING_RISK_URL = "https://maps.csr.ufmg.br/geodownload/"
_FIRE_SPREADING_RISK_HEADERS = {
    "host": "maps.csr.ufmg.br",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "referer": "https://maps.csr.ufmg.br/geodownload/?workspace=CSR&store=tif__projetos_csr__Fip_cerrado__cerrado__cerrado_fire_steps__cerrado_fire_steps&license_agreement=true",
}
_FIRE_SPREADING_RISK_QUERYSTRING = {
    "workspace": "CSR",
    "store": "tif__projetos_csr__Fip_cerrado__cerrado__cerrado_fire_steps__cerrado_fire_steps",
    "license_agreement": "true",
}


def _path_to_pathlib(ctx, param, value):
    _ = ctx  # no warn
    _ = param  # no warn
    return Path(value)


@click.command()
@click.option(
    "--save-dir",
    required=True,
    type=click.Path(exists=True, resolve_path=True, dir_okay=True),
    callback=_path_to_pathlib,
    help="Directory to save the downloaded image.",
)
def main(save_dir: Path):
    """Retrieve the fire spreading risk image."""
    today = datetime.today().strftime("%Y_%m_%d_%H_%M_%S")

    output_file = save_dir / f"fire_spreading_risk_{today}.zip"

    response = requests.get(
        _FIRE_SPREADING_RISK_URL,
        headers=_FIRE_SPREADING_RISK_HEADERS,
        params=_FIRE_SPREADING_RISK_QUERYSTRING,
        timeout=None,
    )

    today = datetime.today().strftime("%Y_%m_%d_%H_%M_%S")

    output_file = save_dir / f"fire_spreading_risk_{today}.zip"
    log_file = save_dir / f"fire_spreading_risk_{today}.log"

    msg = ""

    if not response.ok:
        msg = f"download failed with HTTP status {response.status_code}"
    else:
        try:
            with open(output_file, "wb") as f:
                f.write(response.content)
            msg = "file downloaded successfully"
        except OSError as e:
            msg = f"download succeeded, but failed to save file: {e}"

    with open(log_file, "w", encoding="utf-8") as f:
        f.write(msg)
