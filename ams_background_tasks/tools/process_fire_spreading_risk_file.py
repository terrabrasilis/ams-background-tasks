"""Process all files in the dir."""

from __future__ import annotations

import os
import sys
import tempfile
import zipfile
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
from shapely.geometry import Point

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)

_FIRE_SPREADING_RISK_VALUE = 0
_FIRE_SPREADING_RISK_DB_SCHEMA = "fire_spreading_risk"
_FIRE_SPREADING_RISK_DB_FILE_TABLE_NAME = "risk_file"
_FIRE_SPREADING_RISK_DB_DATA_TABLE_NAME = "risk_data"
_FIRE_SPREADING_RISK_DB_DATA_TABLE = (
    f"{_FIRE_SPREADING_RISK_DB_SCHEMA}.{_FIRE_SPREADING_RISK_DB_DATA_TABLE_NAME}"
)
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
    help="Directory to save the downloaded image.",
)
@click.option("--srid", type=int, default=4674, help="SRID of the risk points.")
def main(db_url: str, save_dir: Path, srid: str):
    """Process all files in the dir."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    sql = f"""
        SELECT file_name
        FROM {_FIRE_SPREADING_RISK_DB_FILE_TABLE}
        WHERE is_new=True
    """

    zip_files = sorted([_[0] for _ in db.fetchall(sql)], reverse=True)

    for zip_path in zip_files:
        zip_path = save_dir / Path(zip_path).name

        parts = zip_path.stem.split("_")
        file_date = f"{parts[3]}-{parts[4]}-{parts[5]}"

        sql = f"""
            SELECT view_date
            FROM {_FIRE_SPREADING_RISK_DB_DATA_TABLE}
            WHERE view_date='{file_date}'::date
        """
        res = db.fetchall(sql)

        if len(res):
            logger.debug(
                "skipping file %s - data for %s already exists in the database",
                zip_path,
                file_date,
            )
            continue

        logger.info("processing %s ...", zip_path)
        if Path(zip_path).exists():
            process_fire_spreading_risk_file(db=db, zip_path=zip_path, srid=srid)
        else:
            logger.debug("file %s not found.", zip_path)

        db.execute(
            sql=f"""
                UPDATE {_FIRE_SPREADING_RISK_DB_FILE_TABLE}
                SET is_new=False, processed_at=now()
                WHERE file_name='{zip_path.name}';
            """
        )

    sql = f"""
        UPDATE {_FIRE_SPREADING_RISK_DB_DATA_TABLE} AS rk
        SET geocode=mun.geocode, municipality=mun.name
        FROM public.municipalities_biome mub
        JOIN public.municipalities mun
	        ON mun.geocode=mub.geocode
        WHERE
	        mub.biome='Cerrado'
	        AND rk.geom && mun.geometry
	        AND ST_Within(rk.geom, mun.geometry);
    """

    db.execute(sql)

    db.commit()


def process_fire_spreading_risk_file(db: DatabaseFacade, zip_path: Path, srid: str):
    """Extract risk point data from a ZIP file and store it in the database."""

    def _insert_into_risk_data_table(values: list):
        sql = f"INSERT INTO {_FIRE_SPREADING_RISK_DB_DATA_TABLE} (geom, biome, view_date, src) VALUES {','.join(values)};"
        db.execute(sql=sql, log=False)

    with zipfile.ZipFile(zip_path, "r") as zipf:
        with tempfile.TemporaryDirectory() as temp_dir:
            zipf.extractall(temp_dir)

            file_path = Path(temp_dir) / "cerrado_fire_map.tif"
            logger.info(file_path)
            if not file_path.exists():
                return

            parts = zip_path.name.split("_")
            view_date = f"{parts[3]}-{parts[4]}-{parts[5]}"

            dfr = gpd.GeoDataFrame()
            with rio.open(file_path, "r") as src:
                data = src.read(1)

                _ = np.where(data == _FIRE_SPREADING_RISK_VALUE)
                indices = list(zip(_[0], _[1]))

                geom = [Point(src.xy(x, y)[0], src.xy(x, y)[1]) for x, y in indices]

                values = [_FIRE_SPREADING_RISK_VALUE] * len(indices)

                dfr = gpd.GeoDataFrame({"geometry": geom}, crs=src.crs)
                dfr.to_crs(crs=srid, inplace=True)

            values = []
            for _, row in dfr.iterrows():
                values.append(
                    f"(ST_PointFromText('{row['geometry'].wkt}', {srid}),'Cerrado','{view_date}', '{zip_path.name}')"
                )

                if len(values) > 1e5:
                    _insert_into_risk_data_table(values=values)
                    values = []

            if len(values) > 0:
                _insert_into_risk_data_table(values=values)
