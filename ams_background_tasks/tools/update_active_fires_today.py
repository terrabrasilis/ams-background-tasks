"""Update the AMS active fires tables."""

# pylint: disable=too-many-try-statements,broad-exception-caught

from __future__ import annotations

import os
import sys
from datetime import datetime
from io import StringIO
from urllib.parse import urljoin

import click
import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pyproj import CRS

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    ACTIVE_FIRES_TODAY_INDICATOR,
    BIOMES,
    analyze_table,
    create_processing,
    finalize_processing,
    optimize_table,
    prepare_table_to_update,
)

_ACTIVE_FIRES_TODAY_BASE_URL = (
    "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/diario/Brasil/"
)
_ACTIVE_FIRES_TODAY_SCHEMA = "fires"
_ACTIVE_FIRES_TODAY_DATA_TABLE = "active_fires_today"
_ACTIVE_FIRES_TODAY_FILE_LOG_TABLE = "active_fires_today_file"
_ACTIVE_FIRES_TODAY_FILE_LOG_TABLE_FQN = (
    f"{_ACTIVE_FIRES_TODAY_SCHEMA}.{_ACTIVE_FIRES_TODAY_FILE_LOG_TABLE}"
)
_ACTIVE_FIRES_TODAY_DATA_TABLE_FQN = (
    f"{_ACTIVE_FIRES_TODAY_SCHEMA}.{_ACTIVE_FIRES_TODAY_DATA_TABLE}"
)


logger = get_logger(__name__, sys.stdout)


@click.command("update-active-fires")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--base-url",
    required=False,
    type=str,
    default=_ACTIVE_FIRES_TODAY_BASE_URL,
    help="Base url to download the lastest active fires of the day.",
)
@click.option(
    "--biome",
    type=click.Choice(BIOMES),
    required=False,
    help="Biome.",
    multiple=True,
    default=BIOMES,
)
@click.option(
    "--limit",
    required=False,
    type=int,
    default=0,
    help="Restrict the number of rows to update (test purpose).",
)
def main(
    db_url: str,
    base_url: str,
    biome: tuple,
    limit: int,
):
    """Run the active-fires-today update pipeline."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.info(base_url)

    base_url = base_url if base_url.endswith("/") else base_url + "/"

    db = DatabaseFacade.create(db_url=db_url)

    create_processing(
        db=db,
        indicator=ACTIVE_FIRES_TODAY_INDICATOR,
        process="update",
        status="processing",
    )

    update_active_fires_today(
        db=db,
        base_url=base_url,
        biome_list=list(biome),
        limit=limit,
    )

    finalize_processing(
        db=db,
        indicator=ACTIVE_FIRES_TODAY_INDICATOR,
        process="update",
        status="completed",
    )

    db.commit()

    analyze_table(
        db=db,
        schema=_ACTIVE_FIRES_TODAY_SCHEMA,
        name=_ACTIVE_FIRES_TODAY_DATA_TABLE,
    )


def _insert_into_file_log_table(db: DatabaseFacade, filename, status, msg, dt):
    file_table = _ACTIVE_FIRES_TODAY_FILE_LOG_TABLE_FQN

    msg = msg.replace("'", '"')

    sql = f"""
        INSERT INTO {file_table} (file_name, process_status, process_message, file_date)
        VALUES('{filename}', {status}, '{msg}', '{dt}');
    """

    db.execute(sql=sql)


def get_latest_active_fires_today_file_url(db: DatabaseFacade, base_url: str):
    """Resolve and validate today's latest active-fires CSV URL.

    This function parses the INPE directory listing, selects the most recent
    CSV entry, checks whether it belongs to the current day, verifies the file
    is reachable, and logs the outcome in `fires.active_fires_today_file`.

    Returns:
        str: The latest CSV URL for today when available; otherwise an empty
        string.
    """
    now = datetime.now()
    dt = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        response = requests.get(url=base_url, timeout=None)

        if not response.ok:
            msg = f"access to url {base_url} is currently unavailable"
            _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
            return ""

        soup = BeautifulSoup(response.text, "html.parser")

        files = sorted(
            [
                urljoin(base_url, _["href"])
                for _ in soup.find_all("a", href=True)
                if _["href"].endswith("csv")
            ],
            reverse=True,
        )

        if len(files) == 0:
            msg = "no active fires file was found"
            _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
            return ""

        last_file_url = files[0]

        if not now.strftime("%Y%m%d") in last_file_url:
            msg = "no active fires file for today was found"
            _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
            return ""

        response = requests.get(last_file_url, timeout=None)

        if not response.ok:
            msg = f"access to url {last_file_url} is currently unavailable"
            _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
            return ""

        return last_file_url

    except requests.RequestException as exc:
        msg = f"request error while resolving active fires file: {exc}"
        _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
        return ""

    except Exception as exc:
        msg = f"unexpected error while resolving active fires file: {exc}"
        _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
        return ""


def process_active_fires_today_file(
    db: DatabaseFacade, base_url: str, file_url: str, biome_list: list, limit: int
):
    """Download and process the daily active fires CSV file.

    Reads the file from the given URL, converts fire points to a GeoDataFrame
    (EPSG:4674), filters records by biome, and persists them into
    `fires.active_fires_today`.
    """

    srid = 4674

    def _insert_into_data_table(db: DatabaseFacade, values: list):
        sql = f"INSERT INTO {_ACTIVE_FIRES_TODAY_DATA_TABLE_FQN} (geom, uuid, biome, view_date, viewed_at, satelite, src) VALUES {','.join(values)};"
        db.execute(sql=sql, log=False)

    crs = CRS.from_epsg(srid)

    now = datetime.now()
    dt = now.strftime("%Y-%m-%d %H:%M:%S")

    try:
        response = requests.get(file_url, timeout=None)

        if not response.ok:
            msg = f"access to url {file_url} is currently unavailable"
            _insert_into_file_log_table(db=db, filename="", status=0, msg=msg, dt=dt)
            return

        df = pd.read_csv(StringIO(response.text))
        df = df.drop_duplicates(subset=["id"], keep="last")

        logger.debug(df.size)

        if limit:
            df = df[:limit]

        # active fires geodataframe
        gdf_points = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.lon, df.lat), crs=crs
        )

        src = file_url.replace(base_url, "")

        db.truncate(table=_ACTIVE_FIRES_TODAY_DATA_TABLE_FQN)

        for biome in biome_list:
            # biome border geodataframe
            sql = f"""
                SELECT geom
                FROM public.biome_border
                WHERE biome='{biome}' 
            """

            gdf_biome = gpd.read_postgis(sql, db.conn, geom_col="geom")

            assert (
                not gdf_biome.empty
            ), f"Missing biome border geometry for biome='{biome}'"

            border = gdf_biome.geometry.iloc[0]

            gdf = gdf_points[gdf_points.geometry.within(border)]

            values = []
            for row in gdf.itertuples(index=False):
                uuid = row.id
                view_date = row.data_hora_gmt
                satelite = row.satelite
                geom_wkt = row.geometry.wkt
                values.append(
                    f"(ST_PointFromText('{geom_wkt}', {srid}),'{uuid}','{biome}','{view_date}','{view_date}','{satelite}','{src}')"
                )

                if len(values) > 1e5:
                    _insert_into_data_table(db=db, values=values)
                    values = []

            if len(values) > 0:
                _insert_into_data_table(db=db, values=values)

            sql = f"""
                UPDATE {_ACTIVE_FIRES_TODAY_DATA_TABLE_FQN} AS af
                SET geocode=mun.geocode, municipio=mun.name
                FROM public.municipalities_biome mub
                JOIN public.municipalities mun
	                ON mun.geocode=mub.geocode
                WHERE
	                mub.biome='{biome}'
	                AND af.geom && mun.geometry
	                AND ST_Within(af.geom, mun.geometry);
            """

            db.execute(sql)

        msg = "file processed successfully"
        status = 1

    except Exception as exc:
        msg = f"unexpected error while processing active fires file: {exc}"
        status = 0

    _insert_into_file_log_table(
        db=db,
        filename=file_url.replace(base_url, ""),
        status=status,
        msg=msg,
        dt=dt,
    )


def update_active_fires_today(
    db: DatabaseFacade,
    base_url: str,
    biome_list: list,
    limit: int,
):
    logger.info("updating the active_fires_today table")

    index_columns = [
        "view_date:btree",
        "biome:btree",
        "geocode:btree",
        "biome,view_date:btree",
        "view_date,id,biome:btree",
        "geom:gist",
    ]

    prepare_table_to_update(
        db=db, schema="fires", name="active_fires_today", columns=index_columns
    )

    file_url = get_latest_active_fires_today_file_url(db=db, base_url=base_url)

    if not file_url:
        return

    process_active_fires_today_file(
        db=db, base_url=base_url, file_url=file_url, biome_list=biome_list, limit=limit
    )

    optimize_table(
        db=db, schema="fires", name="active_fires_today", columns=index_columns
    )
