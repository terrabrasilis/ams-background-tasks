"""Update the inpe risk indicator."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
from shapely.geometry import Point

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import AMAZONIA, is_valid_biome
from ams_background_tasks.tools.risk_utils import (
    RISK_SRC_INPE,
    get_last_risk_file_info,
    get_risk_date_id,
    mark_risk_file_as_used,
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
    "--risk-threshold",
    type=float,
    default=0.80,
    help="A threshold used to determine whether a point is considered a risk.",
)
@click.option("--srid", type=int, default=4674, help="SRID of the risk points.")
@click.option("--biome", type=str, required=True, help="Biome.")
def main(db_url: str, risk_threshold: float, srid: str, biome: str):
    """Update the ibama risk indicator."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    assert is_valid_biome(biome=biome)
    assert biome == AMAZONIA

    update_risk_file(
        db_url=db_url, risk_threshold=risk_threshold, srid=srid, biome=biome
    )


def update_risk_file(
    db_url: str, risk_threshold: float, srid: str, biome: str
):  # pylint:disable=too-many-statements
    """Update the ibama risk indicator."""
    assert biome == AMAZONIA

    schema = "risk"
    risk_tmp = "risk_tmp_inpe"
    risk_geom = "risk_matrix_inpe"
    risk_data = "risk_data_inpe"
    risk_date = "risk_image_date"

    def _insert_into_tmp(values: list):
        sql = f"""
            INSERT INTO {schema}.{risk_tmp} (geometry, data)
            VALUES {','.join(values)};
        """
        db.execute(sql=sql, log=False)

    db = DatabaseFacade.from_url(db_url=db_url)

    # creating view
    view = "public.last_risk_data_inpe"

    sql = f"DROP VIEW IF EXISTS {view}"
    db.execute(sql)

    sql = f"""
        CREATE VIEW {view} AS
        SELECT
            geo.id,
            geo.geom,
            wd.risk,
            dt.risk_date AS view_date
        FROM
            risk.{risk_data} wd,
            risk.{risk_date} dt,
            risk.{risk_geom} geo
        WHERE
            wd.date_id = (
                SELECT {risk_date}.id
                FROM risk.{risk_date}
                ORDER BY {risk_date}.expiration_date DESC
                LIMIT 1
            )
            AND wd.geom_id=geo.id
            AND wd.date_id=dt.id;
    """

    db.execute(sql)

    # last risk file
    risk_file, _ = get_last_risk_file_info(db=db, is_new=True, src=RISK_SRC_INPE)
    logger.info(risk_file)

    if risk_file is None or not Path(risk_file).exists():
        logger.info("There is no new risk file")
        return

    risk_date_id = get_risk_date_id(db=db, file_name=risk_file)
    assert risk_date_id

    # updating the tmp table
    dfr = gpd.GeoDataFrame()
    with rio.open(risk_file) as dataset:
        val = dataset.read(dataset.meta.get("count"))
        logger.info("reading band %s", dataset.meta.get("count"))

        _indices = np.where(val > risk_threshold)
        indices = list(zip(_indices[0], _indices[1]))

        logger.debug(len(indices))

        geometry = [Point(dataset.xy(x, y)[0], dataset.xy(x, y)[1]) for x, y in indices]
        v = [val[x, y] for x, y in indices]

        dfr = gpd.GeoDataFrame({"geometry": geometry, "data": v}, crs=dataset.crs)
        dfr = dfr.to_crs(crs=srid)

    db.truncate(table=f"{schema}.{risk_tmp}", cascade=True)

    values = []
    for _, row in dfr.iterrows():
        values.append(
            f"(ST_PointFromText('{row['geometry'].wkt}',{srid}),{row['data']})"
        )
        if len(values) > 1e5:
            _insert_into_tmp(values=values)
            values = []

    if len(values) > 0:
        _insert_into_tmp(values=values)

    db.truncate(table=f"{schema}.{risk_geom}", cascade=True, restart=True)

    sql = f"""
        INSERT INTO {schema}.{risk_geom}(geom)
        SELECT risk_tmp.geometry
        FROM {schema}.{risk_tmp} as risk_tmp, public.biome_border as border
        WHERE ST_Intersects(risk_tmp.geometry, border.geom) AND border.biome='{biome}';
    """

    db.execute(sql)

    # updating the weekly_data
    db.truncate(table=f"{schema}.{risk_data}")

    sql = f"""
        INSERT INTO {schema}.{risk_data} (date_id, geom_id, risk, biome)
        SELECT {risk_date_id}, geom.id, risk_tmp.data, '{biome}'
        FROM {schema}.{risk_tmp} risk_tmp, {schema}.{risk_geom} geom
        WHERE ST_Equals(risk_tmp.geometry, geom.geom);
    """

    db.execute(sql)

    # intersecting with municipalities
    logger.info("intersecting with municipalities")

    table1 = f"{schema}.{risk_data}"  # risk_data_inpe
    table2 = f"{schema}.{risk_geom}"  # risk_matrix_inpe

    sql = f"""
        UPDATE {table1} AS rk
        SET geocode = a.geocode	
        FROM (
            SELECT 
                rk2.id, rk2.biome, mun.geocode, rkg.id AS geom_id
            FROM 
                {table1} AS rk2
            JOIN
                {table2} rkg
                ON rk2.geom_id=rkg.id                
            JOIN 
                public.municipalities_biome mub
                ON rk2.biome=mub.biome
            JOIN 
                public.municipalities mun
                ON mun.geocode=mub.geocode
                AND rkg.geom && mun.geometry
                AND ST_Within(rkg.geom, mun.geometry)
        ) AS a
        WHERE 
            rk.id=a.id
            AND rk.biome=a.biome
            AND rk.geom_id=a.geom_id;
    """

    db.execute(sql)

    # mark risk file as used
    mark_risk_file_as_used(db=db, file_name=risk_file)
