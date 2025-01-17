"""Calculate the land use area and insert the results into database."""

# pylint: disable=not-callable

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import rasterio as rio
from alive_progress import alive_bar
from rasterio.mask import mask
from shapely.geometry import mapping

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    PIXEL_LAND_USE_AREA,
    is_valid_biome,
    read_spatial_units,
)

logger = get_logger(__name__, sys.stdout)


@click.command()
@click.option(
    "--db-url",
    required=False,
    type=str,
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--land-use-image",
    required=True,
    type=click.Path(exists=True, resolve_path=True, file_okay=True),
    help="Land use image path.",
)
@click.option("--biome", type=str, required=True, help="Biome.", multiple=True)
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="Force to recreate the land use tables.",
)
def main(
    db_url: str,
    land_use_image: str,
    biome: tuple,
    force_recreate: bool,
):
    """Create the land use tables, calculate the land use area and insert the results into database."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(land_use_image)
    assert Path(land_use_image).exists()

    logger.debug(biome)

    db = DatabaseFacade.from_url(db_url=db_url)

    create_land_use_area_tables(db=db, force_recreate=force_recreate)

    for _biome in list(biome):
        logger.info("processing biome %s", _biome)

        assert is_valid_biome(biome=_biome)

        populate_municipalities_biome_intersection(db=db, biome=_biome)
        calculate_land_use_area(db=db, land_use_image=land_use_image, biome=_biome)


def create_land_use_area_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the tables required for calculating land use area."""
    schema = "public"

    # {}_land_use_area
    for spatial_unit, _ in read_spatial_units(db=db).items():
        name = f"{spatial_unit}_land_use_area"

        # if db.table_exist(schema=schema, table=name) and not force_recreate:
        #    continue

        db.create_table(
            schema=schema,
            name=name,
            columns=[
                "id serial NOT NULL PRIMARY KEY",
                "suid int4",
                "land_use_id int4",
                "counts int4",
                "area double precision",
                "biome varchar(254)",
                "geocode varchar(80)",
                "UNIQUE (id, land_use_id, geocode, biome)",
            ],
            force_recreate=True,
        )

        db.create_indexes(
            schema=schema,
            name=name,
            columns=[
                "suid:btree",
                "land_use_id:btree",
                "biome:btree",
                "geocode:btree",
            ],
            force_recreate=True,
        )

    # municipalities_biome_intersection table
    name = "municipalities_biome_intersection"

    if not db.table_exist(schema=schema, table=name) or force_recreate:
        db.create_table(
            schema=schema,
            name=name,
            columns=[
                "id serial PRIMARY KEY",
                "geocode character varying(80)",
                "biome character varying(254)",
                "geometry geometry(MultiPolygon, 4674)",
            ],
            force_recreate=force_recreate,
        )

        db.create_indexes(
            schema=schema,
            name=name,
            columns=["geocode:btree", "biome:btree", "geometry:gist"],
            force_recreate=force_recreate,
        )


def populate_municipalities_biome_intersection(db: DatabaseFacade, biome: str = ""):
    """Intersecting municipalities with biomes."""
    schema = "public"
    name = "municipalities_biome_intersection"

    rows = db.count_rows(
        table=f"{schema}.{name}", conditions=f"biome='{biome}'" if biome else ""
    )

    logger.debug(rows)

    if not rows:
        where = f"AND bb.biome='{biome}'" if biome else ""

        sql = f"""
            INSERT INTO {schema}.{name} (geocode, biome, geometry)
            SELECT 
                mu.geocode, 
                bb.biome, 
                ST_Multi(ST_CollectionExtract(ST_Intersection(bb.geom, mu.geometry),3)) AS geom
            FROM 
                public.municipalities mu
            INNER JOIN 
                public.municipalities_biome mb ON mu.geocode = mb.geocode
            INNER JOIN 
                public.biome_border bb ON mb.biome = bb.biome
            WHERE 
	            ST_IsValid(ST_Intersection(bb.geom, mu.geometry))
	            AND NOT ST_IsEmpty(ST_Intersection(bb.geom, mu.geometry))
	            {where};
        """

        db.execute(sql)


def calculate_land_use_area(db: DatabaseFacade, land_use_image: Path, biome: str):
    """Calculate the land use area and insert the results into database."""

    def _insert_into_land_use_area(db: DatabaseFacade, table_prefix: str, values: list):
        sql = f"""
            INSERT INTO
                public.{table_prefix}_land_use_area (suid, land_use_id, counts, area, biome, geocode)
            VALUES {','.join(list(set(values)))};
        """
        logger.info("inserting into land_use_area")
        db.execute(sql=sql, log=False)

    with rio.open(land_use_image) as raster:
        for table, col_id in read_spatial_units(db=db).items():
            # if table != "states":
            #    print(f"ignoring {table}")
            #    continue

            logger.info('calculating the land use are for "%s" spatial units', table)

            rows = db.count_rows(
                table=f"public.{table}_land_use_area",
                conditions=f"biome='{biome}'" if biome else "",
            )

            logger.debug(rows)

            if rows > 0:
                logger.info(
                    "ignoring new insertion because the table '%s_land_use_area' already contains data.",
                    table,
                )
                continue

            join = (
                "sub.geocode = su.geocode"
                if table == "municipalities"
                else f"sub.{col_id} = su.{col_id}"
            )

            spatial_units_query = f"""
                SELECT
                    su.suid,
                    mbi.geocode,
                    mbi.biome,
                    ST_Multi(ST_CollectionExtract(ST_Intersection(su.geometry, mbi.geometry), 3)) AS geom
                FROM
                    public.{table} su
                INNER JOIN
                    public.{table}_biome sub ON {join}
                INNER JOIN
                    public.municipalities_biome_intersection mbi ON sub.biome = mbi.biome
                WHERE 
                    ST_IsValid(ST_CollectionExtract(ST_Intersection(su.geometry, mbi.geometry),3))
                    AND NOT ST_IsEmpty(ST_CollectionExtract(ST_Intersection(su.geometry, mbi.geometry),3))
                    AND sub.biome = '{biome}';
            """

            logger.debug(spatial_units_query)

            spatial_units = gpd.read_postgis(
                spatial_units_query, con=db.conn, geom_col="geom"
            )

            logger.debug(len(spatial_units))

            values = []

            with alive_bar(len(spatial_units)) as progress_bar:
                for _, row in spatial_units.iterrows():
                    geometry = [mapping(row["geom"])]
                    out_image, _ = mask(raster, geometry, crop=True)
                    out_image[0][out_image[0] == 255] = 0
                    unique, counts = np.unique(
                        out_image[out_image > 0], return_counts=True
                    )
                    for land_use_id, count in tuple(
                        zip(unique.tolist(), counts.tolist())
                    ):
                        suid = row["suid"]
                        geocode = row["geocode"]
                        biome = row["biome"]

                        values.append(
                            f"({suid}, {land_use_id}, {count}, {count * PIXEL_LAND_USE_AREA}, '{biome}', '{geocode}')"
                        )
                    if len(values) >= 1e4:  # optimizing insertion
                        _insert_into_land_use_area(
                            db=db, values=values, table_prefix=table
                        )
                        values = []

                    progress_bar()

                if len(values) > 0:
                    _insert_into_land_use_area(db=db, values=values, table_prefix=table)
                    values = []
