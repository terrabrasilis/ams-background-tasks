"""Cross the indicators with the land use image and group them by spatial units."""

# pylint: disable=not-callable

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from alive_progress import alive_bar
from rasterio.mask import mask
from shapely.geometry import mapping

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    PIXEL_LAND_USE_AREA,
    get_prefix,
    is_valid_biome,
    read_spatial_units,
    reset_land_use_tables,
)

logger = get_logger(__name__, sys.stdout)


@click.command("classify-by-land-use")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--land-use-dir",
    required=True,
    type=click.Path(exists=True, resolve_path=True, dir_okay=True),
    help="Land use image path.",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
@click.option("--biome", type=str, required=True, help="Biome.", multiple=True)
def main(db_url: str, land_use_dir: str, all_data: bool, biome: str):
    """Cross the indicators with the land use image and group them by spatial units."""
    assert not all_data

    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(land_use_dir)
    logger.debug(biome)

    reset_land_use_tables(db_url=db_url, is_temp=True)

    biome_list = list(biome)

    for index, _biome in enumerate(biome_list):
        logger.info("processing biome %s", _biome)
        assert is_valid_biome(biome=_biome)

        land_use_image = Path(land_use_dir) / f"{_biome}_land_use.tif"
        logger.debug(land_use_image)
        assert land_use_image.exists()

        process_deter_land_structure(
            db_url=db_url,
            is_temp=True,
            land_use_image=land_use_image,
            biome=_biome,
            force_recreate=not index,
        )

    insert_deter_in_land_use_tables(db_url=db_url, is_temp=True)
    percentage_calculation_for_areas(db_url=db_url, is_temp=True)
    reset_land_use_tables(db_url=db_url, is_temp=False)
    copy_data_to_final_tables(db_url=db_url, all_data=all_data)


def _create_land_structure_table(db_url: str, table: str, force_recreate: bool):
    logger.info("creating %s.", table)
    logger.debug("%s:%s", "force_recreate", force_recreate)

    db = DatabaseFacade.from_url(db_url=db_url)

    db.create_table(
        schema="public",
        name=table,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "gid varchar(254) NOT NULL",
            "land_use_id int4 NULL",
            "num_pixels int4 NULL",
            "geocode varchar(80) NULL",
            "biome varchar(254) NULL",
            "CONSTRAINT gid_landuse_biome_unique UNIQUE (gid, biome, land_use_id)",
        ],
        force_recreate=force_recreate,
    )

    db.create_indexes(
        schema="public",
        name=table,
        columns=[
            "gid:hash",
            "biome:btree",
            "geocode:btree",
            "gid,biome:btree",
        ],
        force_recreate=force_recreate,
    )


def process_deter_land_structure(
    is_temp: bool,
    land_use_image: Path,
    db_url: str,
    biome: str,
    force_recreate: bool,
):
    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    table = f"{table_prefix}deter_land_structure"
    if force_recreate:
        _create_land_structure_table(
            db_url=db_url, table=table, force_recreate=force_recreate
        )

    logger.info("filling %s.", table)

    sql = f"""
        SELECT gid, biome, geocode, geom
        FROM deter.tmp_data
        WHERE biome='{biome}';
    """
    logger.debug(sql)
    deter = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.conn, geom_col="geom")
    logger.debug("len(deter): %s", len(deter))

    landuse_raster = rio.open(land_use_image)

    with alive_bar(len(deter)) as progress_bar:
        values: list = []
        for _, row in deter.iterrows():
            geoms = [mapping(row.geom)]
            out_image, _ = mask(landuse_raster, geoms, crop=True)
            unique, counts = np.unique(out_image[0], return_counts=True)
            unique_counts = np.asarray((unique, counts)).T
            counts = pd.DataFrame(unique_counts)
            for _, count in counts.iterrows():
                if count[0] > 0:
                    values.append(
                        f"('{row.gid}', '{row.biome}', '{row.geocode}', {count[0]}, {count[1]})"
                    )
            progress_bar()

        logger.debug("len(values): %s", len(values))

        if not len(values):
            # logger....
            return

        sql = f"""
            INSERT INTO {table_prefix}deter_land_structure (gid, biome, geocode, land_use_id, num_pixels)
            VALUES {','.join(values)};
        """

        db.execute(sql=sql, log=False)


def insert_deter_in_land_use_tables(db_url: str, is_temp: bool):
    logger.info("Insert DETER data in land use tables for each spatial units.")

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    land_structure = gpd.GeoDataFrame.from_postgis(
        sql=f""" 
            SELECT 
                a.id, 
                a.land_use_id, 
                a.num_pixels,
                a.biome,
                a.geocode,
                d.name AS classname, 
                b.date, 
                b.geom AS geometry
            FROM 
                {table_prefix}deter_land_structure a
            INNER JOIN (
                SELECT 
                    tb.gid, 
                    tb.date, 
                    ST_PointOnSurface(tb.geom) AS geom, 
                    tb.classname,
                    tb.biome
                FROM (
                    SELECT 
                        gid, 
                        date, 
                        classname, 
                        geom,
                        biome
                    FROM 
                        deter.deter_auth
                    UNION
                    SELECT 
                        gid, 
                        date, 
                        classname, 
                        geom,
                        biome
                    FROM 
                        deter.deter_history
                ) AS tb
            ) b ON a.gid = b.gid AND a.biome = b.biome
            INNER JOIN 
                deter_class c ON b.classname = c.name
            INNER JOIN 
                deter_class_group d ON c.group_id = d.id;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    for spatial_unit in read_spatial_units(db=db):
        tmpspatial_unit = f"{table_prefix}{spatial_unit}"
        logger.info("processing %s ...", tmpspatial_unit)

        spatial_units = gpd.GeoDataFrame.from_postgis(
            sql=f'SELECT suid, geometry FROM "{spatial_unit}"',
            con=db.conn,
            geom_col="geometry",
        )

        logger.info("joining...")
        join = gpd.sjoin(
            land_structure, spatial_units, how="inner", predicate="intersects"
        )

        logger.info("grouping...")
        group = (
            join[
                [
                    "suid",
                    "land_use_id",
                    "classname",
                    "date",
                    "num_pixels",
                    "biome",
                    "geocode",
                ]
            ]
            .groupby(["suid", "land_use_id", "classname", "date", "biome", "geocode"])[
                "num_pixels"
            ]
            .sum()
        )

        with alive_bar(len(group)) as progress_bar:
            values: list = []
            for key, value in group.items():
                values.append(
                    f"""
                        (
                            {key[0]},
                            {key[1]},
                            '{key[2]}',
                            TIMESTAMP '{key[3].year}-{key[3].month}-{key[3].day}',
                            {value * PIXEL_LAND_USE_AREA},
                            '{key[4]}',
                            '{key[5]}'
                        )
                    """
                )
                progress_bar()

            sql = f"""
                INSERT INTO "{tmpspatial_unit}_land_use" (
                    suid, land_use_id, classname, "date", area, geocode, biome
                ) 
                VALUES {','.join(values)};
            """

            db.execute(sql=sql, log=False)


def percentage_calculation_for_areas(db_url: str, is_temp: bool):
    logger.info("Using Spatial Units areas and DETER areas to calculate percentage.")

    fire_classname = "AF"
    risk_classname = "RK"

    db = DatabaseFacade.from_url(db_url=db_url)

    for spatial_unit in read_spatial_units(db=db):
        tmpspatial_unit = f"{get_prefix(is_temp=is_temp)}{spatial_unit}"
        logger.info("processing %s", tmpspatial_unit)

        sql = f"""
            UPDATE public."{tmpspatial_unit}_land_use"
            SET percentage=public."{tmpspatial_unit}_land_use".area/su.area*100
            FROM public."{spatial_unit}" su
            WHERE
                public."{tmpspatial_unit}_land_use".suid=su.suid
                AND public."{tmpspatial_unit}_land_use".classname NOT IN (
                    '{fire_classname}','{risk_classname}'
                )
        """
        db.execute(sql)


def copy_data_to_final_tables(db_url: str, all_data: bool):
    logger.info("copying the new processed data to the final tables")

    _copy_deter_land_structure(db_url=db_url, all_data=all_data)

    db = DatabaseFacade.from_url(db_url=db_url)

    for spatial_unit in read_spatial_units(db=db):
        land_use_table = f"{spatial_unit}_land_use"
        tmp_land_use_table = f"tmp_{land_use_table}"

        logger.info("copying from %s to %s.", tmp_land_use_table, land_use_table)

        db.copy_table(src=tmp_land_use_table, dst=land_use_table)


def _copy_deter_land_structure(db_url: str, all_data: bool):
    table = "deter_land_structure"

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    db = DatabaseFacade.from_url(db_url=db_url)

    if all_data:
        _create_land_structure_table(db_url=db_url, table=table, force_recreate=True)

    else:
        # here, we expect deter.tmp_data to only have DETER data coming from the current table
        # update sequence value from the ams of table id
        sql = """
            DELETE FROM deter_land_structure WHERE gid like '%_curr';
            SELECT setval('public.deter_land_structure_id_seq', (
                SELECT MAX(id) FROM public.deter_land_structure)::integer, true
            );
        """
        db.execute(sql=sql)

    # copy data from temporary table
    db.copy_table(src=f"{get_prefix(is_temp=True)}{table}", dst=table)
