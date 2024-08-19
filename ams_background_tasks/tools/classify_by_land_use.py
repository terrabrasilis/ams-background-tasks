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
    ACTIVE_FIRES_CLASSNAME,
    ACTIVE_FIRES_INDICATOR,
    DETER_INDICATOR,
    INDICATORS,
    PIXEL_LAND_USE_AREA,
    RISK_CLASSNAME,
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
@click.option(
    "--indicators",
    type=str,
    required=True,
    default=INDICATORS,
    multiple=True,
    help=f"Indicator list ({', '.join(INDICATORS)})",
)
@click.option(
    "--drop-tmp", is_flag=True, default=False, help="Drop the temporary tables."
)
def main(
    db_url: str,
    land_use_dir: str,
    all_data: bool,
    biome: tuple,
    drop_tmp: bool,
    indicators: tuple,
):
    """Cross the indicators with the land use image and group them by spatial units."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(land_use_dir)
    logger.debug(biome)

    reset_land_use_tables(db_url=db_url, is_temp=True)

    assert Path(land_use_dir).exists()

    if DETER_INDICATOR in indicators:
        logger.info("processing deter")
        process_deter(
            db_url=db_url,
            biome_list=list(biome),
            land_use_dir=Path(land_use_dir),
        )

    if ACTIVE_FIRES_INDICATOR in indicators:
        logger.info("processing active fires")
        process_active_fires(
            db_url=db_url,
            biome_list=list(biome),
            land_use_dir=Path(land_use_dir),
            all_data=all_data,
        )

    percentage_calculation_for_areas(db_url=db_url, is_temp=True)
    reset_land_use_tables(db_url=db_url, is_temp=False)
    copy_data_to_final_tables(db_url=db_url, all_data=all_data, indicators=indicators)

    if drop_tmp:
        drop_tmp_tables(db_url=db_url, indicators=indicators)


def create_land_structure_table(db_url: str, table: str, force_recreate: bool):
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
            "UNIQUE (gid, biome, land_use_id)",
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


def insert_data_in_land_use_tables(
    data: gpd.GeoDataFrame, db: DatabaseFacade, table_prefix: str, log: bool = False
):
    for spatial_unit in read_spatial_units(db=db):
        tmpspatial_unit = f"{table_prefix}{spatial_unit}"
        logger.info("processing %s ...", tmpspatial_unit)

        spatial_units = gpd.GeoDataFrame.from_postgis(
            sql=f'SELECT suid, geometry FROM "{spatial_unit}"',
            con=db.conn,
            geom_col="geometry",
        )

        logger.info("joining...")
        join = gpd.sjoin(data, spatial_units, how="inner", predicate="intersects")

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

        values: list = []
        with alive_bar(len(group)) as progress_bar:
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

        assert len(values) > 0

        sql = f"""
            INSERT INTO "{tmpspatial_unit}_land_use" (
                suid, land_use_id, classname, "date", area, geocode, biome
            ) 
            VALUES {','.join(values)};
        """

        logger.info("inserting data into %s_land_use", tmpspatial_unit)
        db.execute(sql=sql, log=log)


def process_active_fires(
    db_url: str, biome_list: list, land_use_dir: Path, all_data: bool
):
    for index, biome in enumerate(biome_list):
        logger.info("processing biome %s", biome)
        assert is_valid_biome(biome=biome)

        land_use_image = land_use_dir / f"{biome}_land_use.tif"
        logger.debug(land_use_image)
        assert land_use_image.exists()

        process_active_fires_land_structure(
            db_url=db_url,
            is_temp=True,
            land_use_image=land_use_image,
            biome=biome,
            all_data=all_data,
            force_recreate=not index,
        )

    insert_fires_in_land_use_tables(db_url=db_url, is_temp=True)


def process_active_fires_land_structure(
    is_temp: bool,
    land_use_image: Path,
    db_url: str,
    biome: str,
    all_data: bool,
    force_recreate: bool,
):
    db = DatabaseFacade.from_url(db_url=db_url)

    spatial_units = read_spatial_units(db=db)

    table_prefix = get_prefix(is_temp=is_temp)

    table = f"{table_prefix}fires_land_structure"
    where = f"WHERE biome='{biome}'"

    if all_data:
        create_land_structure_table(
            db_url=db_url, table=table, force_recreate=force_recreate
        )
    else:
        where += f"""
            AND view_date > (SELECT MAX(date) FROM "{list(spatial_units.keys())[0]}_land_use" WHERE classname='{ACTIVE_FIRES_CLASSNAME}')
        """

    landuse_raster = rio.open(land_use_image)

    sql = f"""
        SELECT id as gid, biome, geocode, geom
        FROM fires.active_fires
        {where}
    """
    logger.debug(sql)
    fires = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.conn, geom_col="geom")
    coord_list = list(zip(fires["geom"].x, fires["geom"].y))

    fires["value"] = list(landuse_raster.sample(coord_list))
    values: list = []

    with alive_bar(len(fires)) as progress_bar:
        for _, point in fires.iterrows():
            if point["value"][0] > 0:
                values.append(
                    f"('{point.gid}', '{point.biome}', '{point.geocode}', {point['value'][0]}, 1)"
                )
            progress_bar()
    logger.debug("len(values): %s", len(values))

    assert len(values) > 0

    sql = f"""
        INSERT INTO {table_prefix}fires_land_structure (gid, biome, geocode, land_use_id, num_pixels)
        VALUES {','.join(values)};
    """

    logger.info("inserting into %sfires_land_structure", table_prefix)

    db.execute(sql=sql, log=False)


def insert_fires_in_land_use_tables(db_url: str, is_temp: bool):
    logger.info("Insert ACTIVE FIRES data in land use tables for each spatial units.")

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    data = gpd.GeoDataFrame.from_postgis(
        sql=f"""
            SELECT
                a.id,
                a.land_use_id,
                a.num_pixels,
                a.biome,
                a.geocode,
                '{ACTIVE_FIRES_CLASSNAME}' as classname,
                b.view_date AS date,
                b.geom AS geometry
            FROM
                {table_prefix}fires_land_structure a 
            INNER JOIN
                fires.active_fires b ON a.gid = b.id::text AND a.biome = b.biome;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    insert_data_in_land_use_tables(
        db=db, data=data, table_prefix=table_prefix, log=False
    )


def process_deter(db_url: str, biome_list: list, land_use_dir: Path):
    for index, biome in enumerate(biome_list):
        logger.info("processing biome %s", biome)
        assert is_valid_biome(biome=biome)

        land_use_image = land_use_dir / f"{biome}_land_use.tif"
        logger.debug(land_use_image)
        assert land_use_image.exists()

        process_deter_land_structure(
            db_url=db_url,
            is_temp=True,
            land_use_image=land_use_image,
            biome=biome,
            force_recreate=not index,
        )

    insert_deter_in_land_use_tables(db_url=db_url, is_temp=True)


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
        create_land_structure_table(
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

    values: list = []

    with alive_bar(len(deter)) as progress_bar:
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

    assert len(values) > 0

    sql = f"""
        INSERT INTO {table_prefix}deter_land_structure (gid, biome, geocode, land_use_id, num_pixels)
        VALUES {','.join(values)};
    """

    logger.info("inserting into %sdeter_land_structure", table_prefix)
    db.execute(sql=sql, log=False)


def insert_deter_in_land_use_tables(db_url: str, is_temp: bool):
    logger.info("Insert DETER data in land use tables for each spatial units.")

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    data = gpd.GeoDataFrame.from_postgis(
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
                class c ON b.classname = c.name
            INNER JOIN 
                class_group d ON c.group_id = d.id;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    insert_data_in_land_use_tables(db=db, data=data, table_prefix=table_prefix, log=False)


def percentage_calculation_for_areas(db_url: str, is_temp: bool):
    logger.info("using spatial units and deter areas to calculate the percentages")

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
                    '{ACTIVE_FIRES_CLASSNAME}','{RISK_CLASSNAME}'
                )
        """
        db.execute(sql)


def copy_data_to_final_tables(db_url: str, all_data: bool, indicators: list):
    logger.info("copying the new processed data to the final tables")

    if DETER_INDICATOR in indicators:
        copy_deter_land_structure(db_url=db_url, all_data=all_data)

    if ACTIVE_FIRES_INDICATOR in indicators:
        copy_fires_land_structure(db_url=db_url, all_data=all_data)

    db = DatabaseFacade.from_url(db_url=db_url)

    for spatial_unit in read_spatial_units(db=db):
        land_use_table = f"{spatial_unit}_land_use"
        tmp_land_use_table = f"tmp_{land_use_table}"

        logger.info("copying from %s to %s.", tmp_land_use_table, land_use_table)

        db.copy_table(src=tmp_land_use_table, dst=land_use_table)


def copy_deter_land_structure(db_url: str, all_data: bool):
    table = "deter_land_structure"

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    db = DatabaseFacade.from_url(db_url=db_url)

    if all_data:
        create_land_structure_table(db_url=db_url, table=table, force_recreate=True)

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


def copy_fires_land_structure(db_url: str, all_data: bool):
    table = "fires_land_structure"

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    db = DatabaseFacade.from_url(db_url=db_url)

    if all_data:
        create_land_structure_table(db_url=db_url, table=table, force_recreate=True)

    # copy data from temporary table
    db.copy_table(src=f"{get_prefix(is_temp=True)}{table}", dst=table)


def drop_tmp_tables(db_url: str, indicators: list):
    """Drop the temporary tables."""
    db = DatabaseFacade.from_url(db_url=db_url)
    for spatial_unit in read_spatial_units(db=db):
        land_use_table = f"tmp_{spatial_unit}_land_use"
        db.drop_table(table=land_use_table)

    if DETER_INDICATOR in indicators:
        db.drop_table(table="tmp_deter_land_structure")

    if ACTIVE_FIRES_INDICATOR in indicators:
        db.drop_table(table="tmp_fires_land_structure")
