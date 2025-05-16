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
    AMAZONIA,
    AMS,
    DETER_INDICATOR,
    INDICATORS,
    LAND_USE_TYPES,
    PIXEL_LAND_USE_AREA,
    RISK_IBAMA_CLASSNAME,
    RISK_IBAMA_INDICATOR,
    RISK_INDICATORS,
    RISK_INPE_CLASSNAME,
    RISK_INPE_INDICATOR,
    get_prefix,
    is_valid_biome,
    is_valid_indicator,
    read_spatial_units,
)

OPT_MAX_VALUES = 5e4
RISK_INPE_SCALE_FACTOR = 1e5

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
    "--land-use-type",
    required=True,
    type=click.Choice(LAND_USE_TYPES),
    help="Land use categories type.",
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
    "--indicator",
    type=str,
    required=True,
    help=f"Indicator ({', '.join(INDICATORS)})",
)
def main(
    *,
    db_url: str,
    land_use_dir: str,
    land_use_type: str,
    all_data: bool,
    biome: tuple,
    indicator: str,
):
    """Cross the indicators with the land use image and group them by spatial units."""
    assert all_data
    assert is_valid_indicator(indicator=indicator)

    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(land_use_dir)
    logger.debug(biome)

    assert Path(land_use_dir).exists()

    if indicator == DETER_INDICATOR:
        logger.info("processing deter")
        process_deter(
            db_url=db_url,
            biome_list=list(biome),
            land_use_dir=Path(land_use_dir),
            land_use_type=land_use_type,
        )

    if indicator == ACTIVE_FIRES_INDICATOR:
        logger.info("processing active fires")
        process_active_fires(
            db_url=db_url,
            biome_list=list(biome),
            land_use_dir=Path(land_use_dir),
            land_use_type=land_use_type,
        )

    if AMAZONIA in list(biome) and indicator in RISK_INDICATORS:
        process_risk(
            db_url=db_url,
            land_use_dir=Path(land_use_dir),
            land_use_type=land_use_type,
            indicator=indicator,
        )


def insert_data_in_land_use_tables(
    *,
    indicator: str,
    data: gpd.GeoDataFrame,
    db: DatabaseFacade,
    table_prefix: str,
    risk: bool,
    log: bool,
    land_use_type: str,
):
    def _insert_into_land_use(
        db: DatabaseFacade, spatial_unit: str, measure: str, log: bool, values: list
    ):
        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
        risk_column = ",risk" if risk else ""
        sql = f"""
            INSERT INTO "{spatial_unit}_land_use{land_use_type_suffix}" (
                suid, land_use_id, classname, "date", {measure}, biome, geocode {risk_column}
            )
            VALUES {','.join(values)};
        """

        logger.info("inserting data into %s_land_use_%s", spatial_unit, land_use_type)
        db.execute(sql=sql, log=log)

    logger.debug(indicator)

    assert is_valid_indicator(indicator=indicator)

    measure = None
    multiplier = 1.0
    if indicator == DETER_INDICATOR:
        measure = "area"
        multiplier = PIXEL_LAND_USE_AREA
    elif indicator == ACTIVE_FIRES_INDICATOR or indicator is RISK_IBAMA_INDICATOR:
        measure = "counts"
    elif indicator == RISK_INPE_INDICATOR:
        measure = "counts"
    else:
        assert False

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
                    "biome",
                    "geocode",
                    "num_pixels",
                ]
                + (["risk"] if risk else [])
            ]
            .groupby(
                ["suid", "land_use_id", "classname", "date", "biome", "geocode"]
                + (["risk"] if risk else [])
            )["num_pixels"]
            .sum()
        )

        values: list = []
        count_values: int = 0

        with alive_bar(len(group)) as progress_bar:
            for key, value in group.items():
                values.append(
                    f"""
                        (
                            {key[0]},
                            {key[1]},
                            '{key[2]}',
                            TIMESTAMP '{key[3].year}-{key[3].month}-{key[3].day}',
                            {value * multiplier},
                            '{key[4]}',
                            '{key[5]}'
                            {(','+str(key[6]) if risk else '')}
                        ) 
                    """
                )
                count_values += 1

                progress_bar()

                if len(values) >= OPT_MAX_VALUES:  # optimizing
                    logger.info("inserting data into %s_land_use", tmpspatial_unit)
                    _insert_into_land_use(
                        db=db,
                        spatial_unit=tmpspatial_unit,
                        measure=measure,
                        values=values,
                        log=log,
                    )
                    values = []

        if len(values) > 0:  # optimizing
            _insert_into_land_use(
                db=db,
                spatial_unit=tmpspatial_unit,
                measure=measure,
                values=values,
                log=log,
            )

        logger.debug("len(values): %s", count_values)
        # assert count_values > 0


def process_active_fires(
    db_url: str, biome_list: list, land_use_dir: Path, land_use_type: str
):
    for biome in biome_list:
        logger.info("processing biome %s", biome)
        assert is_valid_biome(biome=biome)

        land_use_image = land_use_dir / land_use_type / f"{biome}_land_use.tif"
        logger.debug(land_use_image)
        assert land_use_image.exists()

        process_active_fires_land_structure(
            db_url=db_url,
            is_temp=True,
            land_use_image=land_use_image,
            land_use_type=land_use_type,
            biome=biome,
        )

    insert_fires_in_land_use_tables(
        db_url=db_url, is_temp=True, land_use_type=land_use_type
    )


def process_active_fires_land_structure(
    is_temp: bool,
    land_use_image: Path,
    land_use_type: str,
    db_url: str,
    biome: str,
):
    def _insert_into_active_fires_land_structure(
        db: DatabaseFacade, table_prefix: str, values
    ):
        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
        sql = f"""
            INSERT INTO {table_prefix}fires_land_structure{land_use_type_suffix} (gid, biome, geocode, land_use_id, num_pixels)
            VALUES {','.join(values)};
        """
        logger.info(
            "inserting into %sfires_land_structure_%s", table_prefix, land_use_type
        )
        db.execute(sql=sql, log=False)

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    sql = f"""
        SELECT id as gid, biome, geocode, geom
        FROM fires.active_fires
        WHERE biome='{biome}' AND geocode IS NOT NULL
    """

    logger.debug(sql)

    fires = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.conn, geom_col="geom")
    coord_list = list(zip(fires["geom"].x, fires["geom"].y))

    landuse_raster = rio.open(land_use_image)
    assert landuse_raster.nodata == 255

    fires["value"] = list(landuse_raster.sample(coord_list))
    values: list = []
    count_values: int = 0

    with alive_bar(len(fires)) as progress_bar:
        for _, point in fires.iterrows():
            assert point.biome == biome
            assert point.geocode

            if point["value"][0] > 0:
                values.append(
                    f"('{point.gid}', '{point.biome}', '{point.geocode}', {point['value'][0]}, 1)"
                )
                count_values += 1

            progress_bar()

            if len(values) >= OPT_MAX_VALUES:  # optimizing
                _insert_into_active_fires_land_structure(
                    db=db, table_prefix=table_prefix, values=values
                )
                values = []

    if len(values) > 0:
        _insert_into_active_fires_land_structure(
            db=db, table_prefix=table_prefix, values=values
        )
        values = []

    logger.debug("len(values): %s", count_values)
    assert count_values > 0


def insert_fires_in_land_use_tables(db_url: str, is_temp: bool, land_use_type: str):
    logger.info("Insert ACTIVE FIRES data in land use tables for each spatial units.")

    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    data = gpd.GeoDataFrame.from_postgis(
        sql=f"""
            SELECT DISTINCT
                a.id,
                a.land_use_id,
                a.num_pixels,
                a.biome,
                a.geocode,
                '{ACTIVE_FIRES_CLASSNAME}' as classname,
                b.view_date AS date,
                b.geom AS geometry
            FROM
                {table_prefix}fires_land_structure{land_use_type_suffix} a 
            INNER JOIN
                fires.active_fires b ON a.gid = b.id::text AND a.biome = b.biome AND a.geocode = b.geocode;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    insert_data_in_land_use_tables(
        indicator=ACTIVE_FIRES_INDICATOR,
        db=db,
        data=data,
        table_prefix=table_prefix,
        log=False,
        risk=False,
        land_use_type=land_use_type,
    )


def process_deter(
    db_url: str, biome_list: list, land_use_dir: Path, land_use_type: str
):
    for biome in biome_list:
        logger.info("processing biome %s", biome)
        assert is_valid_biome(biome=biome)

        land_use_image = land_use_dir / land_use_type / f"{biome}_land_use.tif"
        logger.debug(land_use_image)
        assert land_use_image.exists()

        process_deter_land_structure(
            db_url=db_url,
            is_temp=True,
            land_use_image=land_use_image,
            land_use_type=land_use_type,
            biome=biome,
        )

    # inserting data in land use table
    insert_deter_in_land_use_tables(
        db_url=db_url, is_temp=True, land_use_type=land_use_type
    )


def process_deter_land_structure(
    is_temp: bool,
    land_use_image: Path,
    land_use_type: str,
    db_url: str,
    biome: str,
):
    def _insert_into_deter_land_structure(
        db: DatabaseFacade, table_prefix: str, values
    ):
        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

        sql = f"""
            INSERT INTO {table_prefix}deter_land_structure{land_use_type_suffix} (gid, biome, geocode, land_use_id, num_pixels)
            VALUES {','.join(list(set(values)))};
        """
        logger.info(
            "inserting into %sdeter_land_structure%s",
            table_prefix,
            land_use_type_suffix,
        )
        db.execute(sql=sql, log=False)

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    sql = f"""
        SELECT gid, biome, geocode, geom
        FROM deter.tmp_data
        WHERE biome='{biome}' AND geocode IS NOT NULL    
    """

    logger.debug(sql)
    deter = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.conn, geom_col="geom")
    logger.debug("len(deter): %s", len(deter))

    landuse_raster = rio.open(land_use_image)
    assert landuse_raster.nodata == 255

    values: list = []
    count_values: int = 0

    with alive_bar(len(deter)) as progress_bar:
        for _, row in deter.iterrows():
            assert row.biome == biome
            assert row.geocode
            geoms = [mapping(row.geom)]
            out_image, _ = mask(landuse_raster, geoms, crop=True)
            out_image[0][out_image[0] == 255] = 0
            unique, counts = np.unique(
                out_image[0][out_image[0] > 0], return_counts=True
            )
            unique_counts = np.asarray((unique, counts)).T
            counts = pd.DataFrame(unique_counts)
            for _, count in counts.iterrows():
                if count[0] > 0:
                    values.append(
                        f"('{row.gid}', '{row.biome}', '{row.geocode}', {count[0]}, {count[1]})"
                    )
                    count_values += 1

            progress_bar()

            if len(values) >= OPT_MAX_VALUES:  # optimizing
                _insert_into_deter_land_structure(
                    db=db, table_prefix=table_prefix, values=values
                )
                values = []

    if len(values) > 0:
        _insert_into_deter_land_structure(
            db=db, table_prefix=table_prefix, values=values
        )

    logger.debug("len(values): %s", count_values)
    assert count_values > 0


def insert_deter_in_land_use_tables(db_url: str, is_temp: bool, land_use_type: str):
    logger.info("Insert DETER data in land use tables for each spatial units.")

    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    data = gpd.GeoDataFrame.from_postgis(
        sql=f""" 
            SELECT DISTINCT
                a.id, 
                a.land_use_id, 
                a.num_pixels,
                a.biome,
                a.geocode,
                d.name AS classname, 
                b.date, 
                b.geom AS geometry
            FROM 
                {table_prefix}deter_land_structure{land_use_type_suffix} a
            INNER JOIN (
                SELECT 
                    tb.gid, 
                    tb.date, 
                    ST_PointOnSurface(tb.geom) AS geom, 
                    tb.classname,
                    tb.biome,
                    tb.geocode
                FROM (
                    SELECT 
                        gid, 
                        date, 
                        classname, 
                        geom,
                        biome,
                        geocode
                    FROM 
                        deter.deter_auth
                    UNION
                    SELECT 
                        gid, 
                        date, 
                        classname, 
                        geom,
                        biome,
                        geocode
                    FROM 
                        deter.deter_history
                ) AS tb
            ) b ON a.gid = b.gid AND a.biome = b.biome AND a.geocode = b.geocode
            INNER JOIN 
                class c ON b.classname = c.name
            INNER JOIN 
                class_group d ON c.group_id = d.id;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    insert_data_in_land_use_tables(
        indicator=DETER_INDICATOR,
        db=db,
        data=data,
        table_prefix=table_prefix,
        log=False,
        risk=False,
        land_use_type=land_use_type,
    )


def process_risk(db_url: str, land_use_dir: Path, land_use_type: str, indicator: str):
    assert indicator in RISK_INDICATORS

    biome = AMAZONIA

    land_use_image = land_use_dir / land_use_type / f"{biome}_land_use.tif"
    logger.debug(land_use_image)
    assert land_use_image.exists()

    process_risk_land_structure(
        db_url=db_url,
        is_temp=True,
        land_use_image=land_use_image,
        land_use_type=land_use_type,
        biome=biome,
        indicator=indicator,
    )

    insert_risk_in_land_use_tables(
        db_url=db_url, is_temp=True, land_use_type=land_use_type, indicator=indicator
    )


def process_risk_land_structure(
    is_temp: bool,
    land_use_image: Path,
    land_use_type: str,
    db_url: str,
    biome: str,
    indicator: str,
):
    def _insert_into_risk_land_structure(db: DatabaseFacade, table_prefix: str, values):
        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

        sql = f"""
            INSERT INTO {table_prefix}risk_land_structure{land_use_type_suffix} (gid, biome, geocode, land_use_id, num_pixels)
            VALUES {','.join(values)};
        """
        logger.info(
            "inserting into %srisk_land_structure%s", table_prefix, land_use_type_suffix
        )
        db.execute(sql=sql, log=False)

    assert indicator in RISK_INDICATORS

    db = DatabaseFacade.from_url(db_url=db_url)

    risk_geom = (
        "risk_matrix_inpe" if indicator == RISK_INPE_INDICATOR else "matrix_ibama_1km"
    )
    risk_data = "risk_data_inpe" if indicator == RISK_INPE_INDICATOR else "weekly_data"

    table_prefix = get_prefix(is_temp=is_temp)

    sql = f"""
        SELECT rmi.id as gid, rwd.biome, rwd.geocode, rmi.geom, rwd.risk
        FROM risk.{risk_geom} rmi
        JOIN
            risk.{risk_data} rwd
            ON rwd.geom_id=rmi.id
        WHERE rwd.biome='{biome}' AND rwd.geocode IS NOT NULL
    """

    logger.debug(sql)

    risk = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.conn, geom_col="geom")
    coord_list = list(zip(risk["geom"].x, risk["geom"].y))

    landuse_raster = rio.open(land_use_image)
    assert landuse_raster.nodata == 255

    risk["value"] = list(landuse_raster.sample(coord_list))
    values: list = []
    count_values: int = 0

    with alive_bar(len(risk)) as progress_bar:
        for _, point in risk.iterrows():
            assert point.biome == biome
            assert point.geocode

            if point["value"][0] > 0:
                values.append(
                    f"('{point.gid}', '{point.biome}', '{point.geocode}', {point['value'][0]}, 1)"
                )
                count_values += 1

            progress_bar()

            if len(values) >= OPT_MAX_VALUES:  # optimizing
                _insert_into_risk_land_structure(
                    db=db, table_prefix=table_prefix, values=values
                )
                values = []

    if len(values) > 0:
        _insert_into_risk_land_structure(
            db=db, table_prefix=table_prefix, values=values
        )
        values = []

    logger.debug("len(values): %s", count_values)
    # assert count_values > 0


def insert_risk_in_land_use_tables(
    db_url: str, is_temp: bool, land_use_type: str, indicator: str
):
    logger.info("Insert RISK data in land use tables for each spatial units.")

    logger.debug(indicator)

    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    risk_view = (
        "last_risk_data_inpe" if indicator == RISK_INPE_INDICATOR else "last_risk_data"
    )
    risk_classname = (
        RISK_INPE_CLASSNAME
        if indicator == RISK_INPE_INDICATOR
        else RISK_IBAMA_CLASSNAME
    )

    db = DatabaseFacade.from_url(db_url=db_url)

    table_prefix = get_prefix(is_temp=is_temp)

    data = gpd.GeoDataFrame.from_postgis(
        sql=f"""
            SELECT DISTINCT
                a.id,
                a.land_use_id,
                a.num_pixels,
                a.biome,
                a.geocode,
                '{risk_classname}' as classname,
                b.risk,
                b.view_date AS date,
                b.geom AS geometry
            FROM
                {table_prefix}risk_land_structure{land_use_type_suffix} a 
            INNER JOIN
                public.{risk_view} b ON a.gid = b.id::text;
        """,
        con=db.conn,
        geom_col="geometry",
        crs="EPSG:4674",
    )

    insert_data_in_land_use_tables(
        indicator=indicator,
        db=db,
        data=data,
        table_prefix=table_prefix,
        log=False,
        risk=True,
        land_use_type=land_use_type,
    )
