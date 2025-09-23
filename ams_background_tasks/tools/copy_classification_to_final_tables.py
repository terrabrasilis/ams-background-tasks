"""Cross the indicators with the land use image and group them by spatial units."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    ACTIVE_FIRES_CLASSNAME,
    ACTIVE_FIRES_INDICATOR,
    AMS,
    DETER_INDICATOR,
    INDICATORS,
    LAND_USE_TYPES,
    RISK_IBAMA_CLASSNAME,
    RISK_IBAMA_INDICATOR,
    RISK_INPE_CLASSNAME,
    RISK_INPE_INDICATOR,
    RISK_SCALE_FACTOR,
    create_land_structure_table,
    delete_land_use_tables,
    get_prefix,
    read_spatial_units,
)

logger = get_logger(__name__, sys.stdout)


@click.command("copy-classification-to-final-tables")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
@click.option(
    "--indicator",
    type=str,
    required=True,
    help=f"Indicator ({', '.join(INDICATORS)})",
)
@click.option(
    "--drop-tmp", is_flag=True, default=False, help="Drop the temporary tables."
)
@click.option(
    "--land-use-type",
    required=True,
    type=click.Choice(LAND_USE_TYPES),
    help="Land use categories type.",
)
def main(
    db_url: str,
    all_data: bool,
    drop_tmp: bool,
    indicator: str,
    land_use_type: str,
):
    """Copy the classification to final tables."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(indicator)

    db = DatabaseFacade.create(db_url=db_url)

    percentage_calculation_for_areas(db=db, is_temp=True, land_use_type=land_use_type)

    if indicator == RISK_INPE_INDICATOR:
        normalize_inpe_risk(db=db, is_temp=True, land_use_type=land_use_type)

    delete_land_use_tables(
        db=db, land_use_type=land_use_type, is_temp=False, indicator=indicator
    )

    copy_data_to_final_tables(
        db=db,
        all_data=all_data,
        indicator=indicator,
        land_use_type=land_use_type,
    )

    if drop_tmp:
        drop_tmp_tables(db=db, indicator=indicator, land_use_type=land_use_type)

    db.commit()


def percentage_calculation_for_areas(
    db: DatabaseFacade, is_temp: bool, land_use_type: str
):
    logger.info("using spatial units and deter areas to calculate the percentages")

    for spatial_unit in read_spatial_units(db=db):
        tmpspatial_unit = f"{get_prefix(is_temp=is_temp)}{spatial_unit}"
        logger.info("processing %s", tmpspatial_unit)

        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

        sql = f"""
            UPDATE public."{tmpspatial_unit}_land_use{land_use_type_suffix}"
            SET percentage=public."{tmpspatial_unit}_land_use{land_use_type_suffix}".area/su.area*100
            FROM public."{spatial_unit}" su
            WHERE
                public."{tmpspatial_unit}_land_use{land_use_type_suffix}".suid=su.suid
                AND public."{tmpspatial_unit}_land_use{land_use_type_suffix}".classname NOT IN (
                    '{ACTIVE_FIRES_CLASSNAME}','{RISK_IBAMA_CLASSNAME}', '{RISK_INPE_CLASSNAME}'
                )
        """
        db.execute(sql)


def normalize_inpe_risk(db: DatabaseFacade, is_temp: bool, land_use_type: str):
    logger.info("normalizing inpe risk values")

    for spatial_unit in read_spatial_units(db=db):
        tmpspatial_unit = f"{get_prefix(is_temp=is_temp)}{spatial_unit}"
        logger.info("processing %s", tmpspatial_unit)

        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

        sql = f"""
            WITH suid_totals AS (
                SELECT
                    suid,
                    SUM(risk) AS total_risk
                FROM public."{tmpspatial_unit}_land_use{land_use_type_suffix}"
                GROUP BY suid
            ),
            max_total AS (
                SELECT MAX(total_risk) AS max_risk FROM suid_totals
            ),
            normalized_risk AS (
                SELECT
                    l.id,
                    CASE
                        WHEN s.total_risk > 0 AND m.max_risk > 0 THEN
                            (l.risk / s.total_risk) * (s.total_risk / m.max_risk) * {RISK_SCALE_FACTOR}
                        ELSE 0
                    END AS score
                FROM public."{tmpspatial_unit}_land_use{land_use_type_suffix}" l
                JOIN suid_totals s ON l.suid = s.suid
                CROSS JOIN max_total m
            )
            UPDATE public."{tmpspatial_unit}_land_use{land_use_type_suffix}"
            SET score = n.score
            FROM normalized_risk n
            WHERE public."{tmpspatial_unit}_land_use{land_use_type_suffix}".id = n.id;
        """

        logger.debug(sql)

        db.execute(sql)


def copy_data_to_final_tables(
    db: DatabaseFacade, all_data: bool, indicator: str, land_use_type: str
):
    logger.info("copying the new processed data to the final tables")

    if indicator == DETER_INDICATOR:
        copy_deter_land_structure(db=db, all_data=all_data, land_use_type=land_use_type)

    if indicator == ACTIVE_FIRES_INDICATOR:
        copy_fires_land_structure(db=db, all_data=all_data, land_use_type=land_use_type)

    if indicator in (RISK_IBAMA_INDICATOR, RISK_INPE_INDICATOR):
        copy_risk_land_structure(db=db, land_use_type=land_use_type)

    for spatial_unit in read_spatial_units(db=db):
        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
        land_use_table = f"{spatial_unit}_land_use{land_use_type_suffix}"
        tmp_land_use_table = f"tmp_{land_use_table}"

        logger.info("updating sequence")
        db.execute(
            sql=f"SELECT setval('{land_use_table}_id_seq', (SELECT MAX(id) FROM {land_use_table}) + 1);",
            log=True,
        )

        logger.info("copying from %s to %s.", tmp_land_use_table, land_use_table)
        db.copy_table(src=tmp_land_use_table, dst=land_use_table, cols_to_ignore=["id"])


def copy_deter_land_structure(db: DatabaseFacade, all_data: bool, land_use_type: str):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
    table = f"deter_land_structure{land_use_type_suffix}"

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    if all_data:
        create_land_structure_table(db=db, table=table, force_recreate=True)

    else:
        # here, we expect deter.tmp_data to only have DETER data coming from the current table
        # update sequence value from the ams of table id
        sql = f"""
            DELETE FROM deter_land_structure{land_use_type_suffix} WHERE gid like '%_curr';
            SELECT setval('public.deter_land_structure_id_seq', (
                SELECT MAX(id) FROM public.deter_land_structure{land_use_type_suffix})::integer, true
            );
        """
        db.execute(sql=sql)

    # copy data from temporary table
    db.copy_table(src=f"{get_prefix(is_temp=True)}{table}", dst=table)


def copy_fires_land_structure(db: DatabaseFacade, all_data: bool, land_use_type: str):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
    table = f"fires_land_structure{land_use_type_suffix}"

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    if all_data:
        create_land_structure_table(db=db, table=table, force_recreate=True)

    # copy data from temporary table
    db.copy_table(src=f"{get_prefix(is_temp=True)}{table}", dst=table)


def copy_risk_land_structure(db: DatabaseFacade, land_use_type: str):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"
    table = f"risk_land_structure{land_use_type_suffix}"

    db.truncate(table=table)

    logger.info("copying data from %s to %s.", get_prefix(is_temp=True) + table, table)

    # copy data from temporary table
    db.copy_table(src=f"{get_prefix(is_temp=True)}{table}", dst=table)


def drop_tmp_tables(db: DatabaseFacade, indicator: str, land_use_type: str):
    """Drop the temporary tables."""
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    for spatial_unit in read_spatial_units(db=db):
        land_use_table = f"tmp_{spatial_unit}_land_use{land_use_type_suffix}"
        db.drop_table(table=land_use_table)

    if indicator == DETER_INDICATOR:
        db.drop_table(table=f"tmp_deter_land_structure{land_use_type_suffix}")

    if indicator == ACTIVE_FIRES_INDICATOR:
        db.drop_table(table=f"tmp_fires_land_structure{land_use_type_suffix}")
