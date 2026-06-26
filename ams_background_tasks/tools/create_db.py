"""Create the AMS database."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    AMS,
    CELL_5KM,
    CELL_25KM,
    CELL_150KM,
    PPCDAM,
    PRODES,
    is_valid_cell,
    is_valid_land_use_type,
    reset_land_use_tables,
)
from ams_background_tasks.tools.indicators import get_description_from_classname

logger = get_logger(__name__, sys.stdout)


@click.command("create-db")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="Force to recreate the AMS database.",
)
def main(db_url: str, force_recreate: bool):
    """Create the AMS database."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    db.create_postgis_extension()
    db.create_dblink_extension()

    db.create_schema(name="fires", force_recreate=False)
    db.create_schema(name="deter", force_recreate=False)

    # spatial units
    create_spatial_units_table(db=db, force_recreate=force_recreate)

    create_states_table(db=db, force_recreate=force_recreate)
    create_states_function(db=db, force_recreate=force_recreate)

    create_municipalities_table(db=db, force_recreate=force_recreate)
    create_municipalities_function(db=db, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_5KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_5KM, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_25KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_25KM, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_150KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_150KM, force_recreate=force_recreate)

    # biome border
    create_biome_tables(db=db, force_recreate=force_recreate)

    # active_fires
    create_active_fires_table(db=db, force_recreate=force_recreate)
    create_active_fires_today_table(db=db, force_recreate=force_recreate)

    # deter
    create_deter_tables(db=db, force_recreate=force_recreate)

    # classnames
    create_class_tables(db=db, force_recreate=force_recreate)

    # land use
    create_land_use_table(db=db, force_recreate=force_recreate, land_use_type=AMS)
    create_land_use_table(db=db, force_recreate=force_recreate, land_use_type=PPCDAM)
    create_land_use_table(db=db, force_recreate=force_recreate, land_use_type=PRODES)

    reset_land_use_tables(
        db=db, is_temp=False, force_recreate=force_recreate, land_use_type=AMS
    )
    reset_land_use_tables(
        db=db, is_temp=False, force_recreate=force_recreate, land_use_type=PPCDAM
    )
    reset_land_use_tables(
        db=db, is_temp=False, force_recreate=force_recreate, land_use_type=PRODES
    )

    # municipalities group
    create_municipalities_group_tables(db=db, force_recreate=force_recreate)

    # risk
    create_risk_tables(db=db, force_recreate=force_recreate)

    # fire spreading risk
    create_fire_spreading_risk_tables(db=db, force_recreate=force_recreate)

    # processing
    create_processing_table(db=db, force_recreate=force_recreate)

    db.commit()


def create_municipalities_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.municipalities and public.municipalities_biome tables."""
    logger.info("creating the municipalities tables.")

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.municipalities_biome")
        db.drop_table(f"{schema}.municipalities")

    name = "municipalities"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "suid serial NOT NULL PRIMARY KEY",
            "name varchar(150)",
            "geocode varchar(80) UNIQUE",
            "area double precision",
            "state_acr varchar(2)",
            "state_name varchar(256)",
            "geometry geometry(MultiPolygon, 4674)",
            "FOREIGN KEY (state_acr) REFERENCES public.states (acronym)",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "geometry:gist", "state_acr:btree"],
        force_recreate=force_recreate,
    )

    name = "municipalities_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "geocode varchar(80)",
        "biome varchar(254)",
        "FOREIGN KEY (geocode) REFERENCES public.municipalities (geocode)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "biome:btree"],
        force_recreate=force_recreate,
    )


def create_municipalities_function(db: DatabaseFacade, force_recreate: bool):
    if force_recreate:
        sql = """
            DROP FUNCTION IF EXISTS public.ams_get_municipalities(
                character varying, date, date, date, integer[], character varying[], character varying, character varying[], double precision, boolean
            );
        """
        db.execute(sql=sql)

    sql = """
        CREATE OR REPLACE FUNCTION public.ams_get_municipalities(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	            geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False                
            )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint, score double precision, units bigint, ratio double precision)
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                    DECLARE
                        effective_publish_date date;
                        min_score float;

                    BEGIN
                        IF isAuthenticated THEN
                            effective_publish_date := CURRENT_DATE;
                        ELSE
                            SELECT MAX(dpd.date)
                            INTO effective_publish_date
                            FROM deter.deter_publish_date dpd
                            WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                        END IF;

                        IF clsname = 'RI' THEN
                            min_score := 0.01;
                        ELSE
                            min_score := 0;
                        END IF;

                        RETURN QUERY
                        SELECT 
                            mun.suid AS suid, 
                            mun.name AS name, 
                            mun.geometry AS geometry, 
                            mlu_j.classname AS classname, 
                            mlu_j.date AS date, 
                            COALESCE(mlu_j.perc, 0) AS percentage, 
                            COALESCE(mlu_j.total, 0) AS area, 
                            COALESCE(mlu_j.counts, 0) AS counts,
                            COALESCE(mlu_j.score, 0) AS score,
                            COALESCE(mlu_j.units, 0) AS units,
                            COALESCE(mlu_j.ratio, 0) AS ratio
                        FROM public."municipalities" mun
                        INNER JOIN (
                            SELECT 
                                mlu.suid, 
                                mlu.classname, 
                                MAX(mlu.date) AS date, 
                                SUM(mlu.percentage) AS perc, 
                                SUM(mlu.area) AS total, 
                                SUM(mlu.counts) AS counts,
                                SUM(mlu.score) AS score,
                                SUM(mlu.units) AS units,
                                CASE
                                    WHEN mlu.classname = 'AV' THEN
                                        COALESCE(
                                            SUM(mlu.counts)::double precision
                                            /
                                            NULLIF(
                                                SUM(mlu.counts2)::double precision,
                                                0
                                            ),
                                            0
                                        )
                                    WHEN mlu.classname = 'IV' THEN
                                        COALESCE(
                                            SUM(mlu.counts)::double precision
                                            /
                                            NULLIF(
                                                SUM(mlu.counts)::double precision +
                                                SUM(mlu.counts2)::double precision,
                                                0
                                            ),
                                            0
                                        )
                                    ELSE
                                        0
                                END AS ratio
                            FROM public."municipalities_land_use" mlu
                            WHERE
                                (mlu.date <= effective_publish_date OR clsname NOT IN ('DS', 'DG', 'CS', 'MN'))
                                AND mlu.land_use_id = ANY (land_use_ids)
                                AND mlu.classname = clsname
                                AND mlu.date > enddate
                                AND mlu.date <= startdate
                                AND mlu.risk >= riskThreshold
                                AND ('ALL' = ANY (biomes) OR mlu.biome = ANY (biomes))
                                AND (municipality_group_name = 'ALL' OR mlu.geocode =
                                    ANY(
                                        SELECT geocode
                                        FROM public.municipalities_group_members mgm
                                        WHERE mgm.group_id = (
                                              SELECT mg.id
                                              FROM public.municipalities_group mg
                                              WHERE mg.name=municipality_group_name
                                        )
                                   )
                                   OR mlu.geocode = ANY(geocodes)   
                               )
                            GROUP BY mlu.suid, mlu.classname
                            HAVING SUM(mlu.score) >= min_score
                        ) AS mlu_j
                        ON mun.suid = mlu_j.suid
                        ORDER BY COALESCE(mlu_j.perc, 0) DESC;
                    END;
        
        $BODY$;
    """

    db.execute(sql=sql)


def create_states_table(db: DatabaseFacade, force_recreate: bool):
    """Create the states and states_biome tables."""

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.states_biome")
        db.drop_table(f"{schema}.states", cascade=True)

    name = "states"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "suid serial NOT NULL PRIMARY KEY",
            "acronym varchar(2) UNIQUE",
            "name varchar(80) UNIQUE",
            "geocode varchar(80) UNIQUE",
            "area double precision",
            "geometry geometry(MultiPolygon, 4674)",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geometry:gist", "acronym:btree", "geocode:btree"],
        force_recreate=force_recreate,
    )

    name = "states_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "name varchar(80)",
        "biome varchar(254)",
        f"FOREIGN KEY (name) REFERENCES {schema}.states (name)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["biome:btree"],
        force_recreate=force_recreate,
    )


def create_states_function(db: DatabaseFacade, force_recreate: bool):
    if force_recreate:
        sql = """
            DROP FUNCTION IF EXISTS public.ams_get_states(
                character varying, date, date, date, integer[], character varying[], character varying, character varying[], double precision, boolean
            );
        """
        db.execute(sql=sql)

    sql = """
        CREATE OR REPLACE FUNCTION public.ams_get_states(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	            geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False                
            )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint, score double precision, units bigint, ratio double precision)
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                    DECLARE
                        effective_publish_date date;
                        min_score float;

                    BEGIN

                        IF isAuthenticated THEN
                            effective_publish_date := CURRENT_DATE;
                        ELSE
                            SELECT MAX(dpd.date)
                            INTO effective_publish_date
                            FROM deter.deter_publish_date dpd
                            WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                        END IF;

                        IF clsname = 'RI' THEN
                            min_score := 0.01;
                        ELSE
                            min_score := 0;
                        END IF;

                        RETURN QUERY
                        SELECT
                            sta.suid AS suid, 
                            sta.name AS name, 
                            sta.geometry AS geometry, 
                            slu_j.classname AS classname, 
                            slu_j.date AS date, 
                            COALESCE(slu_j.perc, 0) AS percentage, 
                            COALESCE(slu_j.total, 0) AS area, 
                            COALESCE(slu_j.counts, 0) AS counts,
                            COALESCE(slu_j.score, 0) AS score,
                            COALESCE(slu_j.units, 0) AS units,
                            COALESCE(slu_j.ratio, 0) AS ratio
                        FROM public."states" sta
                        INNER JOIN (
                            SELECT slu.suid, 
                                slu.classname, 
                                MAX(slu.date) AS date, 
                                SUM(slu.percentage) AS perc, 
                                SUM(slu.area) AS total, 
                                SUM(slu.counts) AS counts,
                                SUM(slu.score) AS score,
                                SUM(slu.units) AS units,
                                CASE
                                    WHEN slu.classname = 'AV' THEN
                                        COALESCE(
                                            SUM(slu.counts)::double precision
                                            /
                                            NULLIF(
                                                SUM(slu.counts2)::double precision,
                                                0
                                            ),
                                            0
                                        )
                                    WHEN slu.classname = 'IV' THEN
                                        COALESCE(
                                            SUM(slu.counts)::double precision
                                            /
                                            NULLIF(
                                                SUM(slu.counts)::double precision +
                                                SUM(slu.counts2)::double precision,
                                                0
                                            ),
                                            0
                                        )
                                    ELSE
                                        0
                                END AS ratio                                               
                            FROM public."states_land_use" slu
                            WHERE (slu.date <= effective_publish_date OR clsname NOT IN ('DS', 'DG', 'CS', 'MN'))
                                AND slu.land_use_id = ANY (land_use_ids)
                                AND slu.classname = clsname
                                AND slu.date > enddate
                                AND slu.date <= startdate
                                AND slu.risk >= riskThreshold
                                AND ('ALL' = ANY (biomes) OR slu.biome = ANY (biomes))
                                AND (municipality_group_name = 'ALL' OR slu.geocode =
                                    ANY(
                                        SELECT geocode
                                        FROM public.municipalities_group_members mgm
                                        WHERE mgm.group_id = (
                                              SELECT mg.id
                                              FROM public.municipalities_group mg
                                              WHERE mg.name=municipality_group_name
                                        )
                                   )
                                   OR slu.geocode = ANY(geocodes)
                               )
                            GROUP BY slu.suid, slu.classname
                            HAVING SUM(slu.score) >= min_score
                        ) AS slu_j
                        ON sta.suid = slu_j.suid
                        ORDER BY COALESCE(slu_j.perc, 0) DESC;
                    END;

        $BODY$;
    """

    db.execute(sql=sql)


def create_cell_table(db: DatabaseFacade, cell: str, force_recreate: bool):
    """Create the cs_{25|50}km and cs_{25|50}km_biome tables."""
    assert is_valid_cell(cell=cell)

    if force_recreate:
        db.drop_table(f"public.cs_{cell}_biome")
        db.drop_table(f"public.cs_{cell}")

    schema = "public"
    name = f"cs_{cell}"
    columns = [
        "suid serial NOT NULL PRIMARY KEY",
        "id varchar(10) UNIQUE",
        "col int4",
        "row int4",
        "area double precision",
        "geometry geometry(Polygon, 4674)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["id:btree", "geometry:gist"],
        force_recreate=force_recreate,
    )

    name = f"cs_{cell}_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "id varchar(10)",
        "biome varchar(254)",
        f"FOREIGN KEY (id) REFERENCES public.cs_{cell} (id)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["id:btree", "biome:btree"],
        force_recreate=force_recreate,
    )


def create_cell_function(db: DatabaseFacade, cell: str, force_recreate: bool):
    """Create the get_cs_{5|25|50}km_function."""
    assert is_valid_cell(cell=cell)

    if force_recreate:
        sql = f"""
            DROP FUNCTION IF EXISTS public.ams_get_cs_{cell}(
                character varying, date, date, date, integer[], character varying[], character varying, character varying[], double precision, boolean
            );
        """
        db.execute(sql=sql)

    sql = f"""
        CREATE OR REPLACE FUNCTION public.ams_get_cs_{cell}(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	            geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False
        )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint, score double precision, units bigint, ratio double precision)
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                        DECLARE
                            effective_publish_date date;
                            min_score float;                            

                        BEGIN
                                IF isAuthenticated THEN
                                    effective_publish_date := CURRENT_DATE;
                                ELSE
                                    SELECT MAX(dpd.date)
                                    INTO effective_publish_date
                                    FROM deter.deter_publish_date dpd
                                    WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                                END IF;

                                IF clsname = 'RI' THEN
                                    min_score := 0.01;
                                ELSE
                                    min_score := 0;
                                END IF;                        

                                RETURN QUERY
                                SELECT
                                        cel.suid AS suid, 
                                        cel.id AS name, 
                                        cel.geometry AS geometry, 
                                        cls_j.classname AS classname, 
                                        cls_j.date AS date, 
                                        COALESCE(cls_j.perc, 0) AS percentage, 
                                        COALESCE(cls_j.total, 0) AS area, 
                                        COALESCE(cls_j.counts, 0) AS counts,
                                        COALESCE(cls_j.score, 0) AS score,
                                        COALESCE(cls_j.units, 0) AS units,
                                        COALESCE(cls_j.ratio, 0) AS ratio
                                FROM public."cs_{cell}" cel
                                INNER JOIN (
                                        SELECT cls.suid, 
                                               cls.classname, 
                                               MAX(cls.date) AS date, 
                                               SUM(cls.percentage) AS perc, 
                                               SUM(cls.area) AS total, 
                                               SUM(cls.counts) AS counts,
                                               SUM(cls.score) AS score,
                                               SUM(cls.units) AS units,
                                               CASE
                                                    WHEN cls.classname = 'AV' THEN
                                                        COALESCE(
                                                            SUM(cls.counts)::double precision
                                                            /
                                                            NULLIF(
                                                                SUM(cls.counts2)::double precision,
                                                                0
                                                            ),
                                                            0
                                                        )
                                                    WHEN cls.classname = 'IV' THEN
                                                        COALESCE(
                                                            SUM(cls.counts)::double precision
                                                            /
                                                            NULLIF(
                                                                SUM(cls.counts)::double precision +
                                                                SUM(cls.counts2)::double precision,
                                                                0
                                                            ),
                                                            0
                                                        )
                                                    ELSE
                                                        0
                                               END AS ratio                                               
                                        FROM public."cs_{cell}_land_use" cls
                                        WHERE (cls.date <= effective_publish_date OR clsname NOT IN ('DS', 'DG', 'CS', 'MN'))
                                            AND cls.land_use_id = ANY (land_use_ids)
                                            AND cls.classname = clsname
                                            AND cls.date > enddate
                                            AND cls.date <= startdate
                                            AND cls.risk >= riskThreshold                                            
                                            AND ('ALL' = ANY (biomes) OR cls.biome = ANY (biomes))
                                            AND (municipality_group_name = 'ALL' OR cls.geocode =
                                                ANY(
                                                    SELECT geocode
                                                    FROM public.municipalities_group_members mgm
                                                    WHERE mgm.group_id = (
                                                        SELECT mg.id
                                                        FROM public.municipalities_group mg
                                                        WHERE mg.name=municipality_group_name
                                                    )
                                                )
                                                OR cls.geocode = ANY(geocodes)   
                                            )

                                        GROUP BY cls.suid, cls.classname
                                        HAVING SUM(cls.score) >= min_score
                                ) AS cls_j
                                ON cel.suid = cls_j.suid
                                ORDER BY COALESCE(cls_j.perc, 0) DESC;
                        END;
        
        $BODY$;
    """

    db.execute(sql=sql)


def create_active_fires_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the fires.active_fires table."""
    columns = [
        "id serial NOT NULL PRIMARY KEY",
        "uuid character varying(254)",
        "biome varchar(254)",
        "view_date date",
        "prodes_class varchar(254)",
        "satelite varchar(254)",
        "estado varchar(254)",
        "municipio varchar(254)",
        "geom geometry(Point, 4674)",
        "geocode varchar(80)",
    ]

    schema = "fires"
    name = "active_fires"

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )


def create_active_fires_today_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the active fires today tables."""
    schema = "fires"

    db.create_table(
        schema=schema,
        name="active_fires_today",
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "uuid character varying(254) UNIQUE",
            "biome varchar(254)",
            "view_date date",
            "viewed_at TIMESTAMP NOT NULL",
            "satelite varchar(254)",
            "municipio varchar(254)",
            "geom geometry(Point, 4674)",
            "geocode varchar(80)",
            "src varchar(254)",
        ],
        force_recreate=force_recreate,
    )

    db.create_table(
        schema=schema,
        name="active_fires_today_file",
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar",
            "process_status int4",
            "process_message varchar",
            "file_date TIMESTAMP NOT NULL",
            "processed_at timestamp with time zone",
        ],
        force_recreate=force_recreate,
    )


def _create_deter_table(db: DatabaseFacade, name: str, force_recreate: bool):
    """Create the deter.{name} table."""
    columns = [
        "gid serial NOT NULL",
        "origin_gid varchar NOT NULL UNIQUE",
        "biome varchar(254)",
        "view_date date",
        "classname varchar(254)",
        "satellite varchar(13)",
        "sensor varchar(10)",
        "path_row varchar(10)",
        "area_km double precision",
        "geom geometry(MultiPolygon,4674)",
        "geocode varchar(80)",
        "PRIMARY KEY (gid, biome)",
    ]

    schema = "deter"

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "classname:btree",
        "view_date:btree",
        "biome:btree",
        "geocode:btree",
        "geom:gist",
    ]

    # db.create_indexes(
    #    schema=schema,
    #    name=name,
    #    columns=columns,
    #    force_recreate=force_recreate,
    # )


def _create_deter_tmp_data_table(db: DatabaseFacade, force_recreate: bool):
    """Create the deter.tmp_data table."""
    columns = [
        "gid serial NOT NULL",
        "biome varchar(254)",
        "classname varchar(254)",
        "view_date date",
        "area_km double precision",
        "geom geometry(MultiPolygon, 4674)",
        "geocode varchar(80)",
        "PRIMARY KEY (gid, biome)",
    ]

    db.create_table(
        schema="deter",
        name="tmp_data",
        columns=columns,
        force_recreate=force_recreate,
    )


def create_deter_tables(db: DatabaseFacade, force_recreate: bool = False):
    """Create the deter.[deter, deter_auth] tables."""
    for prefix in ("", "tmp_"):
        force_recreate = len(prefix) > 0

        # deter, deter_auth, deter_history
        names = ("deter", "deter_auth")  # , "deter_history")
        for name in names:
            _create_deter_table(
                db=db, name=f"{prefix}{name}", force_recreate=force_recreate
            )

        # deter_publish_date
        db.create_table(
            schema="deter",
            name=f"{prefix}deter_publish_date",
            columns=[
                "date date",
                "biome varchar(254)",
            ],
            force_recreate=force_recreate,
        )

    # tmp_data
    _create_deter_tmp_data_table(db=db, force_recreate=force_recreate)


def create_spatial_units_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.spatial_units table."""
    schema = "public"

    if db.table_exist(schema=schema, table="spatial_units") and not force_recreate:
        return

    # spatial_units table
    if force_recreate:
        db.drop_table("public.spatial_units_subsets")
        db.drop_table("public.spatial_units")

    name = "spatial_units"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "dataname varchar NOT NULL UNIQUE",
            "as_attribute_name varchar NOT NULL",
            "center_lat double precision NOT NULL",
            "center_lng double precision NOT NULL",
            "description varchar NOT NULL",
        ],
        force_recreate=force_recreate,
    )

    table = f"{schema}.{name}"

    sql = f"""
        INSERT INTO
            {table} (id, dataname, as_attribute_name, center_lat, center_lng, description)
        VALUES
            (1, 'cs_150km', 'id', -5.491382969006503, -58.467185764253415, 'Célula 150x150 km²'),
            (2, 'cs_25km', 'id', -5.510617783522636, -58.397927203480116, 'Célula 25x25 km²'),
            (3, 'states', 'name', -6.384962796500002, -58.97111531179317, 'Estado'),
            (4, 'municipalities', 'name', -6.384962796413522, -58.97111531172743, 'Município'),
            (5, 'cs_5km', 'id', -5.510617783522636, -58.397927203480116, 'Célula 5x5 km²');
    """

    db.execute(sql)

    # spatial_units_subsets table
    name = "spatial_units_subsets"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "spatial_unit_id int4 NOT NULL",
            "subset varchar(80) NOT NULL",
            "FOREIGN KEY (spatial_unit_id) REFERENCES public.spatial_units (id)",
        ],
        force_recreate=force_recreate,
    )

    table = f"{schema}.{name}"

    sql = f"""
        INSERT INTO
            {table} (spatial_unit_id, subset)
        VALUES
            (1, 'Bioma'),
            (2, 'Bioma'),
            (3, 'Bioma'),
            (4, 'Bioma'),
            (1, 'Municípios'),
            (2, 'Municípios'),
            (4, 'Municípios'),
            (5, 'Municípios');
    """

    db.execute(sql)


def create_biome_tables(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.biome and public.biome_border tables."""
    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.biome_border")
        db.drop_table(f"{schema}.biome")

    name = "biome"

    columns = [
        "id serial PRIMARY KEY",
        "biome varchar(254) UNIQUE",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    name = "biome_border"

    columns = [
        "id serial PRIMARY KEY",
        "biome varchar(254)",
        "area_km double precision",
        "geom geometry(MultiPolygon, 4674)",
        "FOREIGN KEY (biome) REFERENCES public.biome (biome)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "geom:gist",
    ]

    db.create_indexes(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )


def create_class_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the public.class and public.class_group tables."""
    schema = "public"

    def sql_string(value: str) -> str:
        return value.replace("'", "''")

    if db.table_exist(schema=schema, table="class") and not force_recreate:
        return

    if force_recreate:
        db.drop_table(table=f"{schema}.class")
        db.drop_table(table=f"{schema}.class_group")

    name = "class_group"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar NOT NULL UNIQUE",
            "title varchar NOT NULL",
            "subtitle varchar",
            "orderby int4",
            "description varchar",
        ],
        force_recreate=False,
    )

    desc_ds = get_description_from_classname("DS")
    desc_dg = get_description_from_classname("DG")
    desc_cs = get_description_from_classname("CS")
    desc_mn = get_description_from_classname("MN")
    desc_af = get_description_from_classname("AF")
    desc_ri = get_description_from_classname("RI")
    desc_fs = get_description_from_classname("FS")
    desc_ft = get_description_from_classname("FT")
    desc_ai = get_description_from_classname("AI")
    desc_ad = get_description_from_classname("AD")
    desc_iv = get_description_from_classname("IV")
    desc_av = get_description_from_classname("AV")

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, title, subtitle, orderby, description)
        VALUES
            (1, 'DS', 'Desmatamento', 'DETER', 0, '{sql_string(desc_ds)}'),
            (2, 'DG', 'Degradação', 'DETER', 1, '{sql_string(desc_dg)}'),
            (3, 'CS', 'Corte seletivo', 'DETER', 2, '{sql_string(desc_cs)}'),
            (4, 'MN', 'Mineração', 'DETER', 3, '{sql_string(desc_mn)}'),
            (5, 'AF', 'Histórico de Focos', 'Queimadas', 8, '{sql_string(desc_af)}'),
            (6, 'RK', 'Risco de desmatamento', 'IBAMA', 12, ''),
            (7, 'RI', 'Risco de desmatamento', '', 11, '{sql_string(desc_ri)}'),
            (8, 'FS', 'Risco de espalhamento do fogo', 'FIP', 10, '{sql_string(desc_fs)}'),
            (9, 'FT', 'Focos de hoje', 'Queimadas', 9, '{sql_string(desc_ft)}'),
            (10, 'AI', 'Incremento anual', 'PRODES', 4, '{sql_string(desc_ai)}'),
            (11, 'AD', 'Desmatamento Acumulado', 'PRODES', 5, '{sql_string(desc_ad)}'),
            (12, 'IV', 'Vegetação Nativa Remanescente Desmatada', 'PRODES', 6, '{sql_string(desc_iv)}'),
            (13, 'AV', 'Vegetação Nativa Original Desmatada', 'PRODES', 7, '{sql_string(desc_av)}')
    """

    db.execute(sql=sql)

    name = "class"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar NOT NULL",
            "group_id int4",
            "biome varchar(254)",
            "UNIQUE (name, biome)",
            f"FOREIGN KEY (group_id) REFERENCES {schema}.class_group (id)",
        ],
        force_recreate=False,
    )

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, group_id, biome)
        VALUES
            (1, 'DESMATAMENTO_CR', 1, 'Amazônia'),
            (2, 'DESMATAMENTO_VEG', 1, 'Amazônia'),
            (3, 'CICATRIZ_DE_QUEIMADA', 2, 'Amazônia'),
            (4, 'DEGRADACAO', 2, 'Amazônia'),
            (5, 'CS_DESORDENADO', 3, 'Amazônia'),
            (6, 'CS_GEOMETRICO', 3, 'Amazônia'),
            (7, 'MINERACAO', 4, 'Amazônia'),
            (8, 'FOCOS', 5, 'Amazônia'),
            (9, 'RISCO_IBAMA', 6, 'Amazônia'),
            (10, 'DESMATAMENTO_CR', 1, 'Cerrado'),
            (11, 'FOCOS', 5, 'Cerrado'),
            (12, 'RISCO', 7, 'Amazônia'),
            (13, 'FOCOS', 5, 'Pantanal'),
            (14, 'FOCOS', 5, 'Caatinga'),
            (15, 'FOCOS', 5, 'Mata Atlântica'),
            (16, 'FOCOS', 5, 'Pampa'),
            (17, 'DESMATAMENTO_CR', 1, 'Pantanal'),
            (18, 'DESMATAMENTO_VEG', 1, 'Pantanal'),
            (19, 'CICATRIZ_DE_QUEIMADA', 2, 'Pantanal'),
            (20, 'MINERACAO', 4, 'Pantanal'),
            (21, 'RISCO_ESPALHAMENTO_FOGO', 8, 'Cerrado'),
            (22, 'FOCOS_HOJE', 9, 'Amazônia'),
            (23, 'FOCOS_HOJE', 9, 'Cerrado'),
            (24, 'FOCOS_HOJE', 9, 'Pantanal'),
            (25, 'FOCOS_HOJE', 9, 'Caatinga'),
            (26, 'FOCOS_HOJE', 9, 'Mata Atlântica'),
            (27, 'FOCOS_HOJE', 9, 'Pampa'),
            (28, 'INCREMENTO_ANUAL', 10, 'Amazônia'),
            (29, 'DESMATAMENTO_ACUMULADO', 11, 'Amazônia'),
            (30, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Amazônia'),
            (31, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Amazônia'),
            (32, 'INCREMENTO_ANUAL', 10, 'Cerrado'),
            (33, 'DESMATAMENTO_ACUMULADO', 11, 'Cerrado'),
            (34, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Cerrado'),
            (35, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Cerrado'),
            (36, 'INCREMENTO_ANUAL', 10, 'Caatinga'),
            (37, 'DESMATAMENTO_ACUMULADO', 11, 'Caatinga'),
            (38, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Caatinga'),
            (39, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Caatinga'),
            (40, 'INCREMENTO_ANUAL', 10, 'Pantanal'),
            (41, 'DESMATAMENTO_ACUMULADO', 11, 'Pantanal'),
            (42, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Pantanal'),
            (43, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Pantanal'),
            (44, 'INCREMENTO_ANUAL', 10, 'Mata Atlântica'),
            (45, 'DESMATAMENTO_ACUMULADO', 11, 'Mata Atlântica'),
            (46, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Mata Atlântica'),
            (47, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Mata Atlântica'),
            (48, 'INCREMENTO_ANUAL', 10, 'Pampa'),
            (49, 'DESMATAMENTO_ACUMULADO', 11, 'Pampa'),
            (50, 'DESMATAMENTO_VEGETACAO_REMANESCENTE', 12, 'Pampa'),
            (51, 'DESMATAMENTO_VEGETACAO_ORIGINAL', 13, 'Pampa')
    """

    db.execute(sql=sql)


def create_land_use_table(
    db: DatabaseFacade, land_use_type: str, force_recreate: bool = False
):
    """Create the public.land_use_ams table."""
    assert is_valid_land_use_type(land_use_type=land_use_type)
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    schema = "public"

    name = f"land_use{land_use_type_suffix}"

    columns = [
        "id serial PRIMARY KEY",
        "name varchar(64)",
        "priority INT4",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    table_exists = db.table_exist(schema=schema, table=name)

    if table_exists and not force_recreate:
        return

    land_use_categories = []

    if land_use_type == AMS:
        land_use_categories = [
            "Terra indígena",
            "Unidade de conservação de proteção integral",
            "Unidade de conservação de uso sustentável (sem APA)",
            "Território quilombola",
            "Assentamento rural",
            "Área de proteção ambiental",
            "Propriedade privada (Dados do SIGEF)",
            "Floresta pública não destinada",
            "Área sem registro fundiário",
        ]
    elif land_use_type == PPCDAM:
        land_use_categories = [
            "Terra indígena",
            "Unidade de conservação",
            "Território quilombola",
            "Assentamento rural",
            "Área de proteção ambiental",
            "Floresta pública não destinada",
            "CAR sobreposto em terra indígena",
            "CAR sobreposto em unidade de conservação",
            "CAR sobreposto em território quilombola",
            "CAR sobreposto em assentamento rural",
            "CAR sobreposto em área de proteção ambiental",
            "CAR sobreposto em floresta pública não destinada",
            "Propriedade privada (Dados do CAR)",
            "Área sem registro fundiário",
        ]
    else:  # prodes
        land_use_categories = [
            "Vegetacao Nativa",
            "Desmatamento Recente",
            "Desmatamento Consolidado",
            "Outros",
        ]

    values = [
        f"({index+1}, '{value}', {index})"
        for index, value in enumerate(land_use_categories)
    ]

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, priority)
        VALUES {",".join(values)};
        """

    db.execute(sql=sql)


def create_municipalities_group_tables(
    db: DatabaseFacade, force_recreate: bool = False
):
    """Create the public.municipalities_group and public.municipalities_group_members tables."""
    logger.info("creating the municipalities group tables.")

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.municipalities_group_members")
        db.drop_table(f"{schema}.municipalities_group")

    name = "municipalities_group"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar(150) UNIQUE",
            "type varchar(16) NOT NULL",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["name:btree"],
        force_recreate=force_recreate,
    )

    name = "municipalities_group_members"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "group_id int4 NOT NULL",
            "geocode varchar(80)",
            "FOREIGN KEY (group_id) REFERENCES public.municipalities_group (id)",
            "UNIQUE (group_id, geocode)",
        ],
        force_recreate=force_recreate,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "group_id:btree"],
        force_recreate=force_recreate,
    )


def create_risk_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the risk tables."""
    schema = "risk"

    db.create_schema(name=schema, force_recreate=False)

    if force_recreate:
        db.drop_table(f"{schema}.weekly_data", cascade=True)
        db.drop_table(f"{schema}.etl_log_risk", cascade=True)
        db.drop_table(f"{schema}.risk_image_date", cascade=True)
        db.drop_table(f"{schema}.risk_matrix_inpe", cascade=True)

    # common tables
    name = "etl_log_risk"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar",
            "process_status int4",
            "process_message varchar",
            "file_date date",
            "is_new boolean DEFAULT true",
            "created_at timestamp with time zone NOT NULL DEFAULT now()",
            "processed_at timestamp with time zone",
            "source varchar",
        ],
        force_recreate=force_recreate,
    )

    name = "risk_image_date"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar UNIQUE",
            "expiration_date date",
            "created_at date NOT NULL DEFAULT now()",
            "risk_date date",
            "source varchar",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["file_name:btree"],
        force_recreate=force_recreate,
    )

    # ibama risk
    name = "matrix_ibama_1km"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "geom geometry(Point, 4674)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geom:gist"],
        force_recreate=force_recreate,
    )

    name = "weekly_ibama_tmp"
    db.create_table(
        schema=schema,
        name=name,
        columns=["geometry geometry(Point, 4674)", "data real"],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geometry:gist"],
        force_recreate=force_recreate,
    )

    name = "weekly_data"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "date_id int4",
            "geom_id int4",
            "risk double precision",
            "biome varchar(254)",
            "geocode varchar(80)",
            "FOREIGN KEY (date_id) REFERENCES risk.risk_image_date (id)",
            "FOREIGN KEY (geom_id) REFERENCES risk.matrix_ibama_1km (id)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=[
            "date_id:btree",
            "biome:btree",
            "geocode:btree",
        ],
        force_recreate=force_recreate,
    )

    # inpe risk
    name = "risk_matrix_inpe"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "geom geometry(Point, 4674)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geom:gist"],
        force_recreate=force_recreate,
    )

    name = "risk_tmp_inpe"
    db.create_table(
        schema=schema,
        name=name,
        columns=["geometry geometry(Point, 4674)", "data real"],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geometry:gist"],
        force_recreate=force_recreate,
    )

    name = "risk_data_inpe"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "date_id int4",
            "geom_id int4",
            "risk double precision",
            "biome varchar(254)",
            "geocode varchar(80)",
            "FOREIGN KEY (date_id) REFERENCES risk.risk_image_date (id)",
            "FOREIGN KEY (geom_id) REFERENCES risk.risk_matrix_inpe (id)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=[
            "date_id:btree",
            "biome:btree",
            "geocode:btree",
        ],
        force_recreate=force_recreate,
    )


def create_processing_table(db: DatabaseFacade, force_recreate: bool):
    schema = "public"
    name = "processing"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id SERIAL NOT NULL PRIMARY KEY",
            "date DATE NOT NULL",
            "indicator VARCHAR(40) NOT NULL",
            "process VARCHAR(100) NOT NULL",
            "start_process TIMESTAMP WITH TIME ZONE NOT NULL",
            "end_process TIMESTAMP WITH TIME ZONE",
            "status VARCHAR(20) DEFAULT 'pending'",
            "CONSTRAINT valid_dates CHECK (end_process IS NULL OR end_process > start_process)",
            "CONSTRAINT valid_process CHECK (process IN ('update', 'classification-ams', 'classification-ppcdam', 'classification-prodes'))",
            "CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'completed', 'failed'))",
        ],
        force_recreate=force_recreate,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=[
            "indicator:btree",
            "date:btree",
            "process:btree",
            "start_process:btree",
            "end_process:btree",
            "status:btree",
        ],
        force_recreate=force_recreate,
    )


def create_fire_spreading_risk_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the fire spreading risk tables."""
    schema = "fire_spreading_risk"

    db.create_schema(name=schema, force_recreate=False)

    name = "risk_file"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar",
            "process_status int4",
            "process_message varchar",
            "file_date TIMESTAMP NOT NULL",
            "is_new boolean DEFAULT true",
            "processed_at timestamp with time zone",
        ],
        force_recreate=force_recreate,
    )

    name = "risk_data"
    columns = [
        "id serial NOT NULL PRIMARY KEY",
        "biome varchar(254)",
        "view_date date",
        "municipality varchar(254)",
        "geom geometry(Point, 4674)",
        "geocode varchar(80)",
        "src varchar(254)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )
