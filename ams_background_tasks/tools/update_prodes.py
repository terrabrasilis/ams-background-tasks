"""Update the PRODES tables."""
# pylint: disable=too-many-statements

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    BIOMES,
    LAND_USE_TYPES,
    PRODES_ACCUMULATED_DEFORESTATION,
    PRODES_ANNUAL_INCREASE_DEFORESTATION,
    PRODES_DEFORESTATION_RATIO,
    PRODES_NATIVE_VEGETATION,
    read_spatial_units,
)
from ams_background_tasks.tools.prodes_utils import (
    PRODES_DEFORESTATION_PIXEL_AREA,
    PRODES_LAST_YEAR,
    build_accumulated_deforestation_indicator_dataframe,
    build_annual_increase_in_deforestation_indicator_dataframe,
    build_deforestation_land_use_counts_dataframe,
    build_total_vegetation_indicator_dataframe,
    build_vegetation_from_deforestation_dataframe,
    build_vegetation_land_use_counts_dataframe,
    create_prodes_deforestation_indicator_tables,
    save_indicator,
)

logger = get_logger(__name__, sys.stdout)


@click.command("update-prodes")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--biome",
    required=True,
    type=click.Choice(BIOMES),
    help="Biome.",
)
@click.option(
    "--years",
    nargs=2,
    type=int,
    required=True,
    help="First and last PRODES year to process, in that order.",
)
@click.option(
    "--land-use-dir",
    required=True,
    type=click.Path(exists=True, resolve_path=True, dir_okay=True),
    help="Directory to load the land use raster.",
)
@click.option(
    "--land-use-type",
    required=True,
    type=click.Choice(LAND_USE_TYPES),
    help="Land use categories type.",
)
@click.option(
    "--prodes-root-dir",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True, resolve_path=True),
    help=(
        "Root directory containing the PRODES TIFF file and the "
        "`chunk`, `reproject`, and `counts` subdirectories."
    ),
)
@click.option(
    "--chunk-size",
    required=True,
    type=int,
    default=1000,
    show_default=True,
    help="Chunk size, in pixels, used to partition the PRODES raster.",
)
@click.option("--chunk", multiple=True, help="Chunks to process.")
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="If set, recreate the PRODES tables before loading the data.",
)
@click.option(
    "--only-cache",
    required=False,
    is_flag=True,
    default=False,
    help="Update cache and exit.",
)
@click.option(
    "--save-indicators",
    required=False,
    is_flag=True,
    default=False,
    help="Persist the generated PRODES indicators to the database.",
)
def main(
    *,
    db_url: str,
    biome: str,
    years: tuple[int, int],
    land_use_dir: str,
    land_use_type: str,
    prodes_root_dir: str,
    chunk_size: int,
    force_recreate: bool,
    chunk: tuple,
    only_cache: bool,
    save_indicators: bool,
):
    """Update the PRODES tables."""
    assert years[1] >= years[0], "Last year must be greather than or equal first year."

    prodes_tiff_file = Path(prodes_root_dir) / f"prodes_{biome}.tif"
    assert prodes_tiff_file.exists(), f"{prodes_tiff_file} not found"

    prodes_cache_dir = Path(prodes_root_dir) / "cache"
    assert prodes_cache_dir.exists(), f"{prodes_cache_dir} not found"

    chunk_dir = prodes_cache_dir / "chunk"
    assert chunk_dir.exists(), f"{chunk_dir} not found"

    reproject_dir = prodes_cache_dir / "reproject"
    assert reproject_dir.exists(), f"{reproject_dir} not found"

    count_dir = prodes_cache_dir / "count"
    assert count_dir.exists(), f"{count_dir} not found"

    # vegetation_count_dir = prodes_cache_dir / "vegetation"
    # assert vegetation_count_dir.exists(), f"{vegetation_count_dir} not found"

    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    create_prodes_deforestation_indicator_tables(db=db, force_recreate=force_recreate)

    years_list = np.arange(start=years[0], stop=years[1] + 1, step=1).tolist()

    land_use_tiff_file = Path(land_use_dir) / land_use_type / "land_use.tif"

    for year in years_list:
        _ = build_deforestation_land_use_counts_dataframe(
            db=db,
            land_use_tiff_file=land_use_tiff_file,
            prodes_tiff_file=Path(prodes_tiff_file),
            chunk_size=chunk_size,
            chunk_dir=Path(chunk_dir),
            reproject_dir=Path(reproject_dir),
            count_dir=Path(count_dir),
            biome=biome,
            year=year,
            chunk_list=list(chunk),
        )

    _ = build_vegetation_land_use_counts_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=Path(prodes_tiff_file),
        chunk_size=chunk_size,
        chunk_dir=Path(chunk_dir),
        reproject_dir=Path(reproject_dir),
        count_dir=Path(count_dir),
        biome=biome,
        year=PRODES_LAST_YEAR,
        chunk_list=list(chunk),
    )

    if only_cache:
        db.commit()
        return

    # total deforestation
    annual_increase_dfr = build_annual_increase_in_deforestation_indicator_dataframe(
        db=db,
        prodes_tiff_file=prodes_tiff_file,
        land_use_tiff_file=land_use_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        years=years_list,
        chunk_list=list(chunk),
    )

    annual_increase_filename = (
        prodes_cache_dir
        / f"annual_increase_in_deforestation_indicator_b{biome}_from_y{years_list[0]}_to_y{years_list[-1]}.pkl"
    )

    annual_increase_dfr.to_pickle(annual_increase_filename)

    if save_indicators:
        persist_count_based_indicator(
            db=db,
            indicator_dataframe=annual_increase_dfr,
            classname=PRODES_ANNUAL_INCREASE_DEFORESTATION,
        )

    # accumulated deforestation
    accumulated_deforestation_dfr = build_accumulated_deforestation_indicator_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        years=years_list,
        chunk_list=list(chunk),
    )

    accumulated_deforestation_filename = (
        prodes_cache_dir
        / f"accumulated_deforestation_indicator_b{biome}_from_y{years_list[0]}_to_y{years_list[-1]}.pkl"
    )

    accumulated_deforestation_dfr.to_pickle(accumulated_deforestation_filename)

    if save_indicators:
        persist_count_based_indicator(
            db=db,
            indicator_dataframe=accumulated_deforestation_dfr,
            classname=PRODES_ACCUMULATED_DEFORESTATION,
        )

    # vegetation from future deforestation
    _ = build_vegetation_from_deforestation_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        cache_dir=prodes_cache_dir,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        years=years_list,
        chunk_list=list(chunk),
    )

    # vegetation
    vegetation_dfr = build_total_vegetation_indicator_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        cache_dir=prodes_cache_dir,
        biome=biome,
        years=years_list,
        chunk_list=list(chunk),
    )

    vegetation_filename = (
        prodes_cache_dir / f"vegetation_b{biome}_from_y{years[0]}_to_y{years[-1]}.pkl"
    )

    vegetation_dfr.to_pickle(vegetation_filename)

    if save_indicators:
        persist_count_based_indicator(
            db=db,
            indicator_dataframe=vegetation_dfr,
            classname=PRODES_NATIVE_VEGETATION,
            count_column="num_pixels",
        )

    # ratio of accumulated deforestation to available native vegetation
    ratio_deforestation_vegetation_dfr = accumulated_deforestation_dfr.merge(
        vegetation_dfr,
        on=["suid", "land_use_id", "geocode", "spatial_unit", "year", "biome"],
        how="outer",
    )
    ratio_deforestation_vegetation_dfr = ratio_deforestation_vegetation_dfr.rename(
        columns={
            "counts": "deforestation_counts",
            "num_pixels": "vegetation_counts",
        }
    )
    ratio_deforestation_vegetation_dfr = ratio_deforestation_vegetation_dfr.fillna(
        {
            "deforestation_counts": 0,
            "vegetation_counts": 0,
        }
    )

    ratio_deforestation_vegetation_filename = (
        prodes_cache_dir
        / f"ratio_accumulated_deforestation_to_vegetation_indicator_b{biome}_from_y{years_list[0]}_to_y{years_list[-1]}.pkl"
    )

    ratio_deforestation_vegetation_dfr.to_pickle(
        ratio_deforestation_vegetation_filename
    )

    if save_indicators:
        dfr = ratio_deforestation_vegetation_dfr.copy()
        dfr["counts"] = dfr["deforestation_counts"]
        dfr["area"] = PRODES_DEFORESTATION_PIXEL_AREA * dfr["counts"]
        dfr["percentage"] = 0.0

        vegetation_counts = dfr["vegetation_counts"].to_numpy(dtype=float)
        deforestation_counts = dfr["deforestation_counts"].to_numpy(dtype=float)

        score = np.zeros_like(deforestation_counts, dtype=float)

        valid_vegetation_mask = vegetation_counts > 0
        score[valid_vegetation_mask] = (
            deforestation_counts[valid_vegetation_mask]
            / vegetation_counts[valid_vegetation_mask]
        )

        if not np.all(valid_vegetation_mask):
            logger.warning(
                "Found %s ratio rows with vegetation_counts <= 0; using score=0.0 for them.",
                int((~valid_vegetation_mask).sum()),
            )
        dfr["score"] = score
        persist(db=db, indicator_dataframe=dfr, classname=PRODES_DEFORESTATION_RATIO)

    db.commit()


def persist(
    db: DatabaseFacade, indicator_dataframe: pd.DataFrame, classname: str
) -> None:
    """Persist a PRODES indicator dataframe into the spatial-unit tables.

    The dataframe must include the columns required by ``save_indicator`` and
    a ``spatial_unit`` column that matches one of the configured spatial units
    in the database. Rows are split by spatial unit and written to the
    corresponding ``prodes.<spatial_unit>_land_use`` table.
    """
    required_columns = {
        "suid",
        "land_use_id",
        "geocode",
        "biome",
        "counts",
        "area",
        "percentage",
        "score",
        "year",
        "spatial_unit",
    }

    missing_columns = required_columns.difference(indicator_dataframe.columns)
    assert not missing_columns, (
        "indicator_dataframe missing columns: " f"{', '.join(sorted(missing_columns))}"
    )

    if indicator_dataframe.empty:
        return

    spatial_units = read_spatial_units(db=db)
    unexpected_spatial_units = sorted(
        set(indicator_dataframe["spatial_unit"].dropna().unique()) - set(spatial_units)
    )
    assert not unexpected_spatial_units, (
        "indicator_dataframe has invalid spatial_unit values: "
        f"{', '.join(unexpected_spatial_units)}"
    )

    for spatial_unit in spatial_units:
        dfr = indicator_dataframe[indicator_dataframe["spatial_unit"] == spatial_unit]
        save_indicator(
            db=db,
            indicator_dataframe=dfr,
            classname=classname,
            spatial_unit=spatial_unit,
        )


def persist_count_based_indicator(
    *,
    db: DatabaseFacade,
    indicator_dataframe: pd.DataFrame,
    classname: str,
    count_column: str = "counts",
) -> None:
    """Persist a count-based PRODES indicator with derived area and zero score.

    The source count column defaults to ``counts`` but can be overridden for
    dataframes that expose their pixel total under a different name, such as
    ``num_pixels``.
    """
    dfr = indicator_dataframe.copy()
    dfr["counts"] = dfr[count_column]
    dfr["area"] = PRODES_DEFORESTATION_PIXEL_AREA * dfr["counts"]
    dfr["score"] = 0.0
    dfr["percentage"] = 0.0
    persist(db=db, indicator_dataframe=dfr, classname=classname)
