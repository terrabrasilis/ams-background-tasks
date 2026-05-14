"""Update the PRODES tables."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click
import numpy as np

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import BIOMES, LAND_USE_TYPES
from ams_background_tasks.tools.prodes_utils import (
    build_accumulated_deforestation_indicator_dataframe,
    build_deforestation_land_use_counts_dataframe,
    build_total_deforestation_indicator_dataframe,
    build_total_vegetation_dataframe,
    create_prodes_deforestation_indicator_tables,
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

    vegetation_count_dir = prodes_cache_dir / "vegetation"
    assert vegetation_count_dir.exists(), f"{vegetation_count_dir} not found"

    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    create_prodes_deforestation_indicator_tables(db=db, force_recreate=force_recreate)

    years_list = np.arange(start=years[0], stop=years[1] + 1, step=1).tolist()

    land_use_tiff_file = Path(land_use_dir) / land_use_type / "land_use.tif"

    only_vegetation = False

    if not only_vegetation:
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

    if only_cache:
        db.commit()
        return

    # total deforestation
    total_deforestation_dfr = build_total_deforestation_indicator_dataframe(
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

    total_deforestation_filename = (
        prodes_cache_dir
        / f"total_deforestation_indicator_from_y{years_list[0]}_to_y{years_list[-1]}_b{biome}.pkl"
    )

    total_deforestation_dfr.to_pickle(total_deforestation_filename)

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
        / f"accumulated_deforestation_indicator_from_y{years_list[0]}_to_y{years_list[-1]}_b{biome}.pkl"
    )

    accumulated_deforestation_dfr.to_pickle(accumulated_deforestation_filename)

    # vegetation
    vegetation_dfr = build_total_vegetation_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        cache_dir=prodes_cache_dir,
        vegetation_count_dir=vegetation_count_dir,
        biome=biome,
        years=years_list,
        chunk_list=list(chunk),
    )

    vegetation_filename = (
        prodes_cache_dir / f"vegetation_from_y{years[0]}_to_y{years[-1]}_b{biome}.pkl"
    )

    vegetation_dfr.to_pickle(vegetation_filename)

    # ratio of accumulated deforestation to available native vegetation
    ratio_deforestation_vegetation_dfr = accumulated_deforestation_dfr.merge(
        vegetation_dfr, on=["suid", "year", "spatial_unit", "biome"], how="outer"
    )

    ratio_deforestation_vegetation_dfr = ratio_deforestation_vegetation_dfr.rename(
        columns={"num_pixels": "vegetation_pixels"}
    )

    ratio_deforestation_vegetation_filename = (
        prodes_cache_dir
        / f"ratio_accumulated_deforestation_vegetation_indicator_from_y{years_list[0]}_to_y{years_list[-1]}_b{biome}.pkl"
    )

    ratio_deforestation_vegetation_dfr.to_pickle(
        ratio_deforestation_vegetation_filename
    )
