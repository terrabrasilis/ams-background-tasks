# pylint: disable=too-many-statements
from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Final

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from rasterio.warp import Resampling, reproject
from rasterio.windows import Window, from_bounds
from shapely.geometry import Point, box

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import AMS, LAND_USE_TYPES, read_spatial_units

PRODES_DEFORESTATION_PIXEL_BASE_YEAR: Final = 2000
PRODES_REMAINING_DEFORESTATION_PIXEL_REFERENCE_YEAR: Final = 1960
PRODES_NATIVE_FOREST_VEGETATION_PIXEL_VALUE: Final = 100
PRODES_NATIVE_NON_FOREST_VEGETATION_PIXEL_VALUE: Final = 101
PRODES_FIRST_YEAR: Final = 2000
PRODES_LAST_YEAR: Final = 2025
PRODES_DB_SCHEMA: Final = "prodes"
PRODES_DEFORESTATION_PIXEL_AREA = 29.875 * 29.875 * (10**-6)  # km^2

logger = get_logger(__name__, sys.stdout)


def calculate_intersection(
    ds_a: rio.io.DatasetReader, ds_b: rio.io.DatasetReader
) -> tuple[float, float, float, float]:
    """Return the intersection bounding box between two raster datasets."""
    bounds_a = ds_a.bounds
    bounds_b = ds_b.bounds

    left = max(bounds_a.left, bounds_b.left)
    right = min(bounds_a.right, bounds_b.right)
    bottom = max(bounds_a.bottom, bounds_b.bottom)
    top = min(bounds_a.top, bounds_b.top)

    return left, bottom, right, top


def has_intersection(ds_a: rio.io.DatasetReader, ds_b: rio.io.DatasetReader) -> bool:  # type: ignore
    """Return whether two raster datasets overlap spatially."""
    left, bottom, right, top = calculate_intersection(ds_a=ds_a, ds_b=ds_b)

    return left < right and bottom < top


def calculate_chunks_coordinates(
    shape: tuple[int, int, int],
    chunk_size: int,
) -> list[tuple[int, int]]:
    """Return the upper-left coordinates of single-band raster chunks.

    The coordinates are computed against the raster dimensions padded up to the
    next multiple of ``chunk_size`` so the generated windows do not overlap.
    """
    assert len(shape) == 3

    count, height, width = shape

    assert count == 1 and 0 < chunk_size <= height and chunk_size <= width

    padded_height = ((height + chunk_size - 1) // chunk_size) * chunk_size
    padded_width = ((width + chunk_size - 1) // chunk_size) * chunk_size

    range_line = np.arange(0, padded_height, chunk_size)
    range_column = np.arange(0, padded_width, chunk_size)

    lines, columns = np.meshgrid(range_line, range_column, indexing="ij")

    return list(zip(lines.flatten(), columns.flatten()))


def raster_partition(
    src_ds: rio.io.DatasetReader,  # type: ignore
    chunk_size: int,
    save_dir: Path,
    output_prefix: str,
) -> list[Path]:
    """Split a single-band raster into fixed-size chunks and save them to disk."""
    assert save_dir.exists()
    assert src_ds.nodata is not None

    coords = calculate_chunks_coordinates(
        shape=(src_ds.count, src_ds.height, src_ds.width),
        chunk_size=chunk_size,
    )

    output_paths: list[Path] = []

    for row_off, col_off in coords:
        output_path = save_dir / f"{output_prefix}_r{row_off}_c{col_off}.tif"
        nodata_path = save_dir / f"{output_prefix}_r{row_off}_c{col_off}.tif.nodata"

        if output_path.exists():
            if not nodata_path.exists():
                output_paths.append(output_path)
            continue

        window = Window(col_off=col_off, row_off=row_off, width=chunk_size, height=chunk_size)  # type: ignore

        data = src_ds.read(
            1,
            window=window,
            boundless=True,
            fill_value=src_ds.nodata,
        )

        # is_all_nodata = src_ds.nodata is not None and np.all(data == src_ds.nodata)
        # if is_all_nodata:
        # continue

        transform = rio.windows.transform(window, src_ds.transform)  # type: ignore

        profile = src_ds.profile.copy()
        profile.update(
            {
                "width": chunk_size,
                "height": chunk_size,
                "transform": transform,
            }
        )

        with rio.open(output_path, "w", **profile) as dst_ds:
            dst_ds.write(data, 1)

        output_paths.append(output_path)

    return output_paths


def raster_reproject(
    ref_ds: rio.io.DatasetReader,
    src_ds: rio.io.DatasetReader,
    save_dir: Path,
    resampling: Resampling,
    dst_nodata: float | int | None = None,
) -> Path:
    """Reproject a source raster to the reference grid within their intersection area."""
    assert save_dir.exists()

    output_path = save_dir / f"{Path(ref_ds.name).stem}_land_use.tif"
    if output_path.exists():
        return output_path

    if not has_intersection(ds_a=ref_ds, ds_b=src_ds):
        raise ValueError("there is no intersection between the given rasters")

    left, bottom, right, top = calculate_intersection(ds_a=ref_ds, ds_b=src_ds)

    # building a window and a transform for the common bounding box
    dst_window = from_bounds(left, bottom, right, top, transform=ref_ds.transform)
    dst_window = dst_window.round_offsets().round_lengths()

    dst_transform = rio.windows.transform(dst_window, ref_ds.transform)

    dst_width = int(dst_window.width)
    dst_height = int(dst_window.height)

    dst_crs = ref_ds.crs

    dst_nodata = dst_nodata if dst_nodata is not None else src_ds.nodata
    assert dst_nodata is not None

    dst_data = np.full((1, dst_height, dst_width), dst_nodata, dtype=src_ds.dtypes[0])

    reproject(
        source=rio.band(src_ds, 1),
        destination=dst_data[0],
        src_transform=src_ds.transform,
        src_crs=src_ds.crs,
        src_nodata=src_ds.nodata,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        dst_nodata=dst_nodata,
        resampling=resampling,
    )

    profile = src_ds.profile.copy()
    profile.update(
        {
            "width": dst_width,
            "height": dst_height,
            "transform": dst_transform,
            "crs": dst_crs,
        }
    )

    with rio.open(output_path, "w", **profile) as dst_ds:
        dst_ds.write(dst_data)

    return output_path


def build_deforestation_mask(data: np.ndarray, year: int) -> np.ndarray:
    """Return a binary mask for the deforestation classes associated with a given year."""
    assert PRODES_FIRST_YEAR <= year <= PRODES_LAST_YEAR

    deforestation_pixel_value = year - PRODES_DEFORESTATION_PIXEL_BASE_YEAR
    remaining_deforestation_pixel_value = (
        year - PRODES_REMAINING_DEFORESTATION_PIXEL_REFERENCE_YEAR
    )

    return np.isin(
        data, [deforestation_pixel_value, remaining_deforestation_pixel_value]
    ).astype(np.uint8)


def build_native_vegetation_mask(data: np.ndarray) -> np.ndarray:
    """Return a binary mask for native vegetation pixels for the given year.

    The PRODES raster encodes native vegetation with the values
    ``100`` (native forest) and ``101`` (native non-forest).
    """
    return np.isin(
        data,
        [
            PRODES_NATIVE_FOREST_VEGETATION_PIXEL_VALUE,
            PRODES_NATIVE_NON_FOREST_VEGETATION_PIXEL_VALUE,
        ],
    ).astype(np.uint8)


def build_mask_points_with_geocode_geodataframe(
    ds: rio.io.DatasetReader,
    mask: np.ndarray,
    landuse_data: np.ndarray,
    municipalities_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Build a point GeoDataFrame for masked pixels with land-use values and municipality geocodes."""
    assert mask.shape == landuse_data.shape
    assert not np.any(landuse_data == 0)

    rows, cols = np.where(mask == 1)

    xs, ys = ds.transform * (cols + 0.5, rows + 0.5)

    geometry = [Point(x, y) for x, y in zip(xs, ys)]
    values = landuse_data[rows, cols]
    source = Path(ds.name).name

    mask_points_gdf = gpd.GeoDataFrame(
        {"geometry": geometry, "land_use_id": values, "source": source}, crs=ds.crs
    )

    municipalities_in_bounds_gdf = filter_municipalities_by_raster_bounds(
        ds=ds, municipalities_gdf=municipalities_gdf
    )

    return gpd.sjoin(
        mask_points_gdf,
        municipalities_in_bounds_gdf[["geocode", "geometry"]],  # type: ignore
        how="inner",
        predicate="within",
    )[
        ["geometry", "geocode", "land_use_id", "source"]
    ]  # type: ignore


def filter_municipalities_by_raster_bounds(
    ds: rio.io.DatasetReader,
    municipalities_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Return the municipalities whose geometries intersect the raster bounds."""
    bounds = ds.bounds
    geom = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
    bounds_gdf = gpd.GeoDataFrame(
        geometry=[geom],
        crs=ds.crs,
    )
    return gpd.sjoin(
        municipalities_gdf, bounds_gdf, how="inner", predicate="intersects"
    )


def load_spatial_units_gdf(
    db: DatabaseFacade, spatial_unit: str, biome: str
) -> gpd.GeoDataFrame:
    """Load the spatial units associated with a biome from PostGIS."""
    ckey = {
        "cs_5km": "id",
        "cs_25km": "id",
        "cs_150km": "id",
        "municipalities": "geocode",
        "states": "name",
    }[spatial_unit]

    sql = f"""
        SELECT suid, geometry FROM public.{spatial_unit} su
        JOIN public.{spatial_unit}_biome sub ON sub.{ckey}=su.{ckey}
        WHERE sub.biome='{biome}'
    """

    spatial_units_gdf = gpd.GeoDataFrame.from_postgis(
        sql=sql, con=db.conn, geom_col="geometry"
    )

    return spatial_units_gdf


def aggregate_deforestation_by_spatial_unit(
    spatial_unit: str,
    spatial_units_gdf: gpd.GeoDataFrame,
    deforestation_points_geocode_gdf: gpd.GeoDataFrame,
    year: int,
    biome: str,
) -> pd.DataFrame:
    """Aggregate geocoded deforestation points by spatial unit and land-use class."""
    spatial_unit_landuse_counts = gpd.sjoin(
        deforestation_points_geocode_gdf,
        spatial_units_gdf,
        how="inner",
        predicate="within",
    )
    spatial_unit_landuse_counts = spatial_unit_landuse_counts.groupby(
        ["suid", "land_use_id", "geocode", "source"],
        as_index=False,
    ).agg(num_pixels=("geometry", "count"))

    spatial_unit_landuse_counts["spatial_unit"] = spatial_unit
    spatial_unit_landuse_counts["year"] = year
    spatial_unit_landuse_counts["biome"] = biome

    return spatial_unit_landuse_counts


def build_spatial_unit_land_use_counts_dataframe_from_mask(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    year: int,
    mask_label: str,
    build_mask: Callable[[np.ndarray, int], np.ndarray],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Build land-use counts per spatial unit from a binary raster mask.

    The helper performs the shared raster partition, reprojection, point
    generation and spatial aggregation steps. The only mask-specific behavior
    is injected via ``build_mask`` and reflected in the cache file naming
    through ``mask_label``.
    """
    assert chunk_dir.exists()
    assert reproject_dir.exists()
    assert count_dir.exists()
    assert PRODES_FIRST_YEAR <= year <= PRODES_LAST_YEAR

    chunk_counts_year_path = (
        count_dir / f"spatial_unit_{mask_label}_landuse_counts_b{biome}_y{year}.pkl"
    )
    nodata_year_path = (
        count_dir
        / f"spatial_unit_{mask_label}_landuse_counts_b{biome}_y{year}.pkl.nodata"
    )

    if nodata_year_path.exists():
        return pd.DataFrame()

    if chunk_counts_year_path.exists():
        logger.info("loading %s", chunk_counts_year_path)
        return pd.read_pickle(chunk_counts_year_path)

    year_count_dir = count_dir / str(year)
    year_count_dir.mkdir(exist_ok=True)

    output_prefix = f"prodes_{biome}"

    logger.info(
        "partitioning the %s in chunks of (%s, %s) pixels",
        prodes_tiff_file,
        chunk_size,
        chunk_size,
    )
    with rio.open(prodes_tiff_file) as prodes_ds:
        chunks = raster_partition(
            src_ds=prodes_ds,
            chunk_size=chunk_size,
            save_dir=chunk_dir,
            output_prefix=output_prefix,
        )

    logger.info("building the municipalities geodataframe")

    sql = f"""
        SELECT mu.geocode, mu.geometry
        FROM public.municipalities mu
        JOIN public.municipalities_biome mub ON mu.geocode=mub.geocode
        WHERE biome='{biome}'
    """

    municipalities_gdf = gpd.GeoDataFrame.from_postgis(
        sql=sql, con=db.conn, geom_col="geometry"
    )

    spatial_units = list(read_spatial_units(db=db).keys())

    spatial_units_gdf_map = {}
    for spatial_unit in spatial_units:
        spatial_units_gdf_map[spatial_unit] = load_spatial_units_gdf(
            db=db, spatial_unit=spatial_unit, biome=biome
        )

    spatial_unit_landuse_counts_list = []

    chunk_size = len(chunks)

    with rio.open(land_use_tiff_file) as land_use_ds:
        for index_chunk, chunk in enumerate(chunks):
            if chunk_list and not chunk in chunk_list:
                continue

            logger.info(
                "processing the chunk %s (%s/%s)", chunk, index_chunk + 1, chunk_size
            )

            nodata_file = Path(f"{chunk}.nodata")

            if nodata_file.exists():
                continue

            with rio.open(chunk) as chunk_ds:
                prodes_chunk_stem = Path(chunk_ds.name).stem

                chunk_counts_path = (
                    year_count_dir
                    / f"spatial_unit_{mask_label}_landuse_counts_{prodes_chunk_stem}.pkl"
                )

                if chunk_counts_path.exists():
                    spatial_unit_landuse_counts = pd.read_pickle(chunk_counts_path)
                    spatial_unit_landuse_counts_list.append(spatial_unit_landuse_counts)
                    continue

                prodes_chunk_data = chunk_ds.read(1)

                is_all_nodata = chunk_ds.nodata is not None and np.all(
                    prodes_chunk_data == chunk_ds.nodata
                )

                if is_all_nodata:
                    nodata_file.touch()
                    continue

                land_use_chunk = raster_reproject(
                    ref_ds=chunk_ds,
                    src_ds=land_use_ds,
                    save_dir=reproject_dir,
                    resampling=Resampling.nearest,
                )

                logger.info("building %s mask for %s", mask_label, year)

                mask = build_mask(prodes_chunk_data, year)

                if not np.any(mask == 1):
                    logger.info("there is no %s points", mask_label)
                    continue

                with rio.open(land_use_chunk) as land_use_chunk_ds:
                    land_use_chunk_data = land_use_chunk_ds.read(1)

                    spatial_unit_landuse_counts_list_by_chunk = []

                    logger.info("building %s points geodataframe", mask_label)

                    mask_points_geocode_gdf = (
                        build_mask_points_with_geocode_geodataframe(
                            ds=chunk_ds,
                            mask=mask,
                            landuse_data=land_use_chunk_data,
                            municipalities_gdf=municipalities_gdf,
                        )
                    )

                    for spatial_unit in spatial_units:
                        logger.info("aggregating by spatial unit %s", spatial_unit)

                        spatial_unit_landuse_counts = aggregate_deforestation_by_spatial_unit(
                            spatial_unit=spatial_unit,
                            spatial_units_gdf=spatial_units_gdf_map[spatial_unit],
                            deforestation_points_geocode_gdf=mask_points_geocode_gdf,
                            year=year,
                            biome=biome,
                        )

                        if (
                            spatial_unit_landuse_counts is None
                            or spatial_unit_landuse_counts.empty
                        ):
                            continue

                        spatial_unit_landuse_counts_list_by_chunk.append(
                            spatial_unit_landuse_counts
                        )

                if not spatial_unit_landuse_counts_list_by_chunk:
                    continue

                _tmp: pd.DataFrame = pd.concat(
                    spatial_unit_landuse_counts_list_by_chunk
                )
                _tmp.to_pickle(chunk_counts_path)
                spatial_unit_landuse_counts_list.append(_tmp)

    if not spatial_unit_landuse_counts_list:
        Path(f"{chunk_counts_year_path}.nodata").touch()
        return pd.DataFrame()

    spatial_unit_landuse_counts = pd.concat(spatial_unit_landuse_counts_list)
    spatial_unit_landuse_counts = spatial_unit_landuse_counts.groupby(
        ["suid", "land_use_id", "geocode", "spatial_unit", "year", "biome"],
        as_index=False,
    ).agg(num_pixels=("num_pixels", "sum"))

    spatial_unit_landuse_counts.to_pickle(chunk_counts_year_path)

    return spatial_unit_landuse_counts


def build_deforestation_land_use_counts_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    year: int,
    chunk_list: list[Path] | None = None,
):
    """Build an aggregated PRODES deforestation indicator dataframe.

    This is a thin wrapper around the generic mask-based builder, configured
    with the PRODES deforestation mask and the deforestation cache names.
    """
    return build_spatial_unit_land_use_counts_dataframe_from_mask(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        year=year,
        mask_label="deforestation",
        build_mask=build_deforestation_mask,
        chunk_list=chunk_list,
    )


def build_vegetation_land_use_counts_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    year: int,
    chunk_list: list[Path] | None = None,
):
    """Build an aggregated PRODES vegetation indicator dataframe.

    This wrapper reuses the generic mask-based builder, configured with the
    native vegetation mask and vegetation-specific cache names.
    """

    def _build_vegetation_mask(data: np.ndarray, _: int) -> np.ndarray:
        return build_native_vegetation_mask(data=data)

    return build_spatial_unit_land_use_counts_dataframe_from_mask(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        year=year,
        mask_label="vegetation",
        build_mask=_build_vegetation_mask,
        chunk_list=chunk_list,
    )


def build_accumulated_deforestation_indicator_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    years: list[int],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Build the cumulative deforestation indicator across the requested years.

    For each year in `years`, the returned dataframe contains the sum of all
    deforestation counts observed from the first available year up to that year,
    grouped by spatial unit, land-use class, geocode and biome.
    """
    deforestation_land_use_counts_list = [
        build_deforestation_land_use_counts_dataframe(
            db=db,
            land_use_tiff_file=land_use_tiff_file,
            prodes_tiff_file=prodes_tiff_file,
            chunk_size=chunk_size,
            chunk_dir=chunk_dir,
            reproject_dir=reproject_dir,
            count_dir=count_dir,
            biome=biome,
            year=year,
            chunk_list=chunk_list,
        )
        for _, year in enumerate(years)
    ]

    deforestation_land_use_counts_list = [
        _ for _ in deforestation_land_use_counts_list if _ is not None and not _.empty
    ]

    if not deforestation_land_use_counts_list:
        return pd.DataFrame()

    dfr = pd.concat(deforestation_land_use_counts_list)

    dfr_list: list[pd.DataFrame] = []

    for year in years:
        dfr2 = dfr[dfr["year"] <= year]  # type: ignore
        dfr2: pd.DataFrame = dfr2.groupby(
            ["suid", "land_use_id", "geocode", "spatial_unit", "biome"],
            as_index=False,
        ).agg(counts=("num_pixels", "sum"))
        dfr2["year"] = year
        dfr_list.append(dfr2)

    if not dfr_list:
        return pd.DataFrame()

    dfr = pd.concat(dfr_list)
    # dfr["area"] = PRODES_DEFORESTATION_PIXEL_AREA * dfr["counts"]
    # dfr["score"] = 0.0
    # dfr["percentage"] = 0.0

    return dfr


def build_annual_increase_in_deforestation_indicator_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    years: list[int],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Build the annual deforestation indicator from yearly land-use counts.

    The returned dataframe keeps one row per input year and spatial aggregation,
    with the deforestation count, estimated area, score and percentage fields
    populated for downstream persistence or analysis.
    """
    deforestation_land_use_counts_list = [
        build_deforestation_land_use_counts_dataframe(
            db=db,
            land_use_tiff_file=land_use_tiff_file,
            prodes_tiff_file=prodes_tiff_file,
            chunk_size=chunk_size,
            chunk_dir=chunk_dir,
            reproject_dir=reproject_dir,
            count_dir=count_dir,
            biome=biome,
            year=year,
            chunk_list=chunk_list,
        )
        for _, year in enumerate(years)
    ]

    deforestation_land_use_counts_list = [
        _ for _ in deforestation_land_use_counts_list if _ is not None and not _.empty
    ]

    if not deforestation_land_use_counts_list:
        return pd.DataFrame()

    dfr = pd.concat(deforestation_land_use_counts_list)
    dfr["counts"] = dfr["num_pixels"]
    # dfr["area"] = PRODES_DEFORESTATION_PIXEL_AREA * dfr["counts"]
    # dfr["score"] = 0.0
    # dfr["percentage"] = 0.0

    return dfr


def build_total_deforestation_indicator_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    years: list[int],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Backward-compatible alias for the renamed annual increase builder."""
    return build_annual_increase_in_deforestation_indicator_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        years=years,
        chunk_list=chunk_list,
    )


def build_vegetation_from_deforestation_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    cache_dir: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    biome: str,
    years: list[int],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Build a vegetation indicator from the deforestation timeline.

    For each year in ``years``, this returns the pixels that remain available as
    vegetation by counting deforestation pixels occurring in later years of the
    same range. The result preserves the grouped spatial-unit columns together
    with ``num_pixels`` and ``year``.
    """
    vegetation_from_deforestation_filename = cache_dir / (
        f"vegetation_from_deforestation_b{biome}_from_y{years[0]}_to_y{years[-1]}.pkl"
    )

    if vegetation_from_deforestation_filename.exists():
        return pd.read_pickle(vegetation_from_deforestation_filename)

    deforestation_land_use_counts_list = [
        build_deforestation_land_use_counts_dataframe(
            db=db,
            land_use_tiff_file=land_use_tiff_file,
            prodes_tiff_file=prodes_tiff_file,
            chunk_size=chunk_size,
            chunk_dir=chunk_dir,
            reproject_dir=reproject_dir,
            count_dir=count_dir,
            biome=biome,
            year=year,
            chunk_list=chunk_list,
        )
        for _, year in enumerate(years)
    ]

    deforestation_land_use_counts_list = [
        _ for _ in deforestation_land_use_counts_list if _ is not None and not _.empty
    ]

    if not deforestation_land_use_counts_list:
        return pd.DataFrame()

    dfr = pd.concat(deforestation_land_use_counts_list)

    dfr_list: list[pd.DataFrame] = []

    for year in years:
        dfr2 = dfr[dfr["year"] > year]  # type: ignore
        dfr2: pd.DataFrame = dfr2.groupby(
            ["suid", "land_use_id", "geocode", "spatial_unit", "biome"],
            as_index=False,
        ).agg(num_pixels=("num_pixels", "sum"))
        dfr2["year"] = year
        dfr_list.append(dfr2)

    if not dfr_list:
        return pd.DataFrame()

    dfr = pd.concat(dfr_list)

    dfr["source"] = "deforestation"

    dfr.to_pickle(vegetation_from_deforestation_filename)

    return dfr


def save_indicator(
    db: DatabaseFacade,
    indicator_dataframe: pd.DataFrame,
    spatial_unit: str,
    classname: str,
):
    """Persist an indicator dataframe into the PRODES land-use table.

    The dataframe must contain the columns used in the INSERT statement:
    `suid`, `land_use_id`, `geocode`, `biome`, `counts`, `area`,
    `percentage`, and `score`. Rows are written in batches to the
    `{PRODES_DB_SCHEMA}.{spatial_unit}_land_use` table using the provided
    `classname`.
    """
    if indicator_dataframe.empty:
        return

    assert np.all(indicator_dataframe["spatial_unit"] == spatial_unit)

    table_name = f"{PRODES_DB_SCHEMA}.{spatial_unit}_land_use"

    def _insert_into_land_use_table(values: list):
        sql = f"INSERT INTO {table_name} (classname, date, suid, land_use_id, geocode, biome, counts, area, percentage, score) VALUES {','.join(values)};"
        db.execute(sql=sql, log=False)

    values = []
    for row in indicator_dataframe.itertuples():
        values.append(
            f"('{classname}', '{row.year}-01-01'::date, {row.suid}, {row.land_use_id}, '{row.geocode}', '{row.biome}', {row.counts}, {row.area}, {row.percentage}, {row.score})"
        )

        if len(values) > 1e5:
            _insert_into_land_use_table(values=values)
            values = []

    if len(values) > 0:
        _insert_into_land_use_table(values=values)
        values = []


def create_prodes_deforestation_indicator_tables(
    db: DatabaseFacade, force_recreate: bool
):
    db.create_schema(name=PRODES_DB_SCHEMA, force_recreate=False)

    for spatial_unit in read_spatial_units(db=db):
        create_prodes_spatial_unit_tables(
            db=db, spatial_unit=spatial_unit, force_recreate=force_recreate
        )


def create_prodes_spatial_unit_tables(
    db: DatabaseFacade,
    spatial_unit: str,
    force_recreate: bool = True,
) -> None:
    """Create the PRODES output tables associated with a spatial unit."""
    for land_use_type in LAND_USE_TYPES:
        if land_use_type != AMS:
            continue

        land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

        land_use_table = f"{spatial_unit}_land_use{land_use_type_suffix}"

        db.create_table(
            schema=PRODES_DB_SCHEMA,
            name=land_use_table,
            columns=[
                "id serial NOT NULL PRIMARY KEY",
                "suid int NOT NULL",
                "land_use_id int NOT NULL",
                "classname varchar(2) NOT NULL",
                "date date NOT NULL",
                "counts int4",
                "area double precision",
                "percentage double precision",
                "score double precision NOT NULL DEFAULT 0.0",
                "geocode character varying(80)",
                "biome character varying(254)",
            ],
            force_recreate=force_recreate,
        )

        db.create_indexes(
            schema=PRODES_DB_SCHEMA,
            name=land_use_table,
            columns=[
                "classname:btree",
                "date:btree",
                "biome:btree",
                "geocode:btree",
                "suid:btree",
            ],
            force_recreate=force_recreate,
        )


def aggregate_vegetation_by_spatial_unit(
    spatial_unit: str,
    spatial_units_gdf: gpd.GeoDataFrame,
    vegetation_points_gdf: gpd.GeoDataFrame,
    biome: str,
) -> pd.DataFrame:
    """Aggregate native vegetation points by spatial unit."""
    spatial_unit_vegetation_counts = gpd.sjoin(
        vegetation_points_gdf,
        spatial_units_gdf,
        how="inner",
        predicate="within",
    )
    spatial_unit_vegetation_counts = spatial_unit_vegetation_counts.groupby(
        ["suid", "source"],
        as_index=False,
    ).agg(num_pixels=("geometry", "count"))

    spatial_unit_vegetation_counts["spatial_unit"] = spatial_unit
    spatial_unit_vegetation_counts["biome"] = biome

    return spatial_unit_vegetation_counts


def build_total_vegetation_indicator_dataframe(
    *,
    db: DatabaseFacade,
    land_use_tiff_file: Path,
    prodes_tiff_file: Path,
    chunk_size: int,
    chunk_dir: Path,
    reproject_dir: Path,
    count_dir: Path,
    cache_dir: Path,
    biome: str,
    years: list[int],
    chunk_list: list[Path] | None = None,
) -> pd.DataFrame:
    """Build the total vegetation dataframe from mask and deforestation data.

    The resulting dataframe combines the vegetation estimated from the native
    vegetation mask with the vegetation derived from the deforestation timeline
    for each year in ``years``. The output is cached to ``cache_dir`` and
    contains one row per spatial aggregation and year, with ``num_pixels``
    as the aggregated vegetation count.
    """
    assert chunk_dir.exists()
    assert reproject_dir.exists()
    assert count_dir.exists()
    assert cache_dir.exists()

    vegetation_from_mask_dfr = build_vegetation_land_use_counts_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=Path(prodes_tiff_file),
        chunk_size=chunk_size,
        chunk_dir=Path(chunk_dir),
        reproject_dir=Path(reproject_dir),
        count_dir=Path(count_dir),
        biome=biome,
        year=PRODES_LAST_YEAR,
        chunk_list=chunk_list,
    )

    vegetation_from_deforestation_dfr = build_vegetation_from_deforestation_dataframe(
        db=db,
        land_use_tiff_file=land_use_tiff_file,
        prodes_tiff_file=prodes_tiff_file,
        cache_dir=cache_dir,
        chunk_size=chunk_size,
        chunk_dir=chunk_dir,
        reproject_dir=reproject_dir,
        count_dir=count_dir,
        biome=biome,
        years=years,
        chunk_list=chunk_list,
    )

    if vegetation_from_deforestation_dfr.empty and vegetation_from_mask_dfr.empty:
        return pd.DataFrame()

    vegetation_dfr_list = []
    for year in years:
        year_dfr_list: list[pd.DataFrame] = []

        if not vegetation_from_mask_dfr.empty:
            year_dfr_list.append(vegetation_from_mask_dfr)

        vdfr = vegetation_from_deforestation_dfr[
            vegetation_from_deforestation_dfr["year"] == year
        ]

        if not vdfr.empty:
            year_dfr_list.append(vdfr)  # type: ignore

        if not year_dfr_list:
            continue

        vdfr = pd.concat(year_dfr_list)
        vdfr = vdfr.groupby(
            ["suid", "land_use_id", "geocode", "spatial_unit", "biome"],
            as_index=False,
        ).agg(num_pixels=("num_pixels", "sum"))
        vdfr["year"] = year
        vdfr["biome"] = biome

        vegetation_dfr_list.append(vdfr)

    if not vegetation_dfr_list:
        return pd.DataFrame()

    vegetation_dfr = pd.concat(vegetation_dfr_list)

    return vegetation_dfr
