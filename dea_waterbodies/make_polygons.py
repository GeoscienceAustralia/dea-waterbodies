"""Make waterbody polygons from the Water Observations from Space all-time
summary.

Geoscience Australia - 2021
    Claire Krause
    Matthew Alger
"""

from pathlib import Path
import math
from typing import Container, Tuple

import geopandas as gp
import pandas as pd
import geohash as gh
import datacube
import numpy as np
import rioxarray  # noqa: F401
from dea_tools.spatial import xr_vectorize, xr_rasterize


# Sydney, Melbourne, Brisbane, Broadbeach, Surfers, Adelaide, Perth
# This must be a list for pandas indexing to work.
DEFAULT_SA3_URBAN = [11703, 20604, 30501, 30901, 30910, 40101, 50302]

# Bounding box of Menindee Lakes.
BBOX_MENINDEE = (141.93057, -32.74068, 142.93699, -32.03379)

# Path to urban_sa3.geojson, which stores the urban SA3 areas for masking.
URBAN_SA3_PATH = Path(__file__).parent / 'urban_sa3.geojson'


def main(
        bbox: Tuple[int] = BBOX_MENINDEE,
        crs: str = 'EPSG:4326',
        minimum_wet_percentage_detection: float = 0.1,
        minimum_wet_percentage_extent: float = 0.05,
        min_area_m2: int = 3125,
        max_area_m2: int = 5000000000,
        min_valid_observations: int = 128,
        apply_min_valid_observations_first: bool = True,
        urban_mask: bool = True,
        sa3_urban_areas: Container[int] = DEFAULT_SA3_URBAN,
        sa3_filepath: Path = Path('SA3_2016_AUST.shp'),
        handle_large_polygons: str = 'nothing',
        pp_thresh: float = 0.005,
        base_filename: str = 'waterbodies',
        output_path: Path = Path('_wb_outputs/'),
        ):
    minimum_wet_percentage = [minimum_wet_percentage_extent,
                              minimum_wet_percentage_detection]
    xlim = bbox[::2]
    ylim = bbox[1::2]

    # Load WOfS.
    # Set up the datacube to get DEA data.
    dc = datacube.Datacube(app='WaterbodyPolygons')

    # Some query parameters.
    dask_chunks = {'x': 3000, 'y': 3000, 'time': 1}
    # Resolution of WOfS, which changes depending on which collection you use.
    resolution = (-25, 25)

    # Then load the WOfS summary of clear/wet observations:
    wofs_ = dc.load('wofs_summary', x=xlim, y=ylim, dask_chunks=dask_chunks)
    wofs = wofs_.isel(time=0)
    # And set the no-data values to nan:
    wofs = wofs.where(wofs != -1)

    # Also load the all-time summary:
    wofs_filtered_summary = dc.load(
        'wofs_filtered_summary', x=xlim, y=ylim,
        crs=crs, dask_chunks=dask_chunks).isel(time=0)

    # Filter pixels with at least min_valid_observations times.
    wofs_valid_filtered = wofs.count_clear >= min_valid_observations

    # Initial polygon detection.
    for threshold in minimum_wet_percentage:
        # Remove any pixels that are wet < AtLeastThisWet% of the time
        wofs_filtered = wofs_filtered_summary.wofs_filtered_summary > threshold

        # Now find pixels that meet both the minimum valid observations
        # and wetness percentage criteria

        # Change all zeros to NaN to create a nan/1 mask layer
        # Pixels == 1 now represent our water bodies
        if apply_min_valid_observations_first:
            wofs_filtered = wofs_filtered.where(
                wofs_filtered & wofs_valid_filtered)
        else:
            wofs_filtered = wofs_filtered.where(wofs_filtered)

        # Vectorise the raster.
        polygons = xr_vectorize((wofs_filtered == 1).values, crs='EPSG:3577',
                                transform=wofs_filtered_summary.rio.transform()
                                )
        polygons = polygons[polygons.attribute == 1].reset_index(drop=True)

        # Combine any overlapping polygons
        polygons = polygons.geometry.buffer(0).unary_union

        # Turn the combined multipolygon back into a geodataframe
        polygons = gp.GeoDataFrame(
            geometry=[poly for poly in polygons])
        # We need to add the crs back onto the dataframe
        polygons.crs = 'EPSG:3577'

        # Calculate the area of each polygon again now that overlapping
        # polygons have been merged
        polygons['area'] = polygons.area

        # Save the polygons to a shapefile
        filename = output_path / f'{base_filename}_raw_{threshold}.shp'
        polygons.to_file(filename)

    # Make sure we are using the detection threshold not the extent threshold.
    assert threshold == minimum_wet_percentage[1]
    assert minimum_wet_percentage[1] == max(minimum_wet_percentage)
    polygons = gp.read_file(
        output_path / f'{base_filename}_raw_{minimum_wet_percentage[1]}.shp')

    # Filter polygons by size.
    polygons = polygons[
        (polygons['area'] >= min_area_m2) & (polygons['area'] <= max_area_m2)]

    # Load the coastline.
    coastline = dc.load('geodata_coast_100k', output_crs='EPSG:3577', x=xlim,
                        y=ylim, resolution=resolution)

    # Mark any polygon that intersects with the sea as ocean.
    # Set up a column to fill the raster with.
    polygons['polygon_idx'] = range(1, len(polygons) + 1)
    # Then rasterise the polygons to find the area they cover.
    raster = xr_rasterize(polygons, wofs, attribute_col='polygon_idx')
    # Multiply the ocean mask by the polygon_idx at each pixel to find
    # which values of polygon_idx overlap with water and therefore are ocean.
    ocean_ids = pd.unique((raster * (coastline.land == 0)).values.ravel())

    # Exclude the ocean.
    polygons = polygons[~polygons.polygon_idx.isin(ocean_ids)]

    # Filter the CBDs.
    if urban_mask:
        # Read in the SA3 regions.
        sa3 = gp.read_file(sa3_filepath)
        sa3['SA3_CODE16'] = sa3['SA3_CODE16'].astype(int)
        # Get all the regions which are CBDs.
        cbds = sa3.set_index('SA3_CODE16').loc[sa3_urban_areas]
        # Then remove all polygons that intersect with the CBDs.
        city_overlay = gp.overlay(polygons, cbds.to_crs('EPSG:3577'))
        polygons = polygons[
            ~polygons.polygon_idx.isin(city_overlay.polygon_idx)]

    # Combine detected polygons with their maximum extent boundaries.
    # Note that this assumes that the thresholds have been correctly entered
    # into the 'minimum_wet_percentage' variable, with the extent threshold
    # listed first.
    lower_threshold = gp.read_file(
        output_path / f'{base_filename}_raw_{minimum_wet_percentage[0]}.shp')
    lower_threshold['area'] = pd.to_numeric(lower_threshold.area)
    # Filter out those pesky huge polygons
    lower_threshold = lower_threshold.loc[
        (lower_threshold['area'] <= max_area_m2)]
    lower_threshold['lt_index'] = range(len(lower_threshold))
    # Pull out the polygons from the extent shapefile that intersect with
    # the detection shapefile
    overlay_extent = gp.overlay(polygons, lower_threshold)
    lower_threshold_to_use = lower_threshold.loc[overlay_extent.lt_index]
    # Combine the polygons
    polygons = gp.GeoDataFrame(
        pd.concat([lower_threshold_to_use, polygons], ignore_index=True))
    # Merge overlapping polygons
    polygons = polygons.unary_union
    # Back into individual polygons
    polygons = gp.GeoDataFrame(crs='EPSG:3577', geometry=[polygons]).explode()

    # Get rid of the multiindex that explode added:
    polygons = polygons.reset_index(drop=True)

    # Add area, perimeter, and polsby-popper columns:
    polygons['area'] = polygons.area
    polygons['perimeter'] = polygons.length
    polygons['pp_test'] = polygons.area * 4 * math.pi / polygons.perimeter ** 2

    # Dump the merged polygons to a file:
    polygons.to_file(output_path / f'{base_filename}_merged.shp')

    # Split large polygons.
    if handle_large_polygons == 'erode-dilate-v1':
        needs_buffer = polygons[polygons.pp_test <= pp_thresh]
        unbuffered = needs_buffer.buffer(-50)
        unbuffered = unbuffered.explode().reset_index(drop=True).buffer(50)
        unbuffered = gp.GeoDataFrame(geometry=unbuffered, crs='EPSG:3577')
        unbuffered['area'] = unbuffered.area
        unbuffered['perimeter'] = unbuffered.length
        unbuffered['pp_test'] = (
            unbuffered.area * 4 * math.pi / unbuffered.perimeter ** 2)
        polygons = pd.concat(
            [polygons[polygons.pp_test > pp_thresh], unbuffered],
            ignore_index=True)

        # TODO: This doesn't quite match DEA Waterbodies. Need to cut based on
        # the polygons instead.

    if handle_large_polygons == 'erode-dilate-v2':
        splittable = polygons[polygons.pp_test <= pp_thresh]
        if len(splittable) >= 1:
            unbuffered = splittable.buffer(-100)
            buffered = unbuffered.buffer(125)
            subtracted = gp.overlay(splittable, gp.GeoDataFrame(
                geometry=[buffered.unary_union], crs=splittable.crs),
                how='difference').explode().reset_index(drop=True)
            resubtracted = gp.overlay(
                splittable, subtracted, how='difference'
                ).explode().reset_index(drop=True)

            # Assign each chopped-off bit of the polygon to its nearest big
            # neighbour.
            unassigned = np.ones(len(subtracted), dtype=bool)
            recombined = []
            for i, poly in resubtracted.iterrows():
                mask = (subtracted.exterior.intersects(poly.geometry.exterior)
                        & unassigned)
                neighbours = subtracted[mask]
                unassigned[mask] = False
                poly = poly.geometry.union(neighbours.unary_union)
                recombined.append(poly)

            # All remaining polygons are not part of a big polygon.
            results = pd.concat([gp.GeoDataFrame(geometry=recombined),
                                subtracted[unassigned],
                                polygons[polygons.pp_test > pp_thresh]],
                                ignore_index=True)

            polygons = results.explode().reset_index(drop=True)

    if handle_large_polygons == 'nothing':
        print('Not splitting large polygons')

    if not apply_min_valid_observations_first:
        polygons['one_idx'] = range(1, len(polygons) + 1)
        polygon_mask = xr_rasterize(polygons, wofs, attribute_col='one_idx')
        counts = []
        for i in polygons.one_idx:
            mask = polygon_mask == i
            count = wofs.count_clear.values[mask].max()
            counts.append(count)
        polygons['n_valid_observations'] = counts
        polygons = polygons[
            polygons.n_valid_observations >= min_valid_observations]

    # Generate a unique ID for each polygon.
    # We need to convert from Albers coordinates to lat/lon, in order to
    # generate the geohash
    polygons_4326 = polygons.to_crs(epsg=4326)

    # Generate a geohash for the centroid of each polygon
    polygons_4326['UID'] = polygons_4326.apply(
        lambda x: gh.encode(x.geometry.centroid.y,
                            x.geometry.centroid.x, precision=9), axis=1)

    # Check that our unique ID is in fact unique
    assert polygons_4326['UID'].is_unique

    # Make an arbitrary numerical ID for each polygon. We will first sort the
    # dataframe by geohash so that polygons close to each other are numbered
    # similarly
    sorted_polygons = polygons_4326.sort_values(by=['UID']).reset_index()
    sorted_polygons['WB_ID'] = sorted_polygons.index

    # The step above creates an 'index' column, which we don't actually want,
    # so drop it.
    sorted_polygons.drop(labels='index', axis=1, inplace=True)

    polygons.to_file(output_path / f'{base_filename}.shp',
                     driver='ESRI Shapefile')
    polygons.to_file(output_path / f'{base_filename}.geojson',
                     driver='GeoJSON')
