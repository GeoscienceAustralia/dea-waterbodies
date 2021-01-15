from pathlib import Path

import pytest
import geopandas as gpd

from dea_waterbodies import make_polygons

def test_ok():
    assert True
    
GINNINDERRA_BBOX = 149.054596, -35.242293, 149.095430, -35.211391

def test_main(tmp_path):
    # This should really stub out the datacube, but for now we'll use an actual datacube
    out_path = tmp_path / 'wb_outputs'
    out_path.mkdir()
    make_polygons.main(bbox=GINNINDERRA_BBOX,
                       crs='EPSG:4326',
                       minimum_wet_percentage_detection=0.1,
                       minimum_wet_percentage_extent=0.05,
                       min_area_m2=3125,
                       max_area_m2=5000000000,
                       min_valid_observations=128,
                       apply_min_valid_observations_first=True,
                       urban_mask=True,
                       sa3_urban_areas=make_polygons.DEFAULT_SA3_URBAN,
                       sa3_filepath=make_polygons.URBAN_SA3_PATH,
                       handle_large_polygons='nothing',
                       pp_thresh=0.005,
                       base_filename='waterbodies_test_main',
                       output_path=out_path)
    assert (out_path / 'waterbodies_test_main.shp').exists()
    
    file = gpd.read_file(out_path / 'waterbodies_test_main.shp')
    assert len(file) == 6
