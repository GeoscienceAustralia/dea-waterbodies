from pathlib import Path
import re

from click.testing import CliRunner
import pytest
import geopandas as gpd

from dea_waterbodies.make_time_series import main

# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'


@pytest.fixture
def runner():
    return CliRunner()


def test_main(runner):
    result = runner.invoke(main, [])
    assert result


def test_make_one_csv(runner, tmp_path):
    ginninderra_id = 'r3dp84s8n'
    result = runner.invoke(main, [
        ginninderra_id,
        '--config', TEST_SHP,
        '--output', tmp_path,
    ])
    assert result
    expected_out_path = tmp_path / ginninderra_id[:4] / f'{ginninderra_id}.csv'
    print(expected_out_path)
    print(list(tmp_path.iterdir()))
    print([list(l.iterdir()) for l in tmp_path.iterdir()])
    assert expected_out_path.exists()
    csv = gpd.pd.read_csv(expected_out_path, sep=',')
    assert csv.columns[0] == 'Observation Date'
    assert csv.columns[1] == 'Wet pixel percentage'
    assert re.match(r'Wet pixel count (n = \d+)', csv.columns[2])
    assert csv.columns[2] == 'Wet pixel count (n = 1358)'
    assert csv.iloc[0]['Observation Date'].startswith('2021-03-30')
    assert int(csv.iloc[0]['Wet pixel count (n = 1358)']) == 1200
