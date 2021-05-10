from pathlib import Path

from click.testing import CliRunner
import pytest
import geopandas as gpd

from dea_waterbodies.make_time_series import main

def test_main():
    runner = CliRunner()
    result = runner.invoke(main, [])
    print(result)
