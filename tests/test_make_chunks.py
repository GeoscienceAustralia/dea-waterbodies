"""Tests for dea_waterbodies.make_chunks.

Matthew Alger
Geoscience Australia
2021
"""

from unittest import mock
from pathlib import Path

import pytest

import dea_waterbodies.make_chunks as make_chunks


# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = HERE / 'data' / 'waterbodies_canberra.shp'


@pytest.fixture
def config_path(tmp_path):
    config = f"""; config.ini
SHAPEFILE = {TEST_SHP}
"""
    config_path = tmp_path / 'test-config.ini'
    with open(config_path, 'w') as f:
        f.write(config)
    return config_path


def test_get_dbf_from_config(config_path):
    with mock.patch('urllib.request.urlopen', new=open):
        dbf_path = make_chunks.get_dbf_from_config(config_path)
    assert dbf_path.parent == TEST_SHP.parent
    assert dbf_path.stem == TEST_SHP.stem
    assert dbf_path.suffix == '.dbf'
