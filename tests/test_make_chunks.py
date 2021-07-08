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
TEST_DBF = HERE / 'data' / 'waterbodies_canberra.dbf'

# How many polygons are in TEST_SHP.
N_TEST_POLYGONS = 86


@pytest.fixture
def config_path(tmp_path):
    config = f"""; config.ini
[DEFAULT]
SHAPEFILE = {TEST_SHP}
"""
    config_path = tmp_path / 'test-config.ini'
    with open(config_path, 'w') as f:
        f.write(config)
    return config_path


def bytesopen(path):
    """Open a file as bytes, like urlopen does."""
    return open(path, 'rb')


def test_get_dbf_from_config(config_path):
    with mock.patch('dea_waterbodies.make_chunks.urlopen', wraps=bytesopen):
        dbf_path = make_chunks.get_dbf_from_config(config_path)
    assert dbf_path.parent == TEST_SHP.parent
    assert dbf_path.stem == TEST_SHP.stem
    assert dbf_path.suffix == '.dbf'
    assert dbf_path == TEST_DBF


def test_get_areas_and_ids():
    area_ids = make_chunks.get_areas_and_ids(TEST_DBF)
    assert len(area_ids) == N_TEST_POLYGONS
    # Lake Burley Griffin
    lbg, = [(a, i) for a, i in area_ids if i == 'r3dp1nxh8']
    # Check that areas match to within 10 m^2
    assert round(lbg[0]) // 10 == 6478750 // 10


def test_alloc_chunks():
    area_ids = [(100, 'a'), (200, 'b'), (100, 'c')]
    chunks = make_chunks.alloc_chunks(area_ids, 2)
    assert len(chunks) == 2
    assert chunks[0]['ids'] == ['b']
    assert sorted(chunks[1]['ids']) == sorted(['a', 'c'])
