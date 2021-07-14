"""Tests for dea_waterbodies.make_chunks.

Matthew Alger
Geoscience Australia
2021
"""

from pathlib import Path
import random
from unittest import mock
import uuid

import pytest

import dea_waterbodies.make_chunks as make_chunks


# Test directory.
HERE = Path(__file__).parent.resolve()

# Path to Canberra test shapefile.
TEST_SHP = str(HERE / 'data' / 'waterbodies_canberra.shp')
TEST_DBF = str(HERE / 'data' / 'waterbodies_canberra.dbf')

# How many polygons are in TEST_SHP.
N_TEST_POLYGONS = 86

# Path to a non-existent S3 shapefile.
TEST_SHP_S3 = 's3://dea-public-data/waterbodies_canberra.shp'
TEST_DBF_S3 = 's3://dea-public-data/waterbodies_canberra.dbf'


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


@pytest.fixture
def config_path_s3(tmp_path):
    config = f"""; config.ini
[DEFAULT]
SHAPEFILE = {TEST_SHP_S3}
"""
    config_path = tmp_path / 'test-config.ini'
    with open(config_path, 'w') as f:
        f.write(config)
    return config_path


def bytesopen(path):
    """Open a file as bytes, like urlopen does."""
    return open(path, 'rb')


def test_get_config_from_s3(config_path_s3):
    with mock.patch('dea_waterbodies.make_chunks.urlopen', wraps=bytesopen):
        config = make_chunks.parse_config(config_path_s3)
        assert config.get('SHAPEFILE')


def test_get_dbf_from_config(config_path):
    with mock.patch('dea_waterbodies.make_chunks.urlopen', wraps=bytesopen):
        config = make_chunks.parse_config(config_path)
        dbf_path = make_chunks.get_dbf_from_config(config)
    assert dbf_path == TEST_DBF


def test_get_dbf_from_config_s3(config_path_s3):
    # This tests an S3 path being included in the config.
    # This was treated as a local path somehow with the error:
    # No such file or directory: '/code/s3:/file.dbf'
    with mock.patch('dea_waterbodies.make_chunks.urlopen', wraps=bytesopen):
        config = make_chunks.parse_config(config_path_s3)
        dbf_path = make_chunks.get_dbf_from_config(config)
    assert dbf_path == TEST_DBF_S3


def test_get_contexts():
    contexts = make_chunks.get_polygon_context(TEST_DBF)
    assert len(contexts) == N_TEST_POLYGONS
    # Lake Burley Griffin
    lbg, = [c for c in contexts if c.uid == 'r3dp1nxh8']
    # Check that areas match to within 10 m^2
    assert round(lbg.area) // 10 == 6478750 // 10


def test_alloc_chunks():
    contexts = [(100, 'a'), (200, 'b'), (100, 'c')]
    contexts = [make_chunks.PolygonContext(a, i, 'NSW')
                for a, i in contexts]
    chunks = make_chunks.alloc_chunks(contexts, 2)
    assert len(chunks) == 2
    assert chunks[0]['ids'] == ['b']
    assert sorted(chunks[1]['ids']) == sorted(['a', 'c'])


def test_alloc_chunks_insufficient_polygons():
    """Less polygons than chunks."""
    contexts = [(100, 'a'), (200, 'b'), (100, 'c')]
    contexts = [make_chunks.PolygonContext(a, i, 'NSW')
                for a, i in contexts]
    chunks = make_chunks.alloc_chunks(contexts, 4)
    assert len(chunks) == 4


def test_alloc_chunks_fuzz():
    """Correct chunk counts for various chunks and polygons."""
    random.seed(0)
    for _ in range(100):
        n_poly = random.randrange(2, 1500)
        n_chunks = random.randrange(1, 150)
        contexts = [make_chunks.PolygonContext(
                random.randrange(1, 10000),
                str(uuid.uuid4()),
                'QLD')
            for _ in range(n_poly)]
        chunks = make_chunks.alloc_chunks(contexts, n_chunks)
        assert len(chunks) == n_chunks, 'Expected {} chunks, got {}'.format(
            n_chunks,
            len(chunks),
        )


def test_filter_state():
    """PolygonContexts are filtered by state."""
    all_states = ['SA', 'VIC', 'OT']
    random.seed(0)
    for _ in range(10):
        n_contexts = random.randrange(100, 1000)
        uids = [str(random.randrange(100000))
                for _ in range(n_contexts)]
        states = [random.choice(all_states)
                  for _ in range(n_contexts)]
        contexts = [
            make_chunks.PolygonContext(0, uid, state)
            for uid, state in zip(uids, states)
        ]
        for s in all_states:
            res = make_chunks.filter_polygons_by_context(
                contexts, '/', False, s)
            assert all(c.state == s for c in res)
        # Test not filtering by state
        res = make_chunks.filter_polygons_by_context(
            contexts, '/', False, None)
        assert len(res) == len(contexts)
        assert all(c1 == c2 for c1, c2 in zip(res, contexts))
