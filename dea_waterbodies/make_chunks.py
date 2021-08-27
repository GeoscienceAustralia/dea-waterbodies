"""Split a set of waterbodies into multiple chunks.

Matthew Alger
Geoscience Australia
2021
"""

from collections import namedtuple
import configparser
import json
import logging
import os.path
from pathlib import Path
import tempfile
from urllib.request import urlopen
import uuid

import click
import fsspec
import geopandas as gpd
from osgeo import ogr

logger = logging.getLogger(__name__)

PolygonContext = namedtuple('PolygonContext', 'area uid state')


def get_dbf_from_config(config: dict) -> str:
    """Find the DBF file specified in a config.

    Must return a string, not a Path, in case there's a protocol.
    """
    shp_path = config['SHAPEFILE']
    dbf_path = shp_path.replace('shp', 'dbf')
    return dbf_path


def get_output_path_from_config(config_path: str, config: dict) -> str:
    """Find the output path based on a config.

    Must return a string, not a Path, in case there's a protocol.
    """
    out_dir = config['OUTPUTDIR']
    out_fname = os.path.split(config_path)[-1] + '_' + \
        str(uuid.uuid4()) + '.json'
    return os.path.join(out_dir, out_fname)


def get_polygon_context(path, extent_area=True):
    """Download and process a DBF or SHP.
    
    Arguments
    ---------
    path : str
        Path to DBF or SHP file. Must be DBF if not extent_area
        and must be SHP if extent_area.
    
    extent_area : bool
        Default True. Whether to use the extent as the area
        instead of the actual polygon area.
    
    Returns
    -------
    [PolygonContext]
    """
    if extent_area and not path.endswith('.shp'):
        raise ValueError('If extent_area then path must be to a SHP.')
    if not extent_area and not path.endswith('.dbf'):
        raise ValueError('If not extent_area then path must be to a DBF.')

    if not extent_area:
        # Can't use pathlib here in case we have an S3 URI instead of a local one.
        dbf_name = path.split('/')[-1]
        dbf_stem = dbf_name.split('.')[0]
        with tempfile.TemporaryDirectory() as tempdir:
            with fsspec.open(path, 'rb') as f:
                dbf_dump_path = Path(tempdir) / dbf_name
                with open(dbf_dump_path, 'wb') as g:
                    g.write(f.read())

            # Get the areas.
            ds = ogr.Open(str(dbf_dump_path), 0)
            layer = ds.ExecuteSQL(f'select area, UID, state from {dbf_stem}')
            area_ids = [PolygonContext(
                        float(i.GetField('area')),
                        i.GetField('UID'),
                        i.GetField('state'))
                        for i in layer]
    else:
        shp = gpd.read_file(path)
        area_ids = []
        for i, poly in shp.iterrows():
            area_ids.append(
                PolygonContext(
                    poly.geometry.envelope.area,
                    poly.UID,
                    poly.STATE,
                )
            )

    return area_ids


def construct_path(output_path: str, uid: str):
    """Construct the path to a waterbody CSV."""
    # TODO(MatthewJA): Move this somewhere more general.
    return os.path.join(output_path, uid[:4], uid)


def filter_polygons_by_context(
        contexts: [PolygonContext],
        output_path: str,
        missing_only: bool,
        filter_state: str or None):
    """Filter polygons based on their properties."""
    state_filtered = [
        c for c in contexts
        if not filter_state or c.state == filter_state]
    # Now filter to see if the output file already exists.
    if missing_only:
        missing_filtered = []
        for context in state_filtered:
            path = construct_path(output_path, context.uid)
            try:
                with fsspec.open(path, 'r') as _:
                    # This exists!
                    logger.debug(f'{path} exists')
                    continue
            except FileNotFoundError:
                missing_filtered.append(context)
                continue

            raise RuntimeError('Unreachable')
    else:
        missing_filtered = state_filtered

    return missing_filtered


def alloc_chunks(contexts, n_chunks):
    """Allocate waterbodies to chunks with estimated memory usage."""
    # Split the waterbodies up by area. First, sort (by area):
    contexts.sort(key=lambda c: c.area, reverse=True)
    # Get the total area as we'll be dividing into chunks based on this.
    total_area = sum(c.area for c in contexts)

    area_budget = total_area / n_chunks
    # Divide the areas into chunks.
    area_chunks = []
    current_area_budget = 0
    current_area_chunk = []
    to_alloc = contexts[::-1]
    while to_alloc:
        context = to_alloc.pop()
        current_area_budget += context.area
        current_area_chunk.append(context.uid)
        if current_area_budget >= area_budget:
            current_area_budget = 0
            area_chunks.append(current_area_chunk)
            current_area_chunk = []
            factor_of_safety = 1
            total_area = sum(c.area for c in to_alloc)
            total_area *= factor_of_safety
            n_remaining_chunks = n_chunks - len(area_chunks)
            if n_remaining_chunks == 0 and to_alloc:
                raise RuntimeError('Not enough chunks remaining')
            if not to_alloc:
                break  # Avoid zero division
            area_budget = total_area / n_remaining_chunks

    while len(area_chunks) < n_chunks:
        area_chunks.append([])

    # Estimate memory usage for each chunk.
    id_to_area = {c.uid: c.area for c in contexts}

    def est_mem(id_):
        # Found this number empirically
        slope = 1.6271623728841915e-05
        # And guessed the intercept.
        # Area in m^2, result in MB.
        return id_to_area[id_] * slope + 320

    # Output chunks as JSON.
    out = [
        {'max_mem_Mi': max(est_mem(i) for i in chunk) if chunk else 0,
         'ids': chunk}
        for chunk in area_chunks
    ]
    return out


def parse_config(config_path: str):
    with urlopen(config_path) as config_file:
        parser = configparser.ConfigParser()
        parser.read_string(config_file.read().decode('ascii'))
    return parser['DEFAULT']


@click.command()
@click.argument('config_path')
@click.argument('n_chunks', type=int)
def main(config_path, n_chunks):
    config = parse_config(config_path)
    shp_path = config['SHAPEFILE']
    out_path = get_output_path_from_config(config_path, config)
    contexts = get_polygon_context(shp_path, extent_area=True)
    missing_only = config['MISSING_ONLY']
    if not isinstance(missing_only, bool):
        missing_only = missing_only.upper() == 'TRUE'
    assert isinstance(missing_only, bool)
    filter_state = config.get('FILTER_STATE', None)
    filtered = filter_polygons_by_context(
        contexts, config['OUTPUTDIR'],
        missing_only, filter_state)
    out = alloc_chunks(filtered, n_chunks)
    with fsspec.open(out_path, 'w') as f:
        json.dump({'chunks': out}, f)
    print(json.dumps({'chunks_path': out_path}), end='')


if __name__ == "__main__":
    main()
