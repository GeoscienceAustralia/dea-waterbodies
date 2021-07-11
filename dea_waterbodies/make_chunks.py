"""Split a set of waterbodies into multiple chunks.

Matthew Alger
Geoscience Australia
2021
"""

import configparser
import json
from pathlib import Path
import tempfile
from urllib.request import urlopen

import click
import fsspec
from osgeo import ogr


def get_dbf_from_config(config_path) -> str:
    """Find the DBF file specified in a config.

    Must return a string, not a Path, in case there's a protocol.
    """
    # Download the config file to find the shapefile.
    with urlopen(config_path) as config_file:
        parser = configparser.ConfigParser()
        parser.read_string(config_file.read().decode('ascii'))
    config = parser['DEFAULT']
    shp_path = config['SHAPEFILE']
    dbf_path = shp_path.replace('shp', 'dbf')
    return dbf_path


def get_areas_and_ids(dbf_path):
    """Download and process a DBF."""
    # Can't use pathlib here in case we have an S3 URI instead of a local one.
    dbf_name = dbf_path.split('/')[-1]
    dbf_stem = dbf_name.split('.')[0]
    with tempfile.TemporaryDirectory() as tempdir:
        with fsspec.open(dbf_path, 'rb') as f:
            dbf_dump_path = Path(tempdir) / dbf_name
            with open(dbf_dump_path, 'wb') as g:
                g.write(f.read())

        # Get the areas.
        ds = ogr.Open(str(dbf_dump_path), 0)
        layer = ds.ExecuteSQL(f'select area, UID from {dbf_stem}')
        area_ids = [(float(i.GetField('area')), i.GetField('UID'))
                    for i in layer]

    return area_ids


def alloc_chunks(area_ids, n_chunks):
    """Allocate waterbodies to chunks with estimated memory usage."""
    # Split the waterbodies up by area. First, sort (by area):
    area_ids.sort(reverse=True)
    # Get the total area as we'll be dividing into chunks based on this.
    total_area = sum(a for a, i in area_ids)

    area_budget = total_area / n_chunks
    # Divide the areas into chunks.
    area_chunks = []
    current_area_budget = 0
    current_area_chunk = []
    to_alloc = area_ids[::-1]
    while to_alloc:
        area, id_ = to_alloc.pop()
        current_area_budget += area
        current_area_chunk.append(id_)
        if current_area_budget >= area_budget:
            current_area_budget = 0
            area_chunks.append(current_area_chunk)
            current_area_chunk = []
            total_area = sum(a for a, _ in to_alloc)
            n_remaining_chunks = n_chunks - len(area_chunks)
            if n_remaining_chunks == 0 and to_alloc:
                raise RuntimeError('Not enough chunks remaining')
            if not to_alloc:
                break  # Avoid zero division
            area_budget = total_area / n_remaining_chunks
    
    while len(area_chunks) < n_chunks:
        area_chunks.append([])

    # Estimate memory usage for each chunk.
    id_to_area = dict([i[::-1] for i in area_ids])

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


@click.command()
@click.argument('config_path')
@click.argument('n_chunks', type=int)
def main(config_path, n_chunks):
    dbf_path = get_dbf_from_config(config_path)
    area_ids = get_areas_and_ids(dbf_path)
    out = alloc_chunks(area_ids, n_chunks)
    print(json.dumps({'chunks': out}))


if __name__ == "__main__":
    main()
