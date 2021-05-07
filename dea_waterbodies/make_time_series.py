"""Get the waterbody time histories.

Here we have a shapefile containing polygons we wish to explore. Each polygon is independent from the other, and
so lends itself to simple parallelisation of the workflow.

The code loops through each polygon in the shapefile and writes out a csv of waterbody percentage area full
and wet pixel count.

**Required inputs**

a config file which contains the filename of the Shapefile containing the polygon set of water bodies to be interrogated,
access to a datacube containing wofls.

Geoscience Australia - 2021
    Vanessa Newey
    Matthew Alger
"""

import sys

import click

from dea_waterbodies.waterbody_timeseries_functions import *

@click.command()
@click.argument('config_file')
@click.argument('part', required=False, type=int)
@click.argument('num_chunks', required=False, type=int)
@click.argument('size', required=False, type=click.Choice(['SMALL', 'HUGE', 'ALL']))
def main(config_file, part=1, num_chunks=1, size='ALL'):
    """Make the waterbodies time series."""
    config_dict = process_config(config_file)

    assert size.isupper()

    # the part and num_chunks arguments are for when you are running a large job in parallel
    if part != 1:
        print(f'Working on chunk {part}')

    if num_chunks != 1:
        print(f'Splitting into {num_chunks} chunks')

    # not used if using huge mem
    config_dict['size'] = size
    print(config_dict['size'])

    #Open the shapefile and get the list of polygons
    shapes_subset, crs, id_field = get_shapefile_list(config_dict, part, num_chunks)
    config_dict['crs'] = crs
    config_dict['id_field'] = id_field

    print('config', config_dict)

    # Loop through the polygons and write out a csv of waterbody percentage area full and wet pixel count
    # process each polygon. attempt each polygon 2 times
    for shapes in shapes_subset:
        result = generate_wb_timeseries(shapes, config_dict)
        if not result:
            result = generate_wb_timeseries(shapes, config_dict)


if __name__ == "__main__":
    main()
