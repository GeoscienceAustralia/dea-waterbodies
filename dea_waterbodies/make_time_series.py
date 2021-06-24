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

import configparser
import logging
import sys

import click

import dea_waterbodies

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def process_config(config_file):
    config = configparser.ConfigParser()
    config_dict = {}
    config.read(config_file)
    start_date = '1986'
    if 'SHAPEFILE' in config['DEFAULT'].keys():
        config_dict['shape_file'] = config['DEFAULT']['SHAPEFILE']

    if 'START_DATE' in config['DEFAULT'].keys():
        config_dict['start_dt'] = config['DEFAULT']['START_DATE']
        logger.info(f'START_DATE {start_date}')

    if 'END_DATE' in config['DEFAULT'].keys():
        config_dict['end_date'] = config['DEFAULT']['END_DATE']
    if 'SIZE' in config['DEFAULT'].keys():
        config_dict['size'] = config['DEFAULT']['SIZE'].upper()
    else:
        config_dict['size'] = 'ALL'
    if 'MISSING_ONLY' in config['DEFAULT'].keys():
        if config['DEFAULT']['MISSING_ONLY'].upper() == 'TRUE':
            config_dict['missing_only'] = True
        else:
            config_dict['missing_only'] = False
    else:
        config_dict['missing_only'] = False

    if 'PROCESSED_FILE' in config['DEFAULT'].keys():
        if len(config['DEFAULT']['PROCESSED_FILE']) > 2:
            config_dict['processed_file'] = config['DEFAULT']['PROCESSED_FILE']
        else:
            config_dict['processed_file'] = ''
    else:
        config_dict['processed_file'] = ''

    if 'TIME_SPAN' in config['DEFAULT'].keys():
        config_dict['time_span'] = config['DEFAULT']['TIME_SPAN'].upper()
    else:
        config_dict['time_span'] = 'ALL'

    if 'OUTPUTDIR' in config['DEFAULT'].keys():
        config_dict['output_dir'] = config['DEFAULT']['OUTPUTDIR']

    if 'FILTER_STATE' in config['DEFAULT'].keys():
        config_dict['filter_state'] = config['DEFAULT']['FILTER_STATE']

    if 'UNCERTAINTY' in config['DEFAULT'].keys():
        if config['DEFAULT']['UNCERTAINTY'].upper() == 'TRUE':
            config_dict['include_uncertainty'] = True
    else:
        config_dict['include_uncertainty'] = False

    return config_dict

@click.command()
@click.argument('ids', required=False)
@click.option('--config', '-c', type=click.Path())
@click.option('--shapefile', type=click.Path())
@click.option('--start', type=str)
@click.option('--end', type=str)
@click.option('--size', type=click.Choice(['ALL', 'SMALL', 'HUGE']))
@click.option('--missing-only/--all', default=False)
@click.option('--skip', type=click.Path())
@click.option('--time-span', '--time', type=click.Choice(['ALL', 'APPEND', 'CUSTOM']))
@click.option('--output', type=click.Path())
@click.option('--state', type=click.Choice(['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']))
@click.option('--no-mask-obs/--mask-obs', default=False)
@click.version_option(version=dea_waterbodies.__version__)
def main(ids, config, shapefile, start, end, size,
         missing_only, skip, time_span, output, state,
         no_mask_obs):
    """Make the waterbodies time series."""

    # If we've specified a config file, load it in.
    if config:
        config_dict = process_config(config)
    else:
        # Otherwise, we have some mandatory arguments.
        required = [
            'shapefile',
            'output',
        ]
        locals_ = locals()
        missing = [r for r in required if not locals_[r]]
        if missing:
            raise click.ClickException(
                'If a config file is not specified, then {} required'.format(
                    ', '.join('--' + r for r in missing)
                ))
        config_dict = {}

    # Override config options with command-line options.
    # This dict maps CLI options to config params.
    override_param_map = {
        'shapefile': 'shape_file',
        'start': 'start_dt',
        'end': 'end_date',
        'size': 'size',
        'missing_only': 'missing_only',
        'skip': 'processed_file',
        'time_span': 'time_span',
        'output': 'output_dir',
        'state': 'filter_state',
        'no_mask_obs': 'include_uncertainty',
    }
    locals_ = locals()
    for cli_p, config_p in override_param_map.items():
        cli_val = locals_[cli_p]
        if cli_val:
            config_dict[config_p] = cli_val

    print(config_dict)
    raise NotImplementedError()

    # Do the import here so that the CLI is fast, because this import is sloooow.
    import dea_waterbodies.waterbody_timeseries_functions as dw_wtf

    num_chunks = chunks

    assert config_dict['size'].isupper()

    # the part and num_chunks arguments are for when you are running a large job in parallel
    if part != 1:
        print(f'Working on chunk {part}')

    if num_chunks != 1:
        print(f'Splitting into {num_chunks} chunks')

    # not used if using huge mem
    print('Size:', config_dict['size'])

    #Open the shapefile and get the list of polygons
    shapes_subset, crs, id_field = dw_wtf.get_shapefile_list(config_dict, part, num_chunks)
    config_dict['crs'] = crs
    config_dict['id_field'] = id_field

    print('config', config_dict)

    # Loop through the polygons and write out a csv of waterbody percentage area full and wet pixel count
    # process each polygon. attempt each polygon 2 times
    for i, shapes in enumerate(shapes_subset):
        logger.info('Processing {} ({}/{})'.format(
            shapes['properties'][config_dict['id_field']],
            i + 1,
            len(shapes_subset)))
        result = dw_wtf.generate_wb_timeseries(shapes, config_dict)
        if not result:
            result = dw_wtf.generate_wb_timeseries(shapes, config_dict)


if __name__ == "__main__":
    main()
