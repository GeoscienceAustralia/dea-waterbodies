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
from pathlib import Path
import sys

import click

import dea_waterbodies

logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
stdout_hdlr = logging.StreamHandler(sys.stdout)
logger.addHandler(stdout_hdlr)
logger.setLevel(logging.INFO)


def process_config(config_file: Path) -> dict:
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


def get_crs(shapefile_path: Path) -> 'datacube.geometry.CRS':
    from datacube.utils import geometry
    import fiona
    with fiona.open(shapefile_path) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
    return crs


def guess_id_field(shapefile_path: Path) -> str:
    import fiona
    with fiona.open(shapefile_path) as shapes:
        row = next(iter(shapes))
        keys = set(row['properties'].keys())
    possible_guesses = [
        # In order of preference.
        'UID', 'WB_ID', 'FID_1', 'FID', 'ID', 'OBJECTID',
    ]
    for guess in possible_guesses:
        if guess in keys:
            return guess
    raise ValueError(
        'Couldn\'t find an ID field in {}'.format(keys))


def get_shapes(config_dict: dict, wb_ids: [str], id_field: str) -> [dict]:
    import fiona
    output_dir = config_dict['output_dir']

    # If missing_only, remove waterbodies that already exist.
    if config_dict['missing_only']:
        logger.info("Filtering waterbodies with existing outputs")
        # NOTE(MatthewJA): I removed references to processed_file here -
        # I think it should be captured later. If this induces a bug,
        # here's a great place to start looking.
        # TODO(MatthewJA): Use Paths earlier on and don't convert here.
        output_dir = Path(config_dict['output_dir'])
        missing_list = []
        for id_ in wb_ids:
            out_path = output_dir / id_[:4] / f'{id}.csv'
            if out_path.exists():
                continue
            
            missing_list.append(id_)
        wb_ids = missing_list

        logger.info(
            f'{len(missing_list)} missing polygons to process')
    
    # Filter the list of shapes to include only specified polygons,
    # possibly constrained to a state.
    filtered_shapes = []
    wb_ids = set(wb_ids)  # for quick membership lookups
    with fiona.open(config_dict['shape_file']) as shapes:
        for shape in shapes:
            if shape['properties'][id_field] not in wb_ids:
                continue
            
            if 'filter_state' in config_dict and shape['properties']['STATE'] != config_dict['filter_state']:
                continue
                
            filtered_shapes.append(shape)

    return filtered_shapes, id_field

@click.command()
@click.argument('ids', required=False)
@click.option('--config', '-c', type=click.Path())
@click.option('--shapefile', type=click.Path())
@click.option('--start', type=str)
@click.option('--end', type=str)
@click.option('--size', type=click.Choice(['ALL', 'SMALL', 'HUGE']))
@click.option('--missing-only/--not-missing-only', default=False)
@click.option('--skip', type=click.Path())
@click.option('--time-span', type=click.Choice(['ALL', 'APPEND', 'CUSTOM']))
@click.option('--output', type=click.Path())
@click.option('--state', type=click.Choice(['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']))
@click.option('--no-mask-obs/--mask-obs', default=False)
@click.option('--all/--some', default=False)
@click.version_option(version=dea_waterbodies.__version__)
def main(ids, config, shapefile, start, end, size,
         missing_only, skip, time_span, output, state,
         no_mask_obs, all):
    """Make the waterbodies time series."""

    # TODO(MatthewJA): Read ids from stdin if necessary.
    # TODO(MatthewJA): Implement --all.
    if all:
        raise NotImplementedError('--all not yet implemented.')

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

    # Additional validation of parameters.
    # If time_span is CUSTOM, start and end should also be specified.
    if config_dict['time_span'] == 'CUSTOM':
        if not config_dict['start_dt'] or not config_dict['end_date']:
            raise click.ClickException('If time-span is CUSTOM then --start and --end must be specified')
    # If start and end are specified, time_span should be CUSTOM
    if config_dict['start_dt'] and config_dict['end_date']:
        if config_dict['time_span'] != 'CUSTOM':
            raise click.ClickException('If --start and --end are specified then --time-span must be CUSTOM')
    # If either start or end are specified then both must be specified
    if config_dict['start_dt'] and not config_dict['end_date']:
        raise click.ClickException('If --start is specified then --end must also be specified')
    if not config_dict['start_dt'] and config_dict['end_date']:
        raise click.ClickException('If --end is specified then --start must also be specified')
    # These comparisons should probably be case-insensitive anyway, but
    # confirm here just to be sure.
    assert config_dict['size'].isupper()

    # Do the import here so that the CLI is fast, because this import is sloooow.
    import dea_waterbodies.waterbody_timeseries_functions as dw_wtf

    # Get the CRS from the shapefile.
    crs = get_crs(config_dict['shape_file'])
    config_dict['crs'] = crs

    # Guess the ID field.
    id_field = guess_id_field(config_dict['shape_file'])
    config_dict['id_field'] = id_field

    # Open the shapefile and get the list of polygons.
    shapes = get_shapes(config_dict, ids, id_field)
    logger.info(f'Found {len(shapes)} polygons for processing, '
                f'out of a possible {len(ids)} (from ids list).')

    # TODO(MatthewJA): Output all the configuration settings in a better way.
    print('config', config_dict)

    # Loop through the polygons and write out a CSV of wet percentage,
    # wet area, and wet pixel count.
    # Attempt each polygon 2 times.
    logger.info('Beginning processing.')
    for i, shape in enumerate(shapes):
        logger.info('Processing {} ({}/{})'.format(
            shape['properties'][id_field],
            i + 1,
            len(shapes)))
        result = dw_wtf.generate_wb_timeseries(shape, config_dict)
        if not result:
            logger.info('Retrying {}'.format(
                shape['properties'][id_field]
            ))
            result = dw_wtf.generate_wb_timeseries(shape, config_dict)
    logger.info('Processing complete.')
    
    return 1


if __name__ == "__main__":
    main()
