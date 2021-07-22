"""Get the waterbody time histories.

Here we have a shapefile containing polygons we wish to explore.
Each polygon is independent from the other, and
so lends itself to simple parallelisation of the workflow.

The code loops through each polygon in the shapefile and writes
out a csv of waterbody percentage area full
and wet pixel count.

**Required inputs**

a config file which contains the filename of the Shapefile
containing the polygon set of water bodies to be interrogated,
access to a datacube containing wofls.

Geoscience Australia - 2021
    Vanessa Newey
    Matthew Alger
"""

import configparser
import logging
from pathlib import Path
import re
import sys

import click

import dea_waterbodies

logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

RE_ID = re.compile(r'[a-z0-9]+$')
RE_IDS_STRING = re.compile(r'(?:[a-z0-9]+,)*[a-z0-9]+$')


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

    if 'WOFLS' in config['DEFAULT'].keys():
        config['WOFLS'] = config['DEFAULT']['WOFLS']
    else:
        config['WOFLS'] = 'wofs_albers'

    return config_dict


def get_crs(shapefile_path):
    from datacube.utils import geometry
    import fiona
    with fiona.open(shapefile_path) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
    return crs


def guess_id_field(shapefile_path) -> str:
    import fiona
    with fiona.open(shapefile_path) as shapes:
        row = next(iter(shapes))
        keys = set(row['properties'].keys())
    possible_guesses = [
        # In order of preference.
        'UID', 'WB_ID', 'FID_1', 'FID', 'ID', 'OBJECTID',
    ]
    possible_guesses += [k.lower() for k in possible_guesses]
    for guess in possible_guesses:
        if guess in keys:
            return guess
    raise ValueError(
        'Couldn\'t find an ID field in {}'.format(keys))


def get_shapes(config_dict: dict,
               wb_ids: [str] or None,
               id_field: str) -> [dict]:
    import fiona
    output_dir = config_dict['output_dir']

    # If missing_only, remove waterbodies that already exist.
    if config_dict['missing_only']:
        logger.info("Filtering waterbodies with existing outputs")
        # NOTE(MatthewJA): I removed references to processed_file here -
        # I think it should be captured later. If this induces a bug,
        # here's a great place to start looking.
        # TODO(MatthewJA): Use Paths earlier on and don't convert here.
        # TODO(MatthewJA): Why doesn't this break with S3 paths?
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
    wb_ids = set(wb_ids or [])  # for quick membership lookups
    config_state = config_dict.get('filter_state')
    with fiona.open(config_dict['shape_file']) as shapes:
        for shape in shapes:
            wb_id = shape['properties'][id_field]
            if wb_ids and wb_id not in wb_ids:
                logger.debug(f'Rejecting {wb_id} (not in wb_ids)')
                continue

            if config_state and shape['properties']['STATE'] != config_state:
                logger.debug(
                    f'Rejecting {wb_id} (not in state {config_state})')
                continue

            logger.debug(f'Accepting {wb_id}')
            filtered_shapes.append(shape)

    return filtered_shapes


@click.command()
@click.argument('ids', required=False, default='')
@click.option('--config', '-c', type=click.Path(), default=None,
              help='Path to a config.ini file. A config file can be provided '
              'instead of using the command line options provided.')
@click.option('--shapefile', type=click.Path(), default=None,
              help='REQUIRED. If --config specifies a shapefile, you do not '
              'need to provide it again here. Path to the waterbody polygon '
              'shapefile (or geojson) you want to run the time series '
              'generation for.')
@click.option('--output', type=click.Path(), default=None,
              help='REQUIRED. If --config specifies an output, you do not '
              'need to provide it again here. Path to the output directory '
              'for the waterbody timeseries.')
@click.option('--time-span', type=click.Choice(['ALL', 'APPEND', 'CUSTOM']),
              default='ALL',
              help='sets the time range for the waterbody '
              'timeseries queries. --time-span will default to ALL if no '
              'other option is specified. If you select APPEND, then only '
              'times since the latest dates in the waterbody timeseries will '
              'be run. If --time-span = CUSTOM, then --start and --start must '
              'also be specified.')
@click.option('--start', type=str, default=None,
              help='Date string. E.g. 2019-01-01. '
              'The start date for the waterbody timeseries query. If --start '
              'is provided --end must also be provided.')
@click.option('--end', type=str, default=None,
              help='Date string. E.g. 2019-12-01. '
              'The end date for the waterbody timeseries query. If --end is '
              'provided --start must also be provided.')
@click.option('--missing-only/--not-missing-only', default=False,
              help='Specifies whether you want to only run '
              'waterbody polygons that DO NOT already have a .csv file '
              'in the --output directory. The default option is to run '
              'every waterbody polygon in the --shapefile file, and overwrite '
              'any existing csv files.')
@click.option('--state', type=click.Choice(
                ['ACT', 'NSW', 'NT', 'OT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']),
              default=None,
              help='Run the analysis for selected '
              'Australian states only. This option requires that each polygon '
              'have an attribute called "STATE" that specifies one of the '
              'specified state options.')
@click.option('--no-mask-obs/--mask-obs', default=False,
              help='Option to include uncertainties in the '
              'output timeseries. if you specify --no-mask-obs then you will '
              'only filter out timesteps with 100% invalid pixels. You will '
              'also record the number invalid pixels per timestep.')
@click.option('--all/--some', default=False,
              help='Option to run a subset of the polygons in '
              'the --shapefile, or --all of them (default). If --some, you '
              'must also provide a list of ids using the ids argument.')
@click.option('--from-queue', default=None,
              help='Name of AWS SQS to read from instead of [ids]')
@click.option('--wofls', default=None,
              help='Name of WOfLs product; default wofs_albers')
@click.option('-v', '--verbose', count=True)
@click.version_option(version=dea_waterbodies.__version__)
def main(ids, config, shapefile, start, end, missing_only,
         time_span, output, state, no_mask_obs, all,
         from_queue, wofls, verbose):
    """
    Make the waterbodies time series. \n
    Args: \n
    ids    List of ids to process if --some flag is used.
    """
    # Set up logging.
    loggers = [logging.getLogger(name)
               for name in logging.root.manager.loggerDict
               if not name.startswith('fiona')
               and not name.startswith('sqlalchemy')
               and not name.startswith('boto')]
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    for logger in loggers:
        if verbose == 0:
            logger.setLevel(logging.WARNING)
        elif verbose == 1:
            logger.setLevel(logging.INFO)
        elif verbose == 2:
            logger.setLevel(logging.DEBUG)
        else:
            raise click.ClickException('Maximum verbosity is -vv')
        logger.addHandler(stdout_hdlr)

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
        # 'size': 'size',  # not currently implemented
        'missing_only': 'missing_only',
        # 'skip': 'processed_file',  # not currently implemented
        'time_span': 'time_span',
        'output': 'output_dir',
        'state': 'filter_state',
        'no_mask_obs': 'include_uncertainty',
        'wofls': 'wofls',
    }
    locals_ = locals()
    for cli_p, config_p in override_param_map.items():
        cli_val = locals_[cli_p]
        if cli_val or config_p not in config_dict:
            config_dict[config_p] = cli_val

    # Additional validation of parameters.
    # If time_span is CUSTOM, start and end should also be specified.
    if config_dict['time_span'] == 'CUSTOM':
        if not config_dict['start_dt'] or not config_dict['end_date']:
            raise click.ClickException(
                'If time-span is CUSTOM then --start and --end '
                'must be specified')
    # If start and end are specified, time_span should be CUSTOM
    if config_dict['start_dt'] and config_dict['end_date']:
        if config_dict['time_span'] != 'CUSTOM':
            raise click.ClickException(
                'If --start and --end are specified then '
                '--time-span must be CUSTOM')
    # If either start or end are specified then both must be specified
    if config_dict['start_dt'] and not config_dict['end_date']:
        raise click.ClickException(
            'If --start is specified then --end must also be specified')
    if not config_dict['start_dt'] and config_dict['end_date']:
        raise click.ClickException(
            'If --end is specified then --start must also be specified')
    # These comparisons should probably be case-insensitive anyway, but
    # confirm here just to be sure.
    #assert config_dict['size'].isupper()  # not currently implemented

    # Process the IDs. If we have some, then read them and split.
    if ids and from_queue:
        raise click.ClickException(
            'If --from-queue then no IDs should be specified')

    if ids:
        ids = ids.split(',')
        if all:
            logger.warning('Ignoring --all since IDs are specified')
    elif (not ids and all) or (not ids and from_queue):
        ids = None  # Handled later in get_shapes
    else:
        assert not ids
        assert not all
        # Read IDs from stdin.
        logger.debug('Reading IDs from stdin')
        ids = []
        for line in sys.stdin:
            line = line.strip()
            if not line:
                break

            if not RE_ID.match(line):
                raise click.ClickException(
                    'Invalid waterbody ID: {}'.format(line))

            ids.append(line)

    if not from_queue:
        logger.debug('Processing IDs: {}'.format(
            repr(ids)
        ))
    else:
        assert not ids
        logger.debug(f'Reading from queue {from_queue}')

    # Do the import here so that the CLI is fast,
    # because this import is sloooow.
    import dea_waterbodies.waterbody_timeseries_functions as dw_wtf

    # Get the CRS from the shapefile.
    crs = get_crs(config_dict['shape_file'])
    config_dict['crs'] = crs

    # Guess the ID field.
    id_field = guess_id_field(config_dict['shape_file'])
    config_dict['id_field'] = id_field
    logger.debug(f'Guessed ID field: {id_field}')

    logger.info('Configuration:')
    for key in sorted(config_dict):
        logger.info(f'\t{key}={config_dict[key]}')

    # IF WE ARE READING FROM A QUEUE:
    # -> loop until nothing is on the queue
    # IF WE ARE NOT READING FROM A QUEUE:
    # -> Use existing IDs

    logger.info(f'Using WOfLs product {config_dict["wofls"]}')
    if not from_queue:
        # Open the shapefile and get the list of polygons.
        shapes = get_shapes(config_dict, ids, id_field)
        logger.info(f'Found {len(shapes)} polygons for processing, '
                    f'out of a possible {len(ids or [])} (from ids list).')

        # Loop through the polygons and write out a CSV of wet percentage,
        # wet area, and wet pixel count.
        # Attempt each polygon 2 times.
        logger.info('Beginning processing.')
        for i, shape in enumerate(shapes):
            logger.info('Processing {} ({}/{})'.format(
                shape['properties'][id_field],
                i + 1,
                len(shapes)))
            result = dw_wtf.generate_wb_timeseries(
                shape, config_dict)
            if not result:
                logger.info('Retrying {}'.format(
                    shape['properties'][id_field]
                ))
                result = dw_wtf.generate_wb_timeseries(
                    shape, config_dict)

    else:
        # From queue
        import boto3

        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=from_queue)

        while True:
            response = queue.receive_messages(
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
            )

            messages = response

            if len(messages) == 0:
                logger.info('No messages received from queue')
                break

            entries = [
                {'Id': msg.message_id,
                 'ReceiptHandle': msg.receipt_handle}
                for msg in messages
            ]

            # Process each ID.
            ids = [e.body for e in messages]
            logger.info(f'Read {ids} from queue')
            shapes = get_shapes(config_dict, ids, id_field)
            logger.info(f'Found {len(shapes)} polygons for processing, '
                        f'out of a possible {len(ids)} (from ids list).')

            # Loop through the polygons and write out a CSV of wet percentage,
            # wet area, and wet pixel count.
            for i, (entry, shape) in enumerate(zip(entries, shapes)):
                id_ = shape['properties'][id_field]
                logger.info('Processing {} ({}/{})'.format(
                    id_,
                    i + 1,
                    len(shapes)))
                result = dw_wtf.generate_wb_timeseries(
                    shape, config_dict)

                # Delete from queue.
                if result:
                    logger.info(f'Successful, deleting {id_}')
                    resp = queue.delete_messages(
                        QueueUrl=from_queue, Entries=[entry],
                    )

                    if len(resp['Successful']) != 1:
                        raise RuntimeError(
                            f"Failed to delete message: {entry}"
                        )

    logger.info('Processing complete.')

    return 0


if __name__ == "__main__":
    main()
