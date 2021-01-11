from datacube import Datacube
from datacube.utils import geometry
import fiona
import rasterio.features
import csv
from math import ceil
import os
from shapely import geometry as shapely_geom
from datetime import datetime, timezone
import configparser
from dateutil import relativedelta, parser
import numpy


def process_config(config_file):
    config = configparser.ConfigParser()
    config_dict = {}
    config.read(config_file)
    start_date = '1986'
    if 'SHAPEFILE' in config['DEFAULT'].keys():
        config_dict['shape_file'] = config['DEFAULT']['SHAPEFILE']

    if 'START_DATE' in config['DEFAULT'].keys():
        config_dict['start_dt'] = config['DEFAULT']['START_DATE']
        print('START_DATE', start_date)

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


def get_shapefile_list(config_dict, part=1, num_chunks=1):
    output_dir = config_dict['output_dir']
    processed_file = config_dict['processed_file']
    # Get the shapefile's crs
    with fiona.open(config_dict['shape_file']) as shapes:
        crs = geometry.CRS(shapes.crs_wkt)
        shapes_list = list(shapes)

    if 'UID' in shapes_list[0]['properties'].keys():
        id_field = 'UID'
    elif 'WB_ID' in shapes_list[0]['properties'].keys():
        id_field = 'WB_ID'
    elif 'FID_1' in shapes_list[0]['properties'].keys():
        id_field = 'FID_1'
    elif 'FID' in shapes_list[0]['properties'].keys():
        id_field = 'FID'
    else:
        id_field = 'ID'

    # not used if using huge mem
    if config_dict['size'] == 'SMALL':
        newlist = []
        for shapes in shapes_list:
            if shapely_geom.shape(shapes['geometry']).envelope.area <= 200000:
                newlist.append(shapes)
        shapes_list = newlist
        print(f'{len(newlist)} small polygons')

    # not used if using huge mem
    if config_dict['size'] == 'HUGE':
        newlist = []
        for shapes in shapes_list:
            if shapely_geom.shape(shapes['geometry']).envelope.area > 200000:
                newlist.append(shapes)
        shapes_list = newlist
        print(f'{len(newlist)} huge polygons')

    print('missing_only', config_dict['missing_only'])
    if config_dict['missing_only']:
        print('missing_only', config_dict['missing_only'])
        if len(processed_file) < 2:
            print('processed_file', processed_file)
            missing_list = []
            for shapes in shapes_list:
                str_poly_name = shapes['properties'][id_field]
                try:
                    fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv')
                except:
                    str_poly_name = str(int(str_poly_name)).zfill(6)
                    fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv')

                if not os.path.exists(fpath):
                    missing_list.append(shapes)
            shapes_list = missing_list
            print(f'{len(missing_list)} missing polygons')

    if len(processed_file) > 1:
        missing_list = []
        files = open(processed_file, 'r').readlines()
        for shapes in shapes_list:
            str_poly_name = shapes['properties'][id_field]
            try:
                fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv\n')
            except:
                str_poly_name = str(int(str_poly_name)).zfill(6)
                fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv\n')
            if not fpath in files:
                missing_list.append(shapes)
        shapes_list = missing_list
        print(f'{len(missing_list)} missing polygons from {processed_file}')

    if 'filter_state' in config_dict.keys():
        shapes_list = [shape for shape in shapes_list if shape['properties']['STATE'] == config_dict['filter_state']]

    chunk_size = ceil(len(shapes_list) / num_chunks) + 1
    shapes_subset = shapes_list[(part - 1) * chunk_size: part * chunk_size]

    print(f'The index we will use is {(part - 1) * chunk_size, part * chunk_size}')
    return shapes_subset, crs, id_field


def get_last_date(fpath, max_days=None):
    try:
        current_time = datetime.now(timezone.utc)
        with open(fpath, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            last_date = last_line.split(',')[0]
            start_date = parser.parse(last_date)
            start_date = start_date + relativedelta.relativedelta(days=1)
            if max_days:
                if (current_time - start_date).days > max_days:
                    start_date = current_time - relativedelta.relativedelta(days=max_days)
            str_start_date = start_date.strftime('%Y-%m-%d')
            return str_start_date
    except:
        return None


def wofls_fuser(dest, src):
    where_nodata = (src & 1) == 0
    numpy.copyto(dest, src, where=where_nodata)
    return dest


# Define a function that does all of the work
def generate_wb_timeseries(shapes, config_dict):
    """
    This is where the code processing is actually done. This code takes in a polygon, and the
    and a config dict which contains: shapefile's crs, output directory, id_field, time_span, and include_uncertainty
    which says whether to include all data as well as an invalid pixel count which can be used for measuring uncertainty
    performs a polygon drill into the wofs_albers product. The resulting
    xarray, which contains the water classified pixels for that polygon over every available
    timestep, is used to calculate the percentage of the water body that is wet at each time step.
    The outputs are written to a csv file named using the polygon UID, which is a geohash of the polygon's centre coords.

    Inputs:
    shapes - polygon to be interrogated
    config_dict - many config settings including crs, id_field, time_span, shapefile

    Outputs:
    Nothing is returned from the function, but a csv file is written out to disk
    """
    output_dir = config_dict['output_dir']
    crs = config_dict['crs']
    id_field = config_dict['id_field']
    time_span = config_dict['time_span']
    include_uncertainty = config_dict['include_uncertainty']

    if include_uncertainty:
        unknown_percent_threshold = 100
    else:
        unknown_percent_threshold = 10

    with Datacube(app='Polygon drill') as dc:
        first_geometry = shapes['geometry']

        str_poly_name = shapes['properties'][id_field]

        try:
            fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv')
        except:
            str_poly_name = str(int(str_poly_name)).zfill(6)
            fpath = os.path.join(output_dir, f'{str_poly_name[0:4]}/{str_poly_name}.csv')
        geom = geometry.Geometry(first_geometry, crs=crs)
        current_year = datetime.now().year

        if time_span == 'ALL':
            if shapely_geom.shape(first_geometry).envelope.area > 2000000:
                years = range(1986, current_year + 1, 5)
                time_periods = [(str(year), str(year + 4)) for year in years]
            else:
                time_periods = [('1986', str(current_year))]
        elif time_span == 'APPEND':
            start_date = get_last_date(fpath)
            if start_date is None:
                print(f'There is no csv for {str_poly_name}')
                return 1
            time_periods = [(start_date, str(current_year))]
        elif time_span == 'CUSTOM':
            time_periods = [(config_dict['start_dt'], config_dict['end_date'])]

        valid_capacity_pc = []
        valid_capacity_ct = []
        invalid_capacity_ct = []
        date_list = []
        for time in time_periods:
            wb_capacity_pc = []
            wb_capacity_ct = []
            wb_invalid_ct = []
            dry_observed = []
            invalid_observations = []

            # Set up the query, and load in all of the WOFS layers
            query = {'geopolygon': geom, 'time': time}
            wofl = dc.load(product='wofs_albers', group_by='solar_day', fuse_func=wofls_fuser, **query)

            if len(wofl.attrs) == 0:
                print(f'There is no new data for {str_poly_name}')
                return 2
            # Make a mask based on the polygon (to remove extra data outside of the polygon)
            mask = rasterio.features.geometry_mask([geom.to_crs(wofl.geobox.crs) for geoms in [geom]],
                                                   out_shape=wofl.geobox.shape,
                                                   transform=wofl.geobox.affine,
                                                   all_touched=False,
                                                   invert=True)
            # mask the data to the shape of the polygon
            # the geometry width and height must both be larger than one pixel to mask.
            if geom.boundingbox.width > 25.3 and geom.boundingbox.height > 25.3:
                wofl_masked = wofl.water.where(mask)
            else:
                wofl_masked = wofl.water

            # Work out how full the waterbody is at every time step
            for ix, times in enumerate(wofl.time):

                # Grab the data for our timestep
                all_the_bit_flags = wofl_masked.isel(time=ix)

                # Find all the wet/dry pixels for that timestep
                lsa_wet = all_the_bit_flags.where(all_the_bit_flags == 136).count().item()
                lsa_dry = all_the_bit_flags.where(all_the_bit_flags == 8).count().item()
                sea_wet = all_the_bit_flags.where(all_the_bit_flags == 132).count().item()
                sea_dry = all_the_bit_flags.where(all_the_bit_flags == 4).count().item()
                sea_lsa_wet = all_the_bit_flags.where(all_the_bit_flags == 140).count().item()
                sea_lsa_dry = all_the_bit_flags.where(all_the_bit_flags == 12).count().item()
                wet_pixels = all_the_bit_flags.where(
                    all_the_bit_flags == 128).count().item() + lsa_wet + sea_wet + sea_lsa_wet
                dry_pixels = all_the_bit_flags.where(
                    all_the_bit_flags == 0).count().item() + lsa_dry + sea_dry + sea_lsa_dry

                # Count the number of masked observations
                masked_all = all_the_bit_flags.count().item()
                # Turn our counts into percents
                try:
                    water_percent = round((wet_pixels / masked_all * 100), 1)
                    dry_percent = round((dry_pixels / masked_all * 100), 1)
                    missing_pixels = masked_all - (wet_pixels + dry_pixels)
                    unknown_percent = missing_pixels / masked_all * 100

                except ZeroDivisionError:
                    water_percent = 0.0
                    dry_percent = 0.0
                    unknown_percent = 100.0
                    missing_pixels = masked_all
                    print(f'{str_poly_name} has divide by zero error')

                # Append the percentages to a list for each timestep
                # Filter out timesteps with < 90% valid observations. Add empty values for timesteps with < 90% valid.
                # if you set 'UNCERTAINTY = True' in your config file then you will only filter out timesteps with
                # 100% invalid pixels.  You will also record the number invalid pixels per timestep.

                if unknown_percent < unknown_percent_threshold:
                    wb_capacity_pc.append(water_percent)
                    invalid_observations.append(unknown_percent)
                    wb_invalid_ct.append(missing_pixels)
                    dry_observed.append(dry_percent)
                    wb_capacity_ct.append(wet_pixels)
                else:
                    wb_capacity_pc.append('')
                    invalid_observations.append('')
                    wb_invalid_ct.append('')
                    dry_observed.append('')
                    wb_capacity_ct.append('')

            valid_obs = wofl.time.dropna(dim='time')
            valid_obs = valid_obs.to_dataframe()
            if 'spatial_ref' in valid_obs.columns:
                valid_obs = valid_obs.drop(columns=['spatial_ref'])
            valid_capacity_pc += wb_capacity_pc
            valid_capacity_ct += wb_capacity_ct
            invalid_capacity_ct += wb_invalid_ct
            date_list += valid_obs.to_csv(None, header=False, index=False,
                                          date_format="%Y-%m-%dT%H:%M:%SZ").split('\n')
            date_list.pop()

        if date_list:
            if include_uncertainty:
                rows = zip(date_list, valid_capacity_pc, valid_capacity_ct, invalid_capacity_ct)
            else:
                rows = zip(date_list, valid_capacity_pc, valid_capacity_ct)
            os.makedirs(os.path.dirname
                        (fpath), exist_ok=True)
            if time_span == 'APPEND':
                with open(fpath, 'a') as f:
                    writer = csv.writer(f)
                    for row in rows:
                        writer.writerow(row)
            else:
                with open(fpath, 'w') as f:
                    writer = csv.writer(f)
                    headings = ['Observation Date', 'Wet pixel percentage',
                                'Wet pixel count (n = {0})'.format(masked_all)]
                    if include_uncertainty:
                        headings.append('Invalid pixel count')
                    writer.writerow(headings)
                    for row in rows:
                        writer.writerow(row)
        else:
            print(f'{str_poly_name} has no new good valid data')
        return True

