
# coding: utf-8

# # Create Water Level Snapshot - pick time
# 
# **What this notebook does** This notebook creates a snapshot of latest water body conditions, appending the dam fill level as an attribute in a shapefile. This code needs to be run manually after new data has been ingested into DEA, and after the .txt files for each water bosy have been updated.
# 
# **Required inputs** 
# - A shapefile to be appended with the latest data (`Shapefile_to_append`)
# - A folder where all the `.txt files` are located from which the snapshot will be created (`Timeseries_dir`)
# - The date for analysis needs to be updated below (`DateToExtractStr`)
# 
# **Date** December 2018
# 
# **Author** Claire Krause and Vanessa Newey

import geopandas as gp
import re
import pandas as pd
import numpy as np
import sys
from dateutil.relativedelta import relativedelta
import warnings
import configparser
import glob
from datetime import datetime
import os
from math import ceil

import datacube

config = configparser.ConfigParser()
config_file = sys.argv[1]

config.read(config_file)
if 'SHAPEFILE' in config['DEFAULT'].keys():
    shape_file = config['DEFAULT']['SHAPEFILE']
if 'SNAPSHOT_DIR' in config['DEFAULT'].keys():
    snapshot_dir = config['DEFAULT']['SNAPSHOT_DIR']
if 'SNAPSHOT_SHAPEFILE' in config['DEFAULT'].keys():
    snapshot_shapefile = config['DEFAULT']['SNAPSHOT_SHAPEFILE']
if 'TIMESERIES_DIR' in config['DEFAULT'].keys():
    timeseries_dir = config['DEFAULT']['TIMESERIES_DIR']
if 'DATES' in config['DEFAULT'].keys():
     dates = config['DEFAULT']['DATES'].split(',')
else:
    dates = None

print(dates)

part = sys.argv[2]
part = int(part)
print(f'Working on chunk {part}')

numChunks = sys.argv[3]
numChunks = int(numChunks)
print(f'Splitting into {numChunks} chunks')

# ### Choose when you want the snapshot for
# 
# Note that there is a filter built in here that excludes observations that are more than 45 days from the chosen date.

if len(sys.argv) >4:
    DateToExtractStr =  sys.argv[4] #'20190304'
    dates = [DateToExtractStr]

if dates is None:
    current_time = datetime.now()
    dc = datacube.Datacube()
    start_time = (current_time + relativedelta(days=-45)).strftime('%m-%d-%Y')
    time_period = (start_time, current_time.strftime('%m-%d-%Y'))
    query = {'time': time_period}
    datasets = dc.find_datasets(product='wofs_albers', **query)
    dataset_times = [dataset.center_time for dataset in datasets]
    dataset_times.sort()
    print(f'Latest wofls in datacube is {dataset_times[-1]}')

    DateToExtract = dataset_times[-1]
    DateToExtractStr = DateToExtract.strftime('%Y%m%d')
    dates = [DateToExtractStr]


print(f'Appending shapefile with nearest values to {dates}')


# ## Set up the file paths, date to be extracted and output file names
# 
# `timeseries_dir` is the folder where all the .txt files are located from which the snapshot will be created
# 
# `shape_file` is the shapefile which the snapshot will be added to as an attribute




season_dict={1:'summer', 2:'summer', 3:'autumn', 4:'autumn', 5:'autumn', 6:'winter', 7:'winter', 8:'winter', 9:'spring',
             10:'spring', 11:'spring', 12:'summer'}
# season = season_dict[DateToExtract.month]

# ## Get a list of all of the files in the folder to loop through

TimeSeriesFiles = glob.glob(f'{timeseries_dir}/**/*.csv',recursive=True)
ChunkSize = ceil(len(TimeSeriesFiles)/numChunks) + 1
TimeSeriesFilesSubset = TimeSeriesFiles[(part - 1) * ChunkSize: part * ChunkSize]

SnapshotShp = gp.read_file(shape_file)

if 'UID' in SnapshotShp.columns:
    id_field = 'UID'
elif 'WB_ID' in SnapshotShp.columns:
    id_field = 'WB_ID'
elif 'FID' in SnapshotShp.columns:
    id_field = 'FID'
elif 'FID_1' in SnapshotShp.columns:
    id_field = 'FID_1'
else:
    id_field = 'ID'


SnapshotShp = None

print(f'The index we will use is {(part - 1) * ChunkSize, part * ChunkSize}')
snapshot_data = {}
for DateToExtractStr in dates:
    snapshot_data[id_field] = []
    snapshot_data[f'{DateToExtractStr}'] = []
    snapshot_data[f'Pc{DateToExtractStr}'] = []


# ## Loop through and extract the relevant date from all the .txt files

for file in TimeSeriesFilesSubset:
    # Get the ID
    NameComponents = re.split('\.|/', file)  # Splitting on '.' and '/'
    PolyID = NameComponents[-2]
    try:
        PolyID = int(PolyID)
    except:
        PolyID = PolyID
    snapshot_data[id_field].append(PolyID)

    for DateToExtractStr in dates:
        try:
            DateToExtract = pd.to_datetime(DateToExtractStr, format='%Y%m%d', utc=True)
            # Create filter range which only includes observations of +/- 45 days
            DateFilterA = DateToExtract + relativedelta(days=-45)
            DateFilterB = DateToExtract + relativedelta(days=45)

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                AllObs = pd.read_csv(file, parse_dates=[
                                     'Observation Date'], index_col='Observation Date')
                ValidObs = AllObs.dropna()
                x = ValidObs.iloc[ValidObs.index.get_loc(DateToExtract, method='nearest')]

            if(x.name > DateFilterA and x.name < DateFilterB):
                ObsDate = str(x.name)
                Pct = float(x['Wet pixel percentage'])

                snapshot_data[f'{DateToExtractStr}'].append(ObsDate)
                snapshot_data[f'Pc{DateToExtractStr}'].append(Pct)

            else:
                print(x.name, f'is out of snapshot range for polyid: {PolyID} for date {DateToExtractStr}')
                snapshot_data[f'{DateToExtractStr}'].append(-999)
                snapshot_data[f'Pc{DateToExtractStr}'].append(-999)
        except:
            print(f'Bad {PolyID}')
            snapshot_data[f'{DateToExtractStr}'].append(-999)
            snapshot_data[f'Pc{DateToExtractStr}'].append(-999)



# Write out the data to csv
csvFilePath=f'{snapshot_dir}{snapshot_shapefile}{dates[0]}.csv'

if os.path.isfile(csvFilePath):
    pd.DataFrame(snapshot_data).to_csv(csvFilePath, mode='a', header=False, index=False, sep=",")
else:
    pd.DataFrame(snapshot_data).to_csv(csvFilePath,  index=False, sep=",")

if part == numChunks-1:
    # wait until other processes finished
    print(f'process {part} is joining the snapshot data to the shapefile')
    import time
    time.sleep(500)
    snapshotdata = csvFilePath

    output_shapefile = f'{snapshot_dir}{snapshot_shapefile}'
    new_data_df = pd.read_csv(snapshotdata)
    if os.path.isfile(output_shapefile):
        print('file exists')
        shape_file = output_shapefile

    print(f'Reading in {shape_file}')
    SnapshotShp = gp.read_file(shape_file)
    SnapshotShp = SnapshotShp.merge(new_data_df, on=id_field, how='left')
    schema = gp.io.file.infer_schema(SnapshotShp)
    for DateToExtractStr in dates:
        schema['properties'][f'{DateToExtractStr}'] = 'str'
        schema['properties'][f'Pc{DateToExtractStr}'] = 'float'

    print(f'Write out appended shapefile {output_shapefile}')
    SnapshotShp.to_file(output_shapefile, schema=schema)

    Snapshot = gp.read_file(output_shapefile)




