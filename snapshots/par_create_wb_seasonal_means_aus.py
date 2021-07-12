
# coding: utf-8

# # Create a seasonal means Snapshot 
# 
# This script creates a snapshot of seasonal means(wet surface area) for each waterbody.
# 
# **Required inputs** 
# - A config file which specifies the input shapefile, the output shapefile, the directory where the timeseries csvs live.
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


print(f'Appending shapefile with seasonal means')


# ## Set up the file paths, date to be extracted and output file names
# 
# `timeseries_dir` is the folder where all the .txt files are located from which the snapshot will be created
# 
# `shape_file` is the shapefile which the seasonal means will be added to as attributes

def convert_date(long_date):
    season_dict={1:'summer', 2:'summer', 3:'autumn', 4:'autumn', 5:'autumn', 6:'winter', 7:'winter', 8:'winter', 9:'spring',
             10:'spring', 11:'spring', 12:'summer'}
    season = str(long_date.year) + season_dict[long_date.month]
    return season

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
seasonal_means=[]

# ## Loop through and extract the relevant date from all the .txt files

for filename in TimeSeriesFilesSubset:
    # Get the ID
    NameComponents = re.split('\.|/', filename)  # Splitting on '.' and '/'
    polyid = NameComponents[-2]
    try:
        polyid = int(polyid)
    except:
        polyid = polyid
    try:
        df = pd.read_csv(filename, parse_dates=['Observation Date'], 
                     index_col='Observation Date')
    except:
        print(filename)
    
    mean=df.resample('Q-NOV').mean()
#     mean=df.groupby(pd.Grouper(freq='QS-NOV')).mean()
    mean =mean.rename(columns={mean.columns[0]: polyid})
    mean=mean.drop(columns=[mean.columns[1]])
    mean = mean.transpose()
    mean.index.rename(id_field, inplace=True)
#     print(mean)
    seasonal_means.append(mean)

seasonal_means = pd.concat(seasonal_means)
for long_date in seasonal_means.columns:
    seasonal_means = seasonal_means.rename(columns={long_date: convert_date(long_date)})

# Write out the data to csv
current_date = datetime.now().strftime('%Y-%m-%d')
csvFilePath=f'{snapshot_dir}{shape_file}{current_date}.csv'
if os.path.isfile(csvFilePath):
    pd.DataFrame(seasonal_means).to_csv(csvFilePath, mode='a', header=False, index=False, sep=",")
else:
    pd.DataFrame(seasonal_means).to_csv(csvFilePath,  index=False, sep=",")

if part == numChunks-1:
    # wait until other processes finished
    print(f'process {part} is joining the snapshot data to the shapefile')
    import time
    time.sleep(400)
    seasonal_means = csvFilePath

    output_shapefile = f'{snapshot_dir}{snapshot_shapefile}'
    new_data_df = pd.read_csv(seasonal_means)
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
    os.remove(csvFilePath)





