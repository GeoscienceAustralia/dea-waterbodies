
# coding: utf-8

# # Create Surface Water Snapshot - pick time
# 
# This script creates a snapshot of latest water body conditions, appending the dam fill level as an attribute in a shapefile. This code needs to be run manually after new data has been ingested into DEA, and after the .csv files for each water body have been updated.
# 
# **Required inputs** 
# - A shapefile to be appended with the latest data (`Shapefile_to_append`)
# - A folder where all the timeseries `.csv files` are located from which the snapshot will be created (`Timeseries_dir`)
# - The date for analysis needs to be updated below (`DateToExtractStr`)
# 
# **Date** December 2018
# 
# **Author** Claire Krause and Vanessa Newey


import glob
import geopandas as gp
import re
import pandas as pd
import sys
from dateutil.relativedelta import relativedelta
import warnings

import datacube
from datetime import datetime
import os
current_time = datetime.now()
dc = datacube.Datacube()

time_period = ('2020-06-01', current_time.strftime('%m-%d-%Y'))
query = {'time': time_period}
datasets= dc.find_datasets(product='wofs_albers', **query)
dataset_times = [dataset.center_time for dataset in datasets]
dataset_times.sort()
print(f'Latest wofls in datacube is {dataset_times[-1]}')


# ### Choose when you want the snapshot for
# 
# Note that there is a filter built in here that excludes observations that are more than 45 days from the chosen date.

if len(sys.argv) >1:
    DateToExtractStr =  sys.argv[1] #'20190304'
    DateToExtract = pd.to_datetime(DateToExtractStr, format='%Y%m%d', utc=True)
else:
   DateToExtract = dataset_times[-1]
   DateToExtractStr = DateToExtract.strftime('%Y%m%d')
   DateToExtract = pd.to_datetime(DateToExtractStr, format='%Y%m%d', utc=True)

print(f'Appending shapefile with nearest values to {DateToExtractStr}')

# Create filter range which only includes observations of +/- 45 days
DateFilterA = DateToExtract + relativedelta(days=-45)
DateFilterB = DateToExtract + relativedelta(days=45)


# ## Set up the file paths, date to be extracted and output file names
# 
# `Timeseries_dir` is the folder where all the .txt files are located from which the snapshot will be created
# 
# `Shapefile_to_append` is the shapefile which the snapshot will be added to as an attribute



# Timeseries_dir = '/g/data/u46/vmn547/Timeseries_lsa/'

Timeseries_dir = '/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/Timeseries_lsa/'


season_dict={1:'summer', 2:'summer', 3:'autumn', 4:'autumn', 5:'autumn', 6:'winter', 7:'winter', 8:'winter', 9:'spring',
             10:'spring', 11:'spring', 12:'summer'}
season = season_dict[DateToExtract.month]

Shapefile_to_append = '/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots/old_AllNSW_20190321_Snapshot/AllNSW_20190321_Snapshot.shp'

#output_shapefile = f'/g/data/u46/vmn547/AllNSW_Snapshots/LSA_AllNSW_{DateToExtract.year}_{season}_Snapshot.shp'
output_shapefile = f'/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots/LSA_AllNSW_{DateToExtract.year}_{season}_Snapshot.shp'

if os.path.isfile(output_shapefile):
    print('file exists')
    Shapefile_to_append = output_shapefile


SnapshotShp = gp.read_file(Shapefile_to_append)


# ## Get a list of all of the files in the folder to loop through

TimeSeriesFiles = glob.glob(f'{Timeseries_dir}/**/*.csv',recursive=True)


# ## Loop through and extract the relevant date from all the .txt files

for file in TimeSeriesFiles:
    # Get the ID
    NameComponents = re.split('\.|/', file)  # Splitting on '.' and '/'
    PolyID = NameComponents[-2]
    PolyID = int(PolyID)
    #print(PolyID)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            AllObs = pd.read_csv(file, parse_dates=[
                                 'Observation Date'], index_col='Observation Date')
            x = AllObs.iloc[AllObs.index.get_loc(DateToExtract, method='nearest')]
            #print(x.name)
        if(x.name > DateFilterA and x.name < DateFilterB):
            ObsDate = str(x.name)
            Pct = float(x['Wet pixel percentage'])
            lsaPct = float(x['LSA Wet Pixel Pct'])
            FindPolyIndex = SnapshotShp.where(
                SnapshotShp['ID'] == PolyID).dropna(how='all').index.values[0]
            SnapshotShp.loc[(FindPolyIndex, f'{DateToExtractStr}')] = ObsDate
            SnapshotShp.loc[(FindPolyIndex, f'Pc{DateToExtractStr}')] = Pct
            SnapshotShp.loc[(FindPolyIndex, f'ls{DateToExtractStr}')] = lsaPct
            #print(SnapshotShp)
        else:
            print(x.name, f'is out of snapshot range for polyid: {PolyID}')
    except:
        print(f'Bad file {file}')


# ## Write out the appended shapefile

schema = gp.io.file.infer_schema(SnapshotShp)
schema['properties'][f'{DateToExtractStr}'] = 'str'
schema['properties'][f'Pc{DateToExtractStr}'] = 'float'
schema['properties'][f'ls{DateToExtractStr}'] = 'float'
SnapshotShp.to_file(output_shapefile, schema = schema)

Snapshot = gp.read_file(output_shapefile)

from osgeo import ogr

data = Snapshot
# change CRS to epsg 3308
data.to_crs(epsg=3308)
## here it is theoretically possible to use fiona.crs function from_epsg(4326),
## however it doesnt work properly on my Windows PC, so below is proj4 string
#data.crs = '+proj=utm +zone=32 +ellps=GRS80 +units=m +no_defs'
# write shp file
NameComponents = re.split('\.', output_shapefile)
output_shapefile_base = NameComponents[0]
data.to_file(f'{output_shapefile_base}_Lambert.shp')

import zipfile

def compress(file_names):
    print("File Paths:")
    print(file_names)

    # Select the compression mode ZIP_DEFLATED for compression
    # or zipfile.ZIP_STORED to just store the file
    compression = zipfile.ZIP_DEFLATED
    zf_path = f'/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots_zip/LSA_AllNSW_{DateToExtract.year}_{season}_Snapshot{DateToExtractStr}.zip'
    # create the zip file first parameter path/name, second mode
    zf = zipfile.ZipFile(zf_path, mode="w")
    try:
        for file_name in file_names:
            # Add file to the zip file
            # first parameter file to zip, second filename in zip
            zf.write(file_name, file_name, compress_type=compression)

    except FileNotFoundError:
        print("An error occurred")
    finally:
        # Don't forget to close the file!
        zf.close()


file_extentions = ['shp','shx','cpg','dbf','prj']
file_names= [f'{output_shapefile_base}.{x}' for x in file_extentions]
file_names+= [f'{output_shapefile_base}_Lambert.{x}' for x in file_extentions]
compress(file_names)



