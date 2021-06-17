# # Append waterbody time histories - parallel workflow
# 
# Here we have a shapefile containing polygons and there associated timseries that we want to update with the most recent data.
# Each polygon is independant from the other, and so lends itself to simple parallelisation of the workflow.
# 
# This code was parallelised by moving all of the processing into a single function (here called `FindOutHowFullTheDamIs`).
# 
# **Required inputs** Shapefile containing the polygon set of water bodies to be interrogated. and the directory where
# all the timeseries csvs live.
# 
# **Date** August 2018
# 
# **Author** Claire Krause, Jono Mettes, Vanessa Newey



from datacube import Datacube
from datacube.utils import geometry
from datacube.storage import masking
import fiona
import rasterio.features
import csv
import sys
from math import ceil
import os
from datetime import datetime
from dateutil import relativedelta, parser
import numpy

def wofls_fuser(dest, src):
    where_nodata = (src & 1) == 0
    numpy.copyto(dest, src, where=where_nodata)
    return dest

# Set up some file paths
shape_file = '/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW2000to201810pcMinMaxRiverCleanedNoSea.shp'

print(f'Reading in {shape_file}')
numChunks = sys.argv[2]
numChunks = int(numChunks)
print(f'Splitting into {numChunks} chunks')


part = sys.argv[1]
part = int(part)
print(f'Working on chunk {part}')


global output_dir
# output_dir = '/g/data/u46/vmn547/Timeseries_lsa/'
output_dir = '/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/Timeseries_lsa/'

# ## Loop through the polygons and write out a csv of dam capacity
current_time = datetime.now()
# Get the shapefile's crs
with fiona.open(shape_file) as shapes:
    crs = geometry.CRS(shapes.crs_wkt) 
    ShapesList = list(shapes)
    ChunkSize = ceil(len(ShapesList)/numChunks) + 1
    shapessubset = shapes[(part - 1) * ChunkSize: part * ChunkSize]


print(f'The index we will use is {(part - 1) * ChunkSize, part * ChunkSize}')


def FindOutHowFullTheDamIs(shapes, crs):
    """
    This is where the code processing is actually done. This code takes in a polygon, and the
    shapefile's crs and performs a polygon drill into the wofs_albers product. The resulting
    xarray, which contains the water classified pixels for that polygon over every available
    timestep, is used to calculate the percentage of the water body that is wet at each time step.
    The outputs are written to a csv file named using the polygon ID.

    Inputs:
    shapes - polygon to be interrogated
    crs - crs of the shapefile

    Outputs:
    True or False - False if something unexpected happened, so the function can be run again.
    a csv file on disk is appended for every valid polygon.
    """
    dc = Datacube(app='Polygon drill')
    first_geometry = shapes['geometry']
    if 'ID' in shapes['properties'].keys():
        polyName = shapes['properties']['ID']
    else:
        polyName = shapes['properties']['FID']

    strPolyName = str(polyName).zfill(6)
    fpath = os.path.join(output_dir, f'{strPolyName[0:4]}/{strPolyName}.csv')

    # start_date = get_last_date(fpath)
    start_date = '2021-05-01'
    if start_date is None:
        time_period = ('2021-03-01', current_time.strftime('%Y-%m-%d'))
        # print(f'There is no csv for {strPolyName}')
        # return 1
    else:
        time_period = ('2021-03-01', current_time.strftime('%Y-%m-%d'))

        geom = geometry.Geometry(first_geometry, crs=crs)

        ## Set up the query, and load in all of the WOFS layers
        query = {'geopolygon': geom, 'time': time_period}
#         WOFL = dc.load(product='wofs_albers', **query)
        WOFL = dc.load(product='wofs_albers', group_by='solar_day', fuse_func=wofls_fuser,**query)
        if len(WOFL.attrs) == 0:
            print(f'There is no new data for {strPolyName}')
            return 2
        # Make a mask based on the polygon (to remove extra data outside of the polygon)
        mask = rasterio.features.geometry_mask([geom.to_crs(WOFL.geobox.crs) for geoms in [geom]],
                                               out_shape=WOFL.geobox.shape,
                                               transform=WOFL.geobox.affine,
                                               all_touched=False,
                                               invert=True)
        wofl_masked = WOFL.water.where(mask)
        ## Work out how full the dam is at every time step
        DamCapacityPc = []
        DamCapacityCt = []
        LSA_WetPc = []
        DryObserved = []
        InvalidObservations = []
        for ix, times in enumerate(WOFL.time):

            # Grab the data for our timestep
            AllTheBitFlags = wofl_masked.isel(time=ix)

            # Find all the wet/dry pixels for that timestep
            LSA_Wet = AllTheBitFlags.where(AllTheBitFlags == 136).count().item()
            LSA_Dry = AllTheBitFlags.where(AllTheBitFlags == 8).count().item()
            WetPixels = AllTheBitFlags.where(AllTheBitFlags == 128).count().item() + LSA_Wet
            DryPixels = AllTheBitFlags.where(AllTheBitFlags == 0).count().item() + LSA_Dry

            # Apply the mask and count the number of observations
            MaskedAll = AllTheBitFlags.count().item()
            # Turn our counts into percents
            try:
                WaterPercent = WetPixels / MaskedAll * 100
                DryPercent = DryPixels / MaskedAll * 100
                UnknownPercent = (MaskedAll - (WetPixels + DryPixels)) / MaskedAll * 100
                LSA_WetPercent = LSA_Wet / MaskedAll * 100
            except ZeroDivisionError:
                WaterPercent = 0.0
                DryPercent = 0.0
                UnknownPercent = 100.0
                LSA_WetPercent = 0.0
            # Append the percentages to a list for each timestep
            DamCapacityPc.append(WaterPercent)
            InvalidObservations.append(UnknownPercent)
            DryObserved.append(DryPercent)
            DamCapacityCt.append(WetPixels)
            LSA_WetPc.append(LSA_WetPercent)

        ## Filter out timesteps with less than 90% valid observations
        try:
            ValidMask = [i for i, x in enumerate(InvalidObservations) if x < 10]
            if len(ValidMask) > 0:
                ValidObs = WOFL.time[ValidMask].dropna(dim='time')
                ValidCapacityPc = [DamCapacityPc[i] for i in ValidMask]
                ValidCapacityCt = [DamCapacityCt[i] for i in ValidMask]
                ValidLSApc = [LSA_WetPc[i] for i in ValidMask]
                ValidObs = ValidObs.to_dataframe()
                if 'spatial_ref' in ValidObs.columns:
                    ValidObs=ValidObs.drop(columns=['spatial_ref'])

                DateList = ValidObs.to_csv(None, header=False, index=False,
                                                          date_format="%Y-%m-%dT%H:%M:%SZ").split('\n')
                rows = zip(DateList, ValidCapacityPc, ValidCapacityCt, ValidLSApc)

                if DateList:
                    strPolyName = str(polyName).zfill(6)
                    fpath = os.path.join(output_dir, f'{strPolyName[0:4]}/{strPolyName}.csv')
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    with open(fpath, 'w') as f:
                        writer = csv.writer(f)
                        Headings = ['Observation Date', 'Wet pixel percentage',
                                    'Wet pixel count (n = {0})'.format(MaskedAll), 'LSA Wet Pixel Pct']
                        writer.writerow(Headings)
                        for row in rows:
                            writer.writerow(row)
            else:
                print(f'{polyName} has no new good (90percent) valid data')
            return 1
        except:
            print(f'This polygon isn\'t working...: {polyName}')
            return 3


#-----------------------------------------------------------------------#

noNewDataCount =0
#process each polygon. attempt each polygon 3 times
#shapessubset.reverse()
for shapes in shapessubset:
    result = FindOutHowFullTheDamIs(shapes, crs)
    if result == 3:
        result = FindOutHowFullTheDamIs(shapes, crs)
    elif result ==2:
        noNewDataCount+= 1
        # if noNewDataCount >300:
        #     print('Over 300 polygons with no new data')
        #     exit
print(f'No new data count is {noNewDataCount}')

