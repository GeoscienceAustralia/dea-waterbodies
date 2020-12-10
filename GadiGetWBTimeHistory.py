# # Get the waterbody time histories
# 
# Here we have a shapefile containing polygons we wish to explore. Each polygon is independant from the other, and
# so lends itself to simple parallelisation of the workflow.
#
# The code loops through each polygon in the shapefile and writes out a csv of waterbody percentage area full
# and wet pixel count
# 
# **Required inputs**
# 
# a config file which contains the filename of the Shapefile containing the polygon set of water bodies to be interrogated,
# access to a datacube containing wofls.
#



import sys
from waterbody_timeseries_functions import *

config_file = sys.argv[1]
config_dict = process_config(config_file)

# the part and num_chunks arguments are for when you are running a large job in parallel
if len(sys.argv) > 2:
    part = sys.argv[2]
    part = int(part)
    print(f'Working on chunk {part}')
else:
    part = 1

if len(sys.argv) > 3:
    num_chunks = sys.argv[3]
    num_chunks = int(num_chunks)
    print(f'Splitting into {num_chunks} chunks')
else:
    num_chunks = 1

# not used if using huge mem
if len(sys.argv) > 4:
    config_dict['size'] = sys.argv[4].upper()
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
