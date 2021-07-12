#!/bin/bash

NCHUNKS=4
CONFIG=../ts_configs/config_append_nci.ini
JOBDIR=$PWD

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel
PYTHONPATH=/g/data/r78/dea-waterbodies/code/dea-waterbodies/:$PYTHONPATH

cd $JOBDIR;
parallel --delay 5 --retries 3 --load 100%  --colsep ',' python -m dea_waterbodies.make_time_series ::: $CONFIG,--part,{1..4},--chunks,$NCHUNKS

wait;

aws s3 sync /g/data/r78/dea-waterbodies/timeseries_aus_uid/ s3://dea-public-data/projects/WaterBodies/timeseries/


