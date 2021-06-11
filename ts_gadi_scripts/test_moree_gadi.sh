#!/bin/bash

NCHUNKS=4
CONFIG=../ts_configs/config_moree_test_gdata
JOBDIR=$PWD

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel
pip install fsspec 0.8.0

export PYTHONPATH=/g/data/r78/dea-waterbodies/code/dea-waterbodies/:$PYTHONPATH
cd $JOBDIR;
parallel --delay 5 --retries 3 --load 100%  --colsep ',' python -m dea_waterbodies.make_time_series ::: $CONFIG,--part,{1..4},--chunks,$NCHUNKS

wait;



