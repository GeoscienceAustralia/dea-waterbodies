#!/bin/bash
#PBS -P u46
#PBS -q express
#PBS -N wb_append
#PBS -l walltime=06:00:00
#PBS -l mem=128GB
#PBS -l jobfs=50GB
#PBS -l ncpus=16
#PBS -l wd
#PBS -l storage=gdata/v10+gdata/r78+gdata/fk4
#PBS -M vanessa.newey@ga.gov.au
#PBS -m abe

NCHUNKS=8
CONFIG=../ts_configs/config_append.ini
JOBDIR=$PWD

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel

cd $JOBDIR;
parallel --delay 5 --retries 3 --load 100%  --colsep ',' python -m dea_waterbodies.make_time_series ::: $CONFIG,{1..8},$NCHUNKS

wait;

#qsub sync_s3_timeseries.sh
#qsub /g/data/r78/vmn547/Dams/Dams_scripts/lsa_recent_water_bodiesNSW.sh

