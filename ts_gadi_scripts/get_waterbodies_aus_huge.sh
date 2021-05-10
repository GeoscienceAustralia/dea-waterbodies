#!/bin/bash
#PBS -P u46
#PBS -q hugemem
#PBS -N wb_huge
#PBS -l walltime=06:00:00
#PBS -l mem=310GB
#PBS -l jobfs=50GB
#PBS -l ncpus=10
#PBS -l wd
#PBS -l storage=gdata/v10+gdata/r78+gdata/fk4
#PBS -M vanessa.newey@ga.gov.au
#PBS -m abe

NCHUNKS=10
CONFIG=../ts_configs/config_huge.ini

JOBDIR=$PWD

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel

cd $JOBDIR;
parallel --delay 5 --retries 3 --load 100%  --colsep ',' python -m dea_waterbodies.make_time_series ::: $CONFIG,--part,{1..10},--chunks,$NCHUNKS

wait;

