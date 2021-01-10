#!/bin/bash
#PBS -P u46
#PBS -q normal
#PBS -N wb_s_missing
#PBS -l walltime=06:00:00
#PBS -l mem=190GB
#PBS -l jobfs=50GB
#PBS -l ncpus=24
#PBS -l wd
#PBS -l storage=gdata/v10+gdata/r78+gdata/fk4
#PBS -M vanessa.newey@ga.gov.au
#PBS -m abe

NCHUNKS=24
CONFIG=../configs/config_small_missing.ini
JOBDIR=$PWD

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel

cd $JOBDIR;
parallel --delay 5 --retries 3 --load 100%  --colsep ',' python ../GetWBTimeHistory.py ::: $CONFIG,{1..24},$NCHUNKS

wait;

