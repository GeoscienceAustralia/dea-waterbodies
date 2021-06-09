#!/bin/bash
#PBS -P r78 
#PBS -q copyq 
#PBS -N wb_sync
#PBS -l walltime=04:00:00
#PBS -l mem=200MB
#PBS -l jobfs=1GB
#PBS -l ncpus=1
#PBS -l wd
#PBS -l storage=gdata/v10+gdata/r78
#PBS -M vanessa.newey@ga.gov.au
#PBS -m abe

module use /g/data/v10/public/modules/modulefiles/
module load dea

aws s3 sync /g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots_zip s3://dea-public-data/projects/WaterBodies/NSWsnapshots/