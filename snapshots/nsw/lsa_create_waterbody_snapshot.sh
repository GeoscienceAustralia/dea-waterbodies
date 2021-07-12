#!/bin/bash
#PBS -P u46
#PBS -q express
#PBS -N nsw_lsa_snapshot
#PBS -l walltime=07:00:00
#PBS -l mem=32GB
#PBS -l jobfs=1GB
#PBS -l ncpus=1
#PBS -l wd
#PBS -l storage=gdata/v10+gdata/r78+gdata/fk4
#PBS -M vanessa.newey@ga.gov.au
#PBS -m abe

module use /g/data/v10/public/modules/modulefiles/
module load dea
python lsa_create_water_snapshot.py

wait;

qsub sync_s3_NSWsnapshots.sh
