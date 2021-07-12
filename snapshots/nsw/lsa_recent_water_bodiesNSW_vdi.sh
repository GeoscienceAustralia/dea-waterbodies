#!/bin/bash

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel

parallel --delay 2 --retries 3 --load 100%  --colsep ',' python3 LSA_RaijinRecentWBTimeHistoryNSW.py ::: {1..4},4
wait;

python lsa_create_water_snapshot.py

wait;
aws s3 sync /g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots_zip s3://dea-public-data/projects/WaterBodies/NSWsnapshots/

