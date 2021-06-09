#!/bin/bash

module use /g/data/v10/public/modules/modulefiles/
module load dea
module load parallel

parallel --delay 2 --retries 3 --load 100%  --colsep ',' python3 /g/data/r78/vmn547/Dams/Dams_scripts/LSA_RaijinRecentWBTimeHistoryNSW.py ::: {1..4},4
wait;

qsub lsa_create_waterbody_snapshot.sh
