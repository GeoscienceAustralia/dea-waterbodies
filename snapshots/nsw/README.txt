The instructions for running a NSW snapshot on VDI.  The location of the script may change if the repo gets tidied up.

Create a NSW snapshot and sync it to s3
1.	Log on to VDI 
2.	Open a terminal
3.	cd /g/data/r78/dea-waterbodies/snapshots/nsw/
4.	./lsa_recent_water_bodiesNSW_vdi.sh or copy paste the contents into a terminal


The last step in this process is the syncing to s3, for which you need aws write credentials.
If you don't have credentials, you can get someone who has to do it.
They can either do 
1.	qsub / g/data/r78/dea-waterbodies/code/dea-waterbodies/snapshots/nsw/sync_s3_NSWsnapshots.sh

2.	aws s3 sync /g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots_zip s3://dea-public-data/projects/WaterBodies/NSWsnapshots/

By the way the above script does:
1.	generates very recent timeseries for the NSW polygons  (/g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/Timeseries_lsa)
2.	creates a snapshot - if there is already a snapshot for that season for that year, it will append columns to that column. 
It also creates a lamberts projection of the same snapshot(shapefile) 
Writes the shapefiles here: /g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots
The snapshot defaults to the date of the latest wofs data in the datacube. But you can specify a different date in the config file.
3.	zips the 2 snapshot shapefiles into : /g/data/r78/cek156/dea-notebooks/Dams/Dams2000to2018/AllNSW_Snapshots_zip
4.	syncs the above folder to https://data.dea.ga.gov.au/?prefix=projects/WaterBodies/NSWsnapshots/
5.	the newly generated zip file will be named by the year, season and snapshot date.
the instructions for running a NSW snapshot on VDI.  The location of the script may change if the repo gets tidied up.

