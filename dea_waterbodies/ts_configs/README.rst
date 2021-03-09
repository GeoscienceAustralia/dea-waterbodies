Digital Earth Australia Waterbodies Config file
################################################

To update the DEA Waterbodies timeseries, you need to run a script called ``GetWBTimeHistory.py``, which uses a config file to set up the run parameters.

The config options are:
 * ``SHAPEFILE``: the file path to the DEA Waterbodies shapefile. 
 * ``OUTPUT_DIR``: the file path to the output directory for the waterbody timeseries. 
 * ``SIZE``: [ ``ALL`` (default) | ``SMALL`` | ``HUGE`` ]. The ``SIZE`` option allows you to process only large or small waterbodies. If you select ``SMALL``, then only waterbodies ``<= 200000`` m\ :sup:`2` \ will be analysed (93% of all the waterbodies). If you select ``LARGE``, then only waterbodies ``> 200000`` m\ :sup:`2` \ will be analysed (7% of all the waterbodies). ``SIZE`` will default to ``ALL`` if no other option is specified. 
 * ``TIME_SPAN``: [ ``ALL`` (default) |  ``APPEND`` | ``CUSTOM`` ]. ``TIME_SPAN`` sets the time range for the waterbody timeseries queries. If you select ``APPEND``, then only times since the latest dates in the waterbody timeseries will be run. ``TIME_SPAN`` will default to ``ALL`` if no other option is specified. If ``TIME_SPAN = CUSTOM``, then ``START_DATE`` and ``END_DATE`` must be set.
 
    * ``START_DATE``: The start date for the waterbody timeseries query.
    * ``END_DATE``: The end date for the waterbody timeseries query.
    
 * ``MISSING_ONLY``: [ ``TRUE`` | ``FALSE`` (default)]. This flag specifies whether you want to only run waterbody polygons that are missing an accompanying timeseries. 
 
    * ``PROCESSED_FILE`` (an optional .txt file): A text file list of the file names that have been already been processed. The code will check whether the file already exists, and if it doesn't it will then run it. The ``PROCESSED_FILE`` file is used to facilitate parallel runs by creating a common check point. If no ``PROCESSED_FILE`` file is provided, the code will create an empty list for this variable.
    
 * ``FILTER_STATE`` (optional): [ ``ACT`` | ``NSW`` | ``NT`` | ``OT`` | ``QLD`` | ``SA`` | ``TAS`` | ``VIC`` | ``WA`` ]. This flag allows you to run the analysis for selected states only.
 * ``UNCERTAINTY``: [ ``TRUE`` | ``FALSE`` (default)]. This flag allows you to include uncertainties in the output timeseries. if you set ``UNCERTAINTY = True`` then you will only filter out timesteps with 100% invalid pixels. You will also record the number invalid pixels per timestep.


Example config to run an append on all timeseries.

 .. code-block:: bash
 
     ; config.ini
     [DEFAULT]
     SHAPEFILE=/g/data/r78/dea-waterbodies/DigitalEarthAustraliaWaterbodies.shp
     OUTPUTDIR=/g/data/r78/dea-waterbodies/Timeseries/
     TIME_SPAN=APPEND
     SIZE=ALL
     MISSING_ONLY=FALSE