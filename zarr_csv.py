#!/usr/bin/env python
# imports

import glob
import logging
import fsspec
import ujson
import time
import dask
import dask.delayed
import xarray as xr
import pandas as pd
import dataretrieval.nwis as nwis  # retrive the observed streamflow of USGS gages
import s3fs
from datetime import datetime
import os
import random
import dask.dataframe as dd
from netCDF4 import Dataset
from dask.delayed import delayed
import zarr
from kerchunk.hdf import SingleHdf5ToZarr 
from kerchunk.combine import MultiZarrToZarr
import pathlib

#INPUT THAT IS HARDCODED AND IS OUGHT TO BE CHANGED BASED ON USER PREFERENCES IS THE YEAR AND DATA RANGE, YOU WANT THE CSV FILES TO BE GENERATED FOR

#year='2018'
year = os.getenv("param_output_year")

# Specify date range of interest to create the csv files for the variables of interest
dates=("{0}-01-01".format(year),"{0}-12-31".format(year))
pathlib.Path('/job/result/csv_files/').mkdir(exist_ok=True)
# path of NWM retrospective (NWM-R2.0)

s3_path = 's3://noaa-nwm-retro-v2-zarr-pds'

# Connect to S3
s3 = s3fs.S3FileSystem(anon=True)
store = s3fs.S3Map(root=s3_path, s3=s3, check=False)

#Adding  data from version 3 bundel 
s3_v3_path = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr'

ds_chrtout = xr.open_zarr(fsspec.get_mapper(s3_v3_path,anon=True),consolidated=True)

s3_path_v2_1 = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
ds_chrtout2 = xr.open_zarr(fsspec.get_mapper(s3_path_v2_1,anon=True),consolidated=True)


# load the dataset
start_time = time.time()
print("before reading amazon bucket data", start_time)
ds = xr.open_zarr(store=store, consolidated=True)
end_time = time.time()
elapsed_time = end_time - start_time

# Print the loop execution time
print("time taken to load zarr data from amazon bucket: {:.2f} seconds".format(elapsed_time))

print("done loading Zarr dataset")



#( INCLUDE THIS PART OF THE CODE IF YOU WANT TO GENERATE THE ZARR FILES FROM THE NETCDF FILES )

###########################################
#start_time = time.time()
#json_list =sorted(glob.glob('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/jsons3/2012*.json'))
#mzz = MultiZarrToZarr(json_list,concat_dims='time',inline_threshold=0)
#mzz.translate('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/combined.json')
#fs = fsspec.filesystem(
#    "reference", 
#    fo="/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/combined3.json", 
#    skip_instance_cache=True
#)
#m = fs.get_mapper("")
#reach_ds = xr.open_dataset(m, engine='zarr').chunk(chunks={"time":67, "feature_id":10000})

#print("Successfully read the information from the combined json and output it as a xarray dataset")
#print(reach_ds)

#reach_ds.to_zarr("/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/multizarr/2012.zarr",consolidated=True, mode="w",safe_chunks=False)
#print("succesfully converted to zarr file")
#end_time = time.time()
#elapsed_time = end_time - start_time
#print("time taken to read zarr files to a dataset is {0}".format(elapsed_time))
#print("Done reading chrtout files")
############################################

# Creating a xarray data set from reading the existing zarr files for the specific input year

reach_ds = xr.open_dataset("/job/result/zarr_files/{0}.zarr".format(year)).chunk(chunks={"time":67, "feature_id":15000})

# path of the route link "Rouet_Link.nc"
jobid_cuahsi_subset_domain='17084577537mYZs'
routelink ='/compute_shared/{}/Route_Link.nc'.format(jobid_cuahsi_subset_domain)

# convert rouetlink to dataframe
route_df = xr.open_dataset(routelink).to_dataframe() # convert routelink to dataframe
route_df.gages = route_df.gages.str.decode('utf-8').str.strip()

########################################################################
########################################################################

## Querying USGS gauges and river reaches that exist within our spatial domain (watershed of interest).
usgs_gages = route_df.loc[route_df['gages'] != '']
usgs_ids=usgs_gages.gages  ## USGS gages_ids exit within the watershed of interest

# Initialize an empty dataframe for the purpose of concatenating the dataframes produced during each iteration of the for-loop.
output_df=pd.DataFrame()
start_time1 = time.time()
# Iterate through the existing USGS gages located within the spatial boundary of the watershed of interest.
for gid in usgs_ids:
    try:
        ll=str(list(usgs_gages.loc[usgs_gages.gages == gid].link)) ## corresponding streamlink to USGS ID 
        ll=ll.replace("[","")
        ll=ll.replace("]","")
        timerange = slice(dates[0], dates[1])
        
        flow_data=nwis.get_record(sites=str(gid),service='iv', start=dates[0], end=dates[1], access='3')
        print("flow data",flow_data)
        if flow_data.empty or "00060" not in flow_data.columns:
            continue    
        # Retrieving the Simulated streamflow data
        sim_streamflow=reach_ds.sel(feature_id=int(ll), time=timerange).streamflow.persist()
        sim_streamflow_df=sim_streamflow.to_dataframe()["streamflow"]
        sim_streamflow_df=sim_streamflow_df.to_frame()
	

	
    
        # Retrieve the streamflow data from NWM-R2.0 (zarr format)
    
        R2_streamflow = ds.sel(feature_id=int(ll),
                     time=timerange).streamflow.persist() 
        R2_streamflow_df=R2_streamflow.to_dataframe()["streamflow"]
        R2_streamflow_df =R2_streamflow_df.to_frame()
       
	# Retrieving streamflow simulated data from version 3
        R2_streamflow_V3 = ds_chrtout.sel(feature_id=int(ll),time=timerange).streamflow.persist() 
        R2_streamflow_df_V3=R2_streamflow_V3.to_dataframe()["streamflow"]
        R2_streamflow_df_V3 =R2_streamflow_df_V3.to_frame()

	# Retrieving streamflow simulated data from version 2.1
        R2_streamflow_V2_1 = ds_chrtout2.sel(feature_id=int(ll),time=timerange).streamflow.persist() 
        R2_streamflow_df_V2_1=R2_streamflow_V2_1.to_dataframe()["streamflow"]
        R2_streamflow_df_V2_1 =R2_streamflow_df_V2_1.to_frame()
        
        
        # Retrieve the USGS obs. streamflow
        # get instantaneous values (iv) (measurement each 15 minutes)
        #qobs=nwis.get_record(sites=str(gid), service='iv', start=dates[0], end=dates[1], access='3')
        # print(qobs)
        #qobs=qobs["00060"]/35.3147  ## divided by 35.3147 to convert to m3/sec
        # convert instantaneous streamflow (15 minutes) to hourly value
        # select only the date and hour parts of the index
        #qobs.index = qobs.index.floor('H')
        # calculate the average value for each date and hour
        # qobs_hourly = qobs.groupby(qobs.index).mean()
        # format the datetime index to a new format with only year, month, day, and hour
       #qobs_hourly.index=qobs_hourly.index.strftime('%Y-%m-%d %H:00:00')
       #qobs_hourly.index=pd.to_datetime(qobs_hourly.index)

	# rename the columns of streamflow records obtained from NWM-R2.0 and simulated streamflow data.
        R2_streamflow_df.rename(columns = {'streamflow':'NWM-R2.0_Streamflow_{}'.format(gid)}, inplace=True)
        R2_streamflow_df_V2_1.rename(columns = {'streamflow':'NWM-R2.1_Streamflow_{}'.format(gid)}, inplace=True)
        R2_streamflow_df_V3.rename(columns = {'streamflow':'NWM-R3.0_Streamflow_{}'.format(gid)}, inplace=True)
        sim_streamflow_df.rename(columns = {"streamflow":'Sim_Streamflow_{}'.format(gid)}, inplace=True)
        #qobs_hourly.rename(columns = {'00060':'Obs_Streamflow_{}'.format(gid)}, inplace=True)
        
        #concatenating the data frames
        df_concat2 = pd.concat([sim_streamflow_df,R2_streamflow_df,R2_streamflow_df_V2_1,R2_streamflow_df_V3],axis=1,join="outer")
        print("output df",df_concat2)
        output_df=pd.concat([df_concat2, output_df], axis=1, join="outer")
    except Exception as ex:
        print("an exception has occured",ex)
        continue
        
output_df.index.name = "time"
# save the merged dataframe to a CSV file
output_df.to_csv('/job/result/csv_files/{0}.csv'.format(year), index=True)
end_time1 = time.time()
# Calculate the elapsed time
elapsed_time1 = end_time1 - start_time1

# Print the loop execution time
print("Time take to create csv files for chrtout data: {:.2f} seconds".format(elapsed_time1))

