import subprocess

import sys



# Function to install packages

def install(package):

    subprocess.check_call([sys.executable, "-m", "pip", "install", package])



# List of required packages

required_packages = [

    "glob2",

    "fsspec",

    "ujson",

    "dask",

    "xarray",

    "pandas",

    "dataretrieval",

    "s3fs",

    "netCDF4",

    "zarr",

    "kerchunk"

]



# Install all required packages

for package in required_packages:

    try:

        __import__(package)

    except ImportError:

        install(package)



print("All required packages are installed.")



#imports
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
from dask.distributed import Client, LocalCluster
import pathlib

print("imports are all successfull")

# Creating a temporary json folder to store the information in netcdf files as jsons files 

pathlib.Path('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/jsons/logan').mkdir(exist_ok=True)
pathlib.Path('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/zarr_results/logan').mkdir(exist_ok=True)
output_wrfhydro="/compute_scratch"

#input year 
year ='2012'
jobid='1723070663hQEyr' 
#year = os.getenv("param_output_year")
#jobid = os.getenv("param_wrfhydro_output_path")

if __name__ == '__main__':
    client = Client()
    client
    chrtout_path = '/anvil/scratch/x-cybergis/compute/1723070663hQEyr/Outputs/CHRTOUT_DOMAIN1/*'
    chrtout_path=glob.glob(chrtout_path)
    files=[]
    for folder_path in chrtout_path:
        files.append(folder_path)
    print(files)

#function to generate json files from the netcdf files

    def gen_json(u):
        with fsspec.open(u) as inf:
            h5chunks = SingleHdf5ToZarr(inf, u, inline_threshold=300)
            with open(f"/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/jsons/logan/{u.split('/')[-1]}.json", 'wb') as outf:
               outf.write(ujson.dumps(h5chunks.translate()).encode())

    dask.compute(*[dask.delayed(gen_json)(u) for u in files])

    json_list =sorted(glob.glob('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/jsons/logan/{0}*.json'.format(year)))
    
    print(json_list)
    mzz = MultiZarrToZarr(json_list,concat_dims='time',inline_threshold=0)
    mzz.translate('/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/jsons/logan/combined.json')
    fs = fsspec.filesystem(
    "reference", 
    fo="/anvil/scratch/x-cybergis/compute/wrfhydro_postprocessing/all_code/jsons/logan/combined.json", 
    skip_instance_cache=True
)
    m = fs.get_mapper("")
    ds = xr.open_dataset(m, engine='zarr').chunk(chunks={"time":6000, "feature_id":67})

    print("Successfully read the information from the combined json and output it as a xarray dataset")
    print(ds)

    ds.to_zarr("/job/result/zarr_files/{0}.zarr".format(year),consolidated=True, mode="w",safe_chunks=False)
    print("succesfully converted to zarr files")

