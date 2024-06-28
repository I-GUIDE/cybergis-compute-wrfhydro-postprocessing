import os
import glob
os.environ['USE_PYGEOS'] = '0' # to use the default shapely library
import folium
import geopandas as gpd
import pandas as pd
import getData
from shapely.geometry import box, Polygon
import datetime
import s3fs
import boto3
import fsspec
import numpy as np
import xarray as xr
import zarr
import glob
import rasterio
import pyproj
import matplotlib.pyplot as plt
import dask 
from dask.distributed import Client
from dask.distributed import progress


# Before getting started we define paths of the outputs folders and otehr parameters
# start and end times represented as years
StartDate = os.getenv("param_start_year")
EndDate = os.getenv("param_end_year")

# path to NWM snow data
conus_bucket_url = 's3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/ldasout.zarr'

# path to save retrieved snotel data
SNOTEL_OutputFolder = '/job/result/snotel_outputs'

# path to save results from NWM
NWM_OutputFolder = '/job/result/nwm_outputs'

# path to save results form WRF-Hydro
WRFHydro_OutputFolder = '/job/result/wrfhydro_outputs'

# Define the bounding box coordinates
ulong = os.getenv("param_ulong")  # Upper longitude (max longitude)
ulat = os.getenv("param_ulat")   # Upper latitude (max latitude)
llat = os.getenv("param_llat")    # Lower latitude (min latitude)
llong = os.getenv("param_llong")  # Lower longitude (min longitude)



# Calculate the no of sites within spatial domain

# Create geodataframe of all stations
all_stations_gdf = gpd.read_file('https://raw.githubusercontent.com/egagli/snotel_ccss_stations/main/all_stations.geojson').set_index('code')
all_stations_gdf = all_stations_gdf[all_stations_gdf['csvData']==True]
filtered_all_stations_gdf = all_stations_gdf[all_stations_gdf.index.str.contains('_SNTL')]  # only select SNOTEL sites

# Extract the bounding box coordinates of a watershed
# Create a polygon using the bounding box coordinates
bounding_box = Polygon([
    (llong, llat),  # Lower left corner
    (llong, ulat),  # Upper left corner
    (ulong, ulat),  # Upper right corner
    (ulong, llat),  # Lower right corner
    (llong, llat)   # Back to lower left to close the polygon
])





# Use the polygon geometry to select snotel sites that are within the domain
gdf_in_bbox = filtered_all_stations_gdf[filtered_all_stations_gdf.geometry.within(bounding_box)]
print(f'There are {len(gdf_in_bbox)} sites within the domain')

gdf_in_bbox.reset_index(inplace=True)
print("gdf_in_box",gdf_in_bbox)


## Retrieving the data for the selected sites

# Create a folder to save results
isExist = os.path.exists(SNOTEL_OutputFolder)
if isExist == True:
    exit
else:
    os.mkdir(SNOTEL_OutputFolder)

# Use the getData module to retrieve data 
for i in gdf_in_bbox.index:
    temp = gdf_in_bbox["name"][i]
    os.system(f"python3 getData.py '{temp}' {gdf_in_bbox['code'][i].split('_')[0]} {gdf_in_bbox['code'][i].split('_')[1]} {StartDate} {EndDate} {SNOTEL_OutputFolder}")


#### Getting Retrospective SWE Data from the National Water Model (NWM) Version 3

#try:
#    print(client.dashboard_link)
#except:    
#    client = Client(n_workers=24, threads_per_worker=1, memory_limit='2GB') 
#    print(client.dashboard_link)


# Create a folder to save results
isExist = os.path.exists(NWM_OutputFolder)
if isExist == True:
    exit
else:
    os.mkdir(NWM_OutputFolder)

ds = ds = xr.open_zarr(fsspec.get_mapper(conus_bucket_url, anon=True), consolidated=True)
print("Dataset\n",ds)

# Retrieve data for the location of snotel sites
input_crs = 'EPSG:4269'
output_crs = pyproj.CRS(ds.crs.esri_pe_string) 

for i in range(0, gdf_in_bbox.shape[0]): 
    
    snotel_y, snotel_x = getData.convert_latlon_to_yx(gdf_in_bbox.iloc[i].latitude, 
                                                      gdf_in_bbox.iloc[i].longitude, input_crs, ds, output_crs)
    
    ds_subset = ds[['SNEQV']].sel(y=snotel_y, x=snotel_x, method='nearest').sel(time=slice(StartDate, EndDate)).compute()
    
    df = ds_subset.to_dataframe()
    df=df.drop(columns=['x', 'y'])
    df.reset_index(inplace=True)
    df["time"] = pd.to_datetime(df["time"])
    df.rename(columns={df.columns[0]:'Date', df.columns[1]:'NWM_SWE_meters'}, inplace=True)
    df.iloc[:, 1:] = df.iloc[:, 1:].apply(lambda x: pd.to_numeric(x)/1000)  # convert mm to m   

    # convert utc to local time zone
    state_abbr = gdf_in_bbox.iloc[0].code.split("_")[1]   # state abbreviation to identify the time zone
    df_local = getData.convert_utc_to_local(state_abbr, df)   
    
    # groupby the data and select the first item from each group 
    df_local.index = pd.to_datetime(df_local['Date_Local'])
    df_local = df_local.groupby(pd.Grouper(freq='D')).first()

    # save
    df_local.to_csv(f'{NWM_OutputFolder}/df_{gdf_in_bbox.iloc[i].code.split("_")[0]}_{gdf_in_bbox.iloc[i].code.split("_")[1]}_SNTL.csv', index=False)

snotel = pd.read_csv(f'{SNOTEL_OutputFolder}/df_1013_UT_SNTL.csv')
nwm = pd.read_csv(f'{NWM_OutputFolder}/df_1013_UT_SNTL.csv')

# Convert Date columns to datetime
snotel['Date'] = pd.to_datetime(snotel['Date'])
nwm['Date_Local'] = pd.to_datetime(nwm['Date_Local'])

# Plot the data
plt.figure(figsize=(10, 6))

plt.plot(snotel['Date'], snotel['Snow Water Equivalent (m) Start of Day Values'], 
         color='b', label='SNOTEL SWE', linewidth=2, marker='o')
plt.plot(nwm['Date_Local'], nwm['NWM_SWE_meters'], color='purple', 
         linestyle='--', label='NWM SWE', linewidth=2, marker='x')

# Add grid lines
plt.grid(True, which='both', linestyle='--', linewidth=0.5)

# Label the axes
plt.ylabel('Snow Water Equivalent (m)', fontsize=14)

# Set the title
plt.title(f'Snow Water Equivalent Comparison at SNOTEL SIte 1013', fontsize=16)

# Show the legend
plt.legend(fontsize=12)

# Improve the appearance of the plot
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

# Show the plot
plt.tight_layout()
plt.savefig("/job/result/SWE_1013.png")

# Combining all the data

snotel_files = glob.glob(os.path.join(SNOTEL_OutputFolder, '*.csv'))
nwm_files = glob.glob(os.path.join(NWM_OutputFolder, '*.csv'))

# call the function from the getData library
combined_df = getData.combine(snotel_files, nwm_files, StartDate, EndDate)

# save
combined_df.to_csv(f'/job/result/snow_comparison_data.csv', index=False)

# Plot the data
plt.figure(figsize=(10, 6))

combined_df['snotel_1013_swe_m'].plot(color='b', label='SNOTEL SWE', linewidth=2, marker='o')
combined_df['nwm_1013_swe_m'].plot(color='purple', linestyle='--', label='NWM SWE', linewidth=2, marker='x')

# Add grid lines
plt.grid(True, which='both', linestyle='--', linewidth=0.5)

# Label the axes
plt.ylabel('Snow Water Equivalent (m)', fontsize=14)

# Set the title
plt.title(f'Snow Water Equivalent Comparison at SNOTEL SIte 1013', fontsize=16)

# Show the legend
plt.legend(fontsize=12)

# Improve the appearance of the plot
plt.xticks(fontsize=12)
plt.yticks(fontsize=12)

# Show the plot
plt.tight_layout()
plt.savefig("/job/result/SWE_1013_v2.png")

