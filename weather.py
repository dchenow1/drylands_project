
import xarray as xr
import numpy as np
import pandas as pd
import h5netcdf
import h5pyd
import glob
import os

gcms = [
    "ACCESS-ESM1-5", 
    "BCC-CSM2-MR",
    "CNRM-ESM2-1", 
    "CanESM5", 
    "EC-Earth3",
    "GFDL-ESM4",
    "GISS-E2-1-G",
    "INM-CM5-0",
    "IPSL-CM6A-LR",
    "MIROC6",
    "MPI-ESM1-2-HR",
    "MRI-ESM2-0",
    "UKESM1-0-LL"
]

gcms_sample = [
    "ACCESS-ESM1-5", 
    "BCC-CSM2-MR",
    "CNRM-ESM2-1", 
    "CanESM5"
    ]

ssps = ['ssp245','ssp585']

site_file = "site_data.txt"
site = {"lon": [], "lat": [], "site_code": []}
with open(site_file, "r") as f:
    next(f)  # Skip the header line
    for line in f:
        parts = line.strip().split('\t')
        lon = float(parts[3])
        lat = float(parts[4])
        site_code = int(parts[5])
        site["lon"].append(lon)
        site["lat"].append(lat)
        site["site_code"].append(site_code)

print(site)

# Convert lon and lat lists to tuples
site["lon"] = tuple(site["lon"])
site["lat"] = tuple(site["lat"])
site["site_code"] = tuple(site["site_code"])

for i in range(len(site)):
    print("site_code=",site["site_code"][i],site["lon"][i],site["lat"][i])
    site_folder_name = f"site_weather_data/site{i+1}"
    if not os.path.exists(site_folder_name):
        os.makedirs(site_folder_name)  # Create site folder if it doesn't exist

    for gcm in gcms:
        path_level1 = '/Volumes/Burke_Lauen/chenoweth/AMES/NEX/GDDP-CMIP6/'+gcm+'/'
        print("gcm=",gcm)

        for ssp in ssps:
            print("ssp=",ssp)
            path_level2 = path_level1 + ssp +'/r1i1p1f1/'
            ppt_path = path_level2 + 'pr/'
            ppt = glob.glob(ppt_path+'*.nc')
            tmin_path = path_level2 + 'tasmin/'
            tmin = glob.glob(tmin_path+'*.nc')
            tmax_path = path_level2 + 'tasmax/'
            tmax = glob.glob(tmax_path+'*.nc')

            if len(ppt) != len(tmin) and len(ppt) != len(tmax): 
                print(f"error: number of files of each type do not match ppt = {len(ppt)}, tmin = {len(tmin)}, tmax = {len(tmax)}")
                exit(1)

            # Check if files matching the pattern already exist
            existing_files = [f for f in os.listdir(site_folder_name) if f"{gcm}_{ssp}" in f]
            if existing_files:
                print(f"Files matching '{gcm}_{ssp}' already exist in '{site_folder_name}'. Skipping...")
                continue  # Skip to the next iteration if files exist
            else:
                
                # Create an empty list to store concatenated data
                concatenated_dfs = {
                    "time_period_mid": pd.DataFrame(),
                    "time_period_late": pd.DataFrame()
                }
                
                for year_file in range(len(ppt)):
                    year = int(ppt[year_file].split("/")[-1].replace(".nc","").split("_")[-1])
                    if year < 2020:
                        continue  # Skip years before 2020

                    tmin_data = xr.open_dataset(tmin[year_file])
                    tmax_data = xr.open_dataset(tmax[year_file])
                    ppt_data = xr.open_dataset(ppt[year_file])
                    ppt_ts = ppt_data.sel(lon=site["lon"][i],lat=site["lat"][i],method='nearest')
                    tmin_ts = tmin_data.sel(lon=site["lon"][i],lat=site["lat"][i],method='nearest')
                    tmax_ts = tmax_data.sel(lon=site["lon"][i],lat=site["lat"][i],method='nearest')
                    
                    print("year=",year)
                    # Concatenate ppt_ts, tmin_ts, and tmax_ts
                    year_data = xr.combine_by_coords([ppt_ts["pr"], tmin_ts["tasmin"], tmax_ts["tasmax"]])
                    # Transform to a dataframe
                    year_df = year_data.to_dataframe()

                    year_df['year'] = year_df.index.get_level_values("time").year
                    year_df['day_of_year'] = year_df.index.get_level_values("time").dayofyear
                    print("year_df=",year_df)                

                    # Append concatenated data to the list
                    if year >= 2015 and year <= 2060:
                        concatenated_dfs["time_period_mid"]=pd.concat([concatenated_dfs["time_period_mid"],year_df])
                    else:
                        concatenated_dfs["time_period_late"]=pd.concat([concatenated_dfs["time_period_late"],year_df])

                for time_period, df in concatenated_dfs.items():
                    csv_filename = f"{site_folder_name}/site{i+1}_{gcm}_{ssp}_{time_period}.csv" #csv_filename = f"{site_name}_{gcm}_{ssp}_{time_period}.csv"
                    concatenated_dfs[time_period].to_csv(csv_filename)
