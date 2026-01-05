#!/usr/bin/env python3
"""
preprocess_ecmwf_clean.py

Clean ECMWF (ERA5-style) preprocessing:
 - loads datasets, optionally extracts pressure-level variables (e.g. r/q/w -> r1000,r850,r500)
 - selects variables listed in config
 - converts units:
     t2m, d2m : K -> C
     u10, v10 : m/s -> km/h (multiplies by 3.6)
 - performs basic safety checks
 - splits by day and saves to processed_data_dir/ecmwf/YYYY/MM/YYYYMMDD.nc
 - configurable via YAML file
"""

import os, sys, yaml, glob
from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr

# ----------------------------
# Load config
# ----------------------------
def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

"""
Returns list of Path objects for year/month directories containing data.
Expects structure base_dir/YYYY/MM/
"""
def find_year_month_folders(base_dir):
    base = Path(base_dir)
    folders = []
    for year_dir in sorted(base.glob("[0-9][0-9][0-9][0-9]")):
        for month_dir in sorted(year_dir.glob("[0-1][0-9]")):
            if month_dir.is_dir():
                folders.append(month_dir)
    return folders

def ensure_valid_time(ds):
    if 'valid_time' in ds.coords:
        return ds
    if 'time' in ds:
       return ds.rename({'time': 'valid_time'})

    # If we get here, try to find a datetime coordinate
    for coord in ds.coords:
        if np.issubdtype(ds[coord].dtype, np.datetime64):
            return ds.rename({coord: 'valid_time'})
        
    # else raise error
    raise ValueError("Dataset has no time coordinate")

"""
    Flatten pressure-level variables into single-level variables.

    Example:
      r(time, pressure, lat, lon) ->
      r1000(time, lat, lon), r850(...), r500(...)
"""

def extract_pressure_levels(ds, level_targets=[1000,850,500]):
    if "pressure_level" not in ds.dims:
        return xr.Dataset()
    
    pressure_vars= [
        v for v in ds.data_vars 
        if 'pressure_level' in ds[v].dims
    ]
    
    if not pressure_vars:
        return xr.Dataset()
    
    pl_vals= ds['pressure_level'].values.astype(int)
    new_vars={}
    
    
    
    for var in pressure_vars:
        for target in level_targets:
            idx= np.argmin(np.abs(pl_vals-target))
            actual= pl_vals[idx]

            if actual!=target:
                print(
                    f"Warning: {var}: requested {target} hPa pressure level"
                    f"using nearest {actual} hPa"
                )

            name= f"{var}{target}"
            new_vars[name]=(
                ds[var]
                .isel(pressure_level=idx)
                .drop_vars("pressure_level",errors='ignore')
                .reset_coords(drop=True)
            )


    return xr.Dataset(new_vars)
  
"""
Convert t2m/d2m from Kelvin->C (if present),
convert u10/v10 m/s -> km/h (if present) and update attributes.
"""
def apply_unit_conversions(ds):
    ds = ds.copy()

    # Temperature in K -> C
    for temp_var in ('t2m', 'd2m'):
        if temp_var in ds.variables:
            ds[temp_var]= ds[temp_var]- 273.15
            ds[temp_var].attrs["units"] = "C"
            ds[temp_var].attrs["GRIB_units"] = "C"
           

    # Wind: m/s -> km/h 
    for wind_var in ('u10', 'v10'):
        if wind_var in ds:
            ds[wind_var]= ds[wind_var] * 3.6
            ds[wind_var].attrs["units"] = "km/h"
            ds[wind_var].attrs["GRIB_units"] = "km/h"

    return ds

"""
Select variables present in var_list from ds.
Warn if requested variable missing.
"""
def select_variables(ds, var_list):
    present = []
    missing = []
    for v in var_list:
        if v in ds:
            present.append(v)
        else:
            missing.append(v)
    if missing:
        print(f"  WARNING: the following requested vars are missing from merged dataset and will be skipped: {missing}")
    if not present:
        raise ValueError("No requested variables are present in dataset after merge.")
    ds_sel = ds[present]
    return ds_sel

"""
Splits an xarray dataset by day and saves each day as a separate NetCDF file
in the directory structure processed_data_dir/YYYY/MM/YYYYMMDD.nc.
Expects dataset to have 'valid_time' coordinate as datetime64.
"""
def store_year_month_day_nc(ds, processed_data_dir, destination_folder):

    base= Path(processed_data_dir) / destination_folder
    base.mkdir(parents=True, exist_ok=True)

    ds= ds.copy()
    ds["valid_time"]= pd.to_datetime(ds["valid_time"].values)
    unique_days = np.unique(ds["valid_time"].dt.date)

    for day in unique_days:
        day_str = day.strftime("%Y-%m-%d")
        day_ds = ds.sel(valid_time=slice(f"{day_str}T00:00", f"{day_str}T23:59"))

        if day_ds["valid_time"].size == 0:
            continue

        year = day.strftime("%Y")
        month = day.strftime("%m")
        date = day.strftime("%d")

        outdir= base/year/month
        outdir.mkdir(parents=True, exist_ok=True)

        outfile = outdir / f"{year}{month}{date}.nc"

        day_ds.to_netcdf(outfile, engine="netcdf4")
        
"""
Converts era5 valid-time from UTC 0 to GMT+8
"""
def convert_valid_time_timezone(ds, h_shift):

    if "valid_time" not in ds.coords:
        return ds

    ds = ds.copy()
    ds["valid_time"] = ds["valid_time"] + np.timedelta64(h_shift, "h")
    return ds

"""
Merges pressure-level, accumulated (t2m, d2m, u10, v10, cloud, sp, cape…), instant (tp, cp, lsp, e…) datasets
Returning 1 single merged dataset with matching coords throughout (valid_time, lat, lon)
"""
def merge_accum_instant_pressure(month_dir):
    month_dir = Path(month_dir)
    files = list(month_dir.glob("*.nc"))

    if len(files) == 0:
        raise FileNotFoundError(f"No .nc files in {month_dir}")

    pressure_file = None
    accum_file = None
    instant_file = None

    # Identify which file is which
    for f in files:
        name = f.name.lower()
        if "pressure" in name:
            pressure_file = f
        elif "accum" in name:
            accum_file = f
        elif "instant" in name:
            instant_file = f

    if not all([pressure_file, accum_file, instant_file]):
        raise RuntimeError(
            f"Missing required ECMWF files in {month_dir}.\n"
            f"Found: {[f.name for f in files]}"
        )

    # Load datasets
    ds_pres = xr.open_dataset(pressure_file, engine="netcdf4")
    ds_acc  = xr.open_dataset(accum_file, engine="netcdf4")
    ds_ins  = xr.open_dataset(instant_file, engine="netcdf4")
    
    #Extract pressure levels first
    pl_ds= extract_pressure_levels(ds_pres)

    # Align time indexes 
    ds_acc, ds_ins, pl_ds = xr.align(ds_acc, ds_ins, pl_ds, join="inner")

    # Merge into one dataset
    ds = xr.merge([ds_acc, ds_ins, pl_ds], compat="override")

    ds_pres.close()
    ds_acc.close()
    ds_ins.close()

    return ds

"""
Main processing per month
folder: Path to YYYY/MM folder
config: loaded config dictionary

"""
def process_month(mnth_folder, config):

    print(f"\n Processing: {mnth_folder}")
    
    ds= merge_accum_instant_pressure(mnth_folder)
    
    ds=apply_unit_conversions(ds)
    
    #Timezone shift
    tz_shift= config.get("timezone_shift_hours",0)
    if tz_shift!=0:
        ds= convert_valid_time_timezone(ds,tz_shift)

    ecmwf_cfg= config["sources"]["ecmwf"]
    ds= select_variables(ds, ecmwf_cfg["vars"])

    store_year_month_day_nc(
        ds,
        processed_data_dir=config["processed_data_dir"],
        destination_folder= config["destination_folder"]
    )

    ds.close()


# ----------------------------
# Main
# ----------------------------
def main():
    if len(sys.argv)<2:
        print("Usage: python ecmwf_clean.py <config_ecmwf_main.yaml>")
        sys.exit(1)

    config_path= sys.argv[1]
    config= load_yaml(config_path)

    if not config.get("preprocess",False):
        print("preprocess: false. Nothing to do")
        return
    
    base_dir= config["untar_ecmwf_dir"]

    month_folders= find_year_month_folders(base_dir)
    if not month_folders:
        print(f"No YYYY/MM folders found under {base_dir}")
        return
    
    print(f"Found {len(month_folders)} month folders")

    for month_dir in month_folders:
        try:
            process_month(month_dir, config)
        except Exception as e:
            print(f"Error processing {month_dir}: {e}")
            continue

    print("\n ECMWF preprocessing complete")
# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    main()