import os
from glob import glob
import pandas as pd
import xarray as xr

def load_dpird_station_csv(station_csv):
    df= pd.read_csv(station_csv, parse_dates=['time'])
    df= df.sort_values('time')
    return df.set_index('time').to_xarray()


def loop_over_all_dpird (root, excluded_stations):
    station_dict={}
    for month_dir in sorted(glob(os.path.join(root, "*"))):
        for csv_file in glob(os.path.join(month_dir,"*.csv")):
            station= os.path.basename(csv_file).replace(".csv","")

            if station in excluded_stations:
                continue

            ds_mnth= load_dpird_station_csv(csv_file)

            station_dict.setdefault(station,[]).append(ds_mnth)

    station_ds= {
        s: xr.concat(dlist, dim='time')
        for s,dlist in station_dict.items()
    }
    return station_ds

#Add station dimension w/ lon and lat    
def combine_stations(station_ds, meta):
    # Align all stations on the union of time
    all_ds = xr.concat(
        [ds.assign_coords(station=s).expand_dims("station") 
         for s, ds in station_ds.items()],
        dim="station"
    )
    
    # Attach station metadata
    all_ds = all_ds.assign_coords(
        lat=("station", [meta.loc[s, "lat"] for s in all_ds.station.data]),
        lon=("station", [meta.loc[s, "lon"] for s in all_ds.station.data]),
    )
    
    return all_ds

