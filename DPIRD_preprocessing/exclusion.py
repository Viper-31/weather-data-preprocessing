import os
from glob import glob

def compare_csv_to_metadata(path, meta):
    csv_files = glob(os.path.join(path, "*.csv"))
    csv_names = {os.path.basename(f).replace(".csv", "") for f in csv_files}

    excl_names= set(path)
    meta_names = set(meta.index)
    #First check, Stations we care about in SW WA
    interest_stations= csv_names - excl_names

    #Second check stations not in filtered*.csv 
    missing_in_meta = interest_stations - meta_names
    
    return missing_in_meta


