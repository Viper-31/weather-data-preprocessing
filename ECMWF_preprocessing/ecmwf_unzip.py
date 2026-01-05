#!/usr/bin/env python3
import os, zipfile, yaml
from pathlib import Path
from datetime import datetime

"""
Unzip files inplace on data/ecmwf_untar
Deletes the single.nc which is a .zip

"""
def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)
    
def find_year_month_folders(base_dir):
    """
    Returns list of Path objects for year/month directories containing data.
    Expects structure base_dir/YYYY/MM/
    """
    base = Path(base_dir)
    folders = []
    for year_dir in sorted(base.glob("[0-9][0-9][0-9][0-9]")):
        for month_dir in sorted(year_dir.glob("[0-1][0-9]")):
            if month_dir.is_dir():
                folders.append(month_dir)
    return folders

def extract_zip(nc_path, remove_zip=True):
    """
    If nc_path is actually a zip archive (some team saved .nc but they are zip),
    extract into the same folder (or a temp folder) and return list of extracted files.
    If it's not a zip, return [nc_path].
    """
    p = Path(nc_path)
    # If file doesn't exist, return empty
    if not p.exists():
        return []

    # Check if file is a zip
    try:
        if zipfile.is_zipfile(p):
            workdir = p.parent
            with zipfile.ZipFile(p, 'r') as z:
                z.extractall(workdir)

            if remove_zip:
                print(f"Removing original zip: {p.name}")
                p.unlink()
            # gather extracted netcdf files in same folder
            extracted = sorted([str(x) for x in workdir.glob("*.nc")])
            # Prefer files that contain 'instant' or 'accum' or 'pressure'
            return extracted
        
    except zipfile.BadZipFile:
        # Not a zip; keep as netcdf
        print(f"Not a zip, skipping: {p.name}")
        return []

    # If not a zip, return original path
    return [str(p)]

def main():

    config = load_yaml("config_ecmwf_main.yaml")
    base_dir = config["untar_ecmwf_dir"]

    print(f"Scanning base folder", base_dir)
    folders = find_year_month_folders(base_dir)
    
    for folder in folders:
        marker = folder / ".unzipped"
        #Skip already processed folders
        if marker.exists():
            continue
    
        candidates = [f for f in folder.glob('*.nc') if "single" in f.name.lower()]

        if not candidates:
            print("  No 'single' file found → skipping.")
            marker.touch()
            continue

        for file in candidates:
            extracted = extract_zip(file)

            if extracted:
                print(f"    Extracted: {len(extracted)} files")
            else:
                print("    Not a zip → nothing to extract.")
        
        marker.touch()
    print("\n ALL DONE ")

if __name__ == "__main__":
    main()