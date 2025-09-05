# ###########################################################################
#
# File    : m_boundary_gfsflux.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : 2023-03-01
#
# Updated : 2025-09-05
#
# Descrp. : Coordinates the data processing from NOMADS GFS Sflux.
#           (https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/)
#
# ###########################################################################

from datetime import date
from os import makedirs, path
from typing import Tuple

import requests


def webrqst(url: str, fout: str) -> int:
    # DUPLICATED FUNCTION. ORIGINAL IS IN M_BOUNDARY_AMSEAS.PY.
    # CREATE A MODULE WITH GENERIC FUNCTIONS.
    """Downloads from a URL to an output file defined in 'fout'.
    WARNING: Does not check if the path to 'fout' exists.
    
    STDOUT:
    - Status code: 0 for completed and 1 for error.

    Keyword arguments:
    - url: download URL;
    - fout: name and path of the output file.
    """

    # Download file:
    print("Downloading...", url)
    resp = requests.get(url)
    size = resp.headers.get("Content-Length")
    status = resp.status_code

    # Download success code = 200:
    if status != 200 or size == '0': return 1

    with open(fout, "wb") as dat:
        dat.write(resp.content)
        
    # File size should be the same of the header:
    return 1 if str(path.getsize(fout)) != size else 0


def getgfs(outdir: str, runid: str, opdate: date) -> Tuple[str, int]:
    """Downloads one day of GFS Sflux data, which corresponds to 24 GRIB
    files (one per hour). The files are saved in a daily folder, which
    is created in the format 'YYMMDD' inside 'outdir'. The user can choose
    the GFS run in 'runid', which can be one of the following options:
    '00', '06', '12', and '18'.
    
    STDOUT:
    - String with the path of the folder containing the GRIB files.
    Returns an empty string when there's an error;
    - Status code: 0 for completed and 1 for error.
    
    Keyword arguments:
    - outdir: output directory;
    - runid: GFS Sflux run ID;
    - opdate: download process date.
    """

    # Check output directory:
    if not path.isdir(outdir):
        print("[ERROR] m_boundary_gfsflux.getgfs: NotADirectoryError")
        print(f"\tOutput directory '{outdir}' not found.")
        return "", 1
    
    # Check GFS run ID:
    if runid not in ("00", "06", "12", "18"):
        print("[ERROR] m_boundary_gfsflux.getgfs: ValueError")
        print(f"\tInvalid GFS Sflux run ID: '{runid}'.")
        return "", 1

    # Build donwload URL prefix and set sufix:
    dt = (opdate - date.today()).days
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs."
    
    if dt > 0:
        # This is a forecast. GFS server date and file id:
        srvdate = date.today()
        fid = 24*dt
    else:
        srvdate = opdate
        fid = 0

    prfx += srvdate.strftime(f"%Y%m%d/{runid}/atmos/gfs.t{runid}z.")

    # Create output directory:
    outdir = path.join(outdir, opdate.strftime("%y%m%d"))
    makedirs(outdir, exist_ok=True)
    
    # Loop files in one day:
    status = 0
    dth = 0  # hourly dt.

    while dth < 24 and status == 0:
        url = prfx + f"sfluxgrbf{fid+dth:03d}.grib2"

        fout = srvdate.strftime("%Y%m%d.") + path.basename(url)
        fout = path.join(outdir, fout)

        status = webrqst(url, fout)
        dth += 1

    if status > 0:
        print("[ERROR] m_boundary_gfsflux.getgfs: RuntimeError")
        print(f"\tGFS Sflux download failed.")
        return "", 1
    
    return outdir, 0


if __name__ == "__main__":
    from datetime import timedelta

    outdir = r"C:\Users\ferna\Downloads\amseas"
    
    fin = date.today()
    ini = fin-timedelta(6)

    while ini <= fin:
        getgfs(outdir, "00", ini)
        ini += timedelta(1)
