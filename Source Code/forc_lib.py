#
# Created by Fernando Mendonca (fmmendonca@ualg.pt) in 22/04/2022
#
# Last updated in 22/04/2022
#
# Script created as a fix for the downloads methods of SMS-Coastal's
# forc_operations module. It is a library of the download processes
# for a single file of a source. All functions will return 0 if no
# error is found, and 1 otherwise.
#

from datetime import date
from os import path, makedirs, unlink, rename
from shutil import copytree, rmtree
from subprocess import run
from glob import glob

import requests


def getbkup(fdate:date, scdir: str) -> int:
    """Copy the desired day folder from the source backup folder
    to the download folder.

    Keyword arguments:
    fdate -- date of the files inside the backup folder;
    scdir -- path to the source local directory.
    """

    # Return if file date is greater than today:
    if fdate > date.today(): return 0

    # Return if output directory is already in download folder:
    diout = path.join(scdir, fdate.strftime("Download\\%y%m%d"))
    if path.isdir(diout): return 0

    # Copy the output directory if it exists in the backup folder:
    bkup = path.join(scdir, fdate.strftime("BKUP\\%y%m%d"))
    if path.isdir(bkup):
        print("Copying backup folder:", path.basename(bkup))
        copytree(bkup, diout)
        return 0

    # If the needed folder is not found, return error:
    return 1


def webrequest(link: str, fout: str) -> int:
    """Download file from an URL link. Removes the output
    file directory if the download fails.
    
    Keyword arguments:
    link -- URL link;
    fout -- output file name.
    """

    # Download URL:
    print("Downloading from", link)
    response = requests.get(link)
    status = response.status_code

    # Check download success:
    if status != 200:
        print("HTTPError:", status)
        rmtree(path.dirname(fout))
        return 1

    # Output file:
    open(fout, "wb").write(response.content)
    return 0


def amseas(fdate: date, scdir: str, nouts: int) -> int:
    """Download NOMADS NCOM AMEAS data for a single day.
    
    Keyword arguments:
    fdate -- file date;
    scdir -- path to AMSEAS local directory;
    nouts -- number of output .TGZ files, 1 to 4.
    """

    # Output directory:
    diout = path.join(scdir, fdate.strftime("Download\\%y%m%d"))
    if path.isdir(diout): rmtree(diout)
    makedirs(diout)

    # Initiate download, URL prefix:
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/ncom/prod/ncom."
    prfx += fdate.strftime("%Y%m%d/amseas_u_ocn_ncout_grid1_%Y%m%d00_t")
    
    for nout in range(nouts):
        # Define AMSEAS URL:
        if nout < 1:
            url = prfx + f"{nout*24:04d}-{(nout + 1)*24:04d}.tgz"
        else:
            url = prfx + f"{(nout*24) + 1:04d}-{(nout + 1)*24:04d}.tgz"

        # Output file path:
        fout = path.join(diout, f"AMSEAS_{nout}.tgz")
        if webrequest(url, fout) > 0: return 1

        # Extract netCDFs from AMSEAS compressed file and remove it:
        cmd = "tar -xvzf " + fout + " -C " + path.dirname(fout)
        run(cmd, shell=True)
        unlink(fout)
    
    # Rename extracted files:
    ncs = sorted(glob(diout + "\\*.nc"))
    for id, ntc in enumerate(ncs):
        fout = diout + fdate.strftime(f"\\AMSEAS_%y%m%d_{id*3:03d}.nc")
        rename(ntc, fout)
    
    # When downloading only one compressed file, the netCDF
    # which contains D+1 data needs to be removed:
    if nout < 2: unlink(sorted(glob(diout + "\\*.nc"))[-1])
    
    return 0


def gfs(fdate: date, scdir: str, nouts: int):
    
    # Output directory:
    diout = path.join(scdir, fdate.strftime("Download\\%y%m%d"))
    if path.isdir(diout): rmtree(diout)
    makedirs(diout)

    # Initiate download, URL prefix:
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs."
    prfx += fdate.strftime("%Y%m%d/00/atmos/gfs.t00z.sfluxgrbf")
    
    for nout in range(1, nouts + 1):
        # Define GFS URL:
        url = prfx + f"{nout:03d}.grib2"

        # Output file path:
        fout = diout + fdate.strftime(f"\\GFS_%y%m%d_{nout:03d}.grib2")
        if webrequest(url, fout) > 0: return 1
    
    return 0
