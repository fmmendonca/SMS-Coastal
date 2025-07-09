# ###########################################################################
#
# File    : m_data_soma.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Nov. 30th, 2023.
#
# Updated : Mar. 19th, 2024.
#
# Descrp. : Converts data from SOMA model to its web interface.
#
# WARNING : Each model has very specific post-processing for thredds and,
#           therefore, this module must be created for each one and inserted
#           manually in m_sim_operations.py. Unfortunately, for this reason,
#           it contains highly sensitive information.
#
# ###########################################################################

from glob import glob
from os import path, makedirs
from json import load
from shutil import rmtree
from typing import Sequence

import xarray as xr

from m_data_sftp import SftpOps
from m_supp_xarray import ncs2Dmean, ncsmean, extractdate


def soma_thredds(ncdirs: Sequence, outdir: str) -> int:
    """Makes the conversion of the data obtained from the SOMA operational
    chain, into the model interface format. The data is sent to the model's
    thredds data server (TDS). The operation is done for each set of netCDFs
    contained in the folders listed in 'ncdirs' and the outputs are saved
    locally in 'outdir' + \\thredds. The interface receives files with 3D
    daily averages and 2D hourly averages.

    WARNING: the function removes old data from 'outdir' + \\thredds .
    
    Keywords arguments:
    - ncdirs: a list of directories containing SOMA netCDF files;
    - outdir: path to a local output directory.
    """

    # Check server inputs:
    print("----- SOMA TDS Module -----".center(78))
    jsonin = ".\\tdsinput.json"

    if not path.isfile(jsonin):
        print(f"[ERROR] m_data_soma.soma_thredds: {jsonin} not found.")
        return 1
    
    with open(jsonin, "rb") as dat:
        inpts = load(dat)
        inpts: dict

    serv = inpts.get("host", "")
    user = inpts.get("user", "")
    pswd = inpts.get("pswd", "")
    rootout = inpts.get("dir", "")
    
    print("HOST :", serv)
    print("USER :", user)
    print("DIR  :", rootout)
    print()

    # Setup output directory:  
    outdir = path.join(outdir, "thredds")
    if path.isdir(outdir): rmtree(outdir)
    makedirs(outdir)

    attrs = {
        "area"        : "South Portugal",
        "contact"     : "https://www.cima.ualg.pt/en/contactos",
        "institution" : "UAlg Centre for Marine and Environmental Research",
        "references"  : "https://soma.ualg.pt",
    }
    
    # Iterate netCDF directories and calculate the averages:
    print("Computing averages...")
    for ncdir in ncdirs:
        print(ncdir)
        for prfx in ("soma_L1", "soma_L2"):
            ncs = sorted(glob(path.join(ncdir, prfx + "*.nc")))
            fout_prfx = path.join(outdir, prfx)

            # 2D average:
            print(" 2D", prfx)
            dset = ncs2Dmean(ncs)
            attrs.update({"title": "SOMA hourly averaged 2D fields forecast"})
            dset.attrs.update(attrs)
            # Save file:
            sfx = extractdate(dset["time"])[0].strftime("_surf_%Y%m%d.nc")
            dset.to_netcdf(fout_prfx + sfx)

            # 3D average:
            print(" 3D", prfx)
            dset = ncsmean(ncs)
            attrs.update({"title": "SOMA daily averaged 3D fields forecast"})
            dset.attrs.update(attrs)
            extractdate(dset["time"])
            # Save file:
            sfx = extractdate(dset["time"])[0].strftime("_%Y%m%d.nc")
            dset.to_netcdf(fout_prfx + sfx)
    
    # Upload file to Thredds server. Thredds parms:
    print("Uploading data to thredds server...")

    sftp = SftpOps()
    status = sftp.open(serv, user, pswd)
    if status > 0: return 1

    # Iterate files:
    ncs = glob(path.join(outdir, "*.nc"))
    inc = 0

    while status == 0 and inc < len(ncs):
        # netCDF and its date:
        ntc = ncs[inc]
        print("", ntc)
        ncdate = xr.open_dataset(ntc, use_cftime=True)["time"].data[0]

        # Output file and directories:
        pdir = rootout + ncdate.strftime("/%Y")  # parent dir
        fdir = pdir + ncdate.strftime("/%m")     # father dir
        fout = fdir + "/" + path.basename(ntc)   # file path

        # Check folders:
        status = sftp.makedir(pdir)
        if status > 0: continue
        status = sftp.makedir(fdir)
        if status > 0: continue

        status = sftp.upfile(ntc, fout)
        inc += 1
    
    sftp.close()
    return status
