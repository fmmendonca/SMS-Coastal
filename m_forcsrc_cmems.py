# ###########################################################################
#
# File    : m_forcsrc_mercator.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Jul. 9th, 2023.
#
# Updated : Mar. 15th, 2024.
#
# Descrp. : Coordinates the data processing from CMEMS Mercator, service
#           GLOBAL_ANALYSISFORECAST_PHY_001_024-TDS, daily 3D product,
#           sufix ID _P1D-m.
#
# ###########################################################################

from glob import glob
from os import makedirs, path, unlink

import xarray as xr
import copernicusmarine as cmemstool

from m_forc_operations import ForcOps
from m_supp_mailing import initmail
from m_supp_xarray import mergencs


def cmems(prms: dict, srcprms: dict) -> None:
    """Coordinates operations with data from CMEMS Global
    Solution, daily 3D product, sufix ID _P1D-m.
    
    Keyword arguments:
    - prms: common parameters between all data sources;
    - srprms: specific parameters for CMEMS operations.
    """

    # Setup source operation:
    print("CMEMS Global Ocean P1D Module")
    ops = ForcOps("mercator", prms, srcprms)
    ops.waitdata()
    status = ops.chkroot()
    if status > 0: return

    # Check CMEMS credentials:
    creds = ops.src.pop("creds", [])

    if not(isinstance(creds, list) and len(creds) == 2):
        err = "[ERROR] m_forcsrc_cmems: " + ops.srcid
        err+= " invalid/missing credentials"
        print(err)
        ops.logentry(err + "\n", True)
        initmail(ops.srcid + " ERROR", err)
        return

    # Download inputs:
    print("Downloading from CMEMS...")
    grid = ops.prms.get("grid")
    drange = ops.prms.get("drange")
    outdir = path.join(ops.src.get("root"), "download")
    makedirs(outdir, exist_ok=True)

    # Remove old files:
    for ntc in glob(path.join(outdir, "*.nc")): unlink(ntc)

    # Download files:
    dsetids = (
        "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
        "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
        "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
    )
    varids = (["uo", "vo"], ["so",], ["thetao",])  
    
    # Download from CMEMS:
    status = 0
    pos = 0

    while status == 0 and pos < len(dsetids):
        try:
            cmemstool.subset(
                dataset_id            = dsetids[pos],
                variables             = varids[pos],
                username              = creds[0],
                password              = creds[1],
                dataset_version       = "202406",
                minimum_longitude     = grid[2],
                maximum_longitude     = grid[3],
                minimum_latitude      = grid[0],
                maximum_latitude      = grid[1],
                start_datetime        = drange[0].strftime("%Y-%m-%dT00:00:00"),
                end_datetime          = drange[1].strftime("%Y-%m-%dT00:00:00"),
                output_filename       = ops.srcid + f"_{pos+1}.nc",
                output_directory      = outdir,
                #force_download        = True,
                #overwrite_output_data = True,
                disable_progress_bar  = True,
            )
        except Exception as err:
            print(err)
            status = 1
        pos += 1
    
    if status > 0:
        err = "[ERROR] m_forcsrc_cmems: " + ops.srcid
        err+= " download failed"
        print(err)
        ops.logentry(err + "\n", True)
        initmail(ops.srcid + " ERROR", err)
        return

    # Mege downloaded files:
    ncs = glob(path.join(outdir, "*_*.nc"))
    fout = path.join(outdir, ops.srcid + ".nc")
    mergencs(ncs, fout)

    # Files are being overwritten in this code, so when using glob, be
    # extra careful not to get also the output file of a previous run.
    # This is why '*_*.nc' is in the last glob statement.
    
    # Use 'fout' to save data in 'database' folder:
    print("Saving in database folder...")
    outdir = path.join(ops.src.get("root"), "database")
    prfx = path.join(outdir, "cmems-")
    makedirs(outdir, exist_ok=True)

    dset = xr.open_dataset(fout, use_cftime=True)
    data = dset["time"].data

    for pos in range(len(data)):
        dsout = dset.isel(time=slice(pos, pos+1))
        fout = prfx + data[pos].strftime("%Y%m%dT%H%M.nc")
        dsout.to_netcdf(fout)

    dset.close()
    del dset, dsout, data
    
    # Conversion to time series:
    # TO BE DONE (check m_forc_operations, maybe the method is already there)

    # Conversion to MOHID HDF5:
    status = ops.mohid_nctohdf5(False)
    if status > 0: return

    # Interpolation to model:
    status = ops.mohid_convtomodel()
    if status > 0: return
    
    # Module ended successfully:
    print(ops.srcid, "operation COMPLETED")
    ops.logentry("[COMPLETED]\n", True)
    initmail(ops.srcid + " COMPLETED", "by Fernando's awesome Python code")
