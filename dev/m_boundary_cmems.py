from datetime import date, timedelta
from glob import glob
from os import makedirs, path
from shutil import rmtree
from typing import Sequence

import copernicusmarine as cmemstool
import pandas as pd
import xarray as xr

from m_supp_management import SmscManager
import pdb

def cmems(prms: dict) -> None:
    # Initialize operation:
    # 
    smsc = SmscManager(prms)
    smsc.initialize_operation()
    if smsc.status > 0: return
    smsc.forcgrid()
    if smsc.status > 0: return
    del prms

    # Check database files:
    #
    row = 0
    ctrl = smsc.prms.get("INI")[0]
    dbfiles = pd.DataFrame({
        "DATE": [ctrl,], "FILE": ["dummy",], "EXIST": [False,],
    })

    while ctrl <= smsc.prms.get("FIN")[-1]:
        # File in database folder:
        fout = path.join(smsc.rootdir, "database")
        fout+= ctrl.strftime("/%Y/%m/cmems-%Y%m%d.nc")

        # Add to dataframe:
        dbfiles.loc[row, "DATE"] = ctrl
        dbfiles.loc[row, "FILE"] = fout
        dbfiles.loc[row, "EXIST"] = path.isfile(fout)

        ctrl += timedelta(1)
        row += 1
    
    # Create output directory and merge dabase file in it:
    #
    dbfile = path.join(smsc.rootdir, "download")
    if path.isdir(dbfile): rmtree(dbfile)
    makedirs(dbfile)

    dbfile += "/mercator_database.nc"
    
    if dbfiles[dbfiles["EXIST"]==True]["FILE"].to_list():
        print("merge files with xarray support to", dbfile)

    # Check download dates:
    #
    dlfile = path.join(smsc.rootdir, "download/mercator_download.nc")
    dates = dbfiles[dbfiles["EXIST"]==False]["DATE"].to_list()
    status = 0
    
    # Run download:
    if dates:
        smsc.forccred()
        if smsc.status > 0: return

        print("Run download from", dates[0], "to", dates[-1])
        print("Download to root/download/mercator_download.nc")
        print("CREDENTIALS", smsc.prms.get("CREDENTIALS"))

    if status > 0:
        print("[ERROR]")
        return
    
    # Merge files:
    #
    fout = path.join(smsc.rootdir, "download/mercator.nc")
    
    if path.isfile(dbfile) and path.isfile(dlfile):
        print("merging files insto", fout)
    elif path.isfile(dbfile) and not path.isfile(dlfile):
        print("copying DBFILE to", fout)
    elif not path.isfile(dbfile) and path.isfile(dlfile):
        print("copying DLFILE to", fout)
    else:
        print("[ERROR] missing files")
        return
    

def cmems_download(
        fout:str, user: str, password: str, ini: date,
        fin: date, grid: Sequence) -> int:
    # CMEMS produc variables:
    #
    dsetids = (
        "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
        "cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m",
        "cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m",
    )
    varids = (["uo", "vo"], ["so",], ["thetao",])  
    
    # Download from CMEMS:
    #
    outdir = path.dirname(fout)
    status = 0
    pos = 0

    while status == 0 and pos < len(dsetids):
        try:
            cmemstool.subset(
                dataset_id            = dsetids[pos],
                variables             = varids[pos],
                username              = user,
                password              = password,
                dataset_version       = "202406",
                minimum_longitude     = grid[2],
                maximum_longitude     = grid[3],
                minimum_latitude      = grid[0],
                maximum_latitude      = grid[1],
                start_datetime        = ini.strftime("%Y-%m-%dT00:00:00"),
                end_datetime          = fin.strftime("%Y-%m-%dT00:00:00"),
                output_filename       = f"mercator_{pos+1}.nc",
                output_directory      = outdir,
                #force_download        = True,
                #overwrite_output_data = True,
                disable_progress_bar  = True,
            )
        except Exception as err:
            print(err)
            status = 1
        pos += 1
    
    if status > 0: return

    # Mege downloaded files:
    #
    ncs = glob(path.join(outdir, "mercator_0*.nc"))
    # mergencs(ncs, fout)
    
    # Extract time-steps to database folder:
    #
    outdir = path.dirname(outdir) + "/database"

    dset = xr.open_dataset(fout, use_cftime=True)
    data = dset["time"].data

    for pos in range(len(data)):
        dsout = dset.isel(time=slice(pos, pos+1))

        # TENHO QUE FAZER UM TESTE DE DATA AQUI.
        # SE data[pos] for menor ou igual a hoje, salva no databse
        # CASO CONTRARIO PULA O CICLO

        # QUAL É O TIPO DE DATA (DEVE SER NP.DATETIME64[ns]) E COMO EU POSSO
        # COMPARAR: if data[pos] <= date.today().isoformat()

        fout = outdir + data[pos].strftime("/%Y/%m/cmems-%Y%m%dT%H%M.nc")
        makedirs(path.dirname(fout), exist_ok=True)
        dsout.to_netcdf(fout)

    dset.close()
    return 0
    

if __name__ == "__main__":
    kargs = {
        "OPDATE": "2025-01-01",
        "DTDAYS": 30,
        "ROOTDIR": "./cmems",
        "GRID": [35.5, 40, -12, -5],
        
        "CREDENTIALS": ["user", "password"],
    }
    cmems(kargs)
