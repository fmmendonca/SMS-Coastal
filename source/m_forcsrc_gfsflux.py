# ###########################################################################
#
# File    : m_forcsrc_gfsflux.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Mar. 1st, 2023.
#
# Updated : May 23rd, 2024.
#
# Descrp. : Coordinates the data processing from NOMADS GFS sflux.
#           (https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/)
#
# ###########################################################################

from datetime import datetime, timedelta
from glob import glob
from os import makedirs, path
from shutil import rmtree, copytree
from typing import Sequence

from m_forc_operations import ForcOps
from m_supp_mailing import initmail


def gfsflux(prms: dict, srcprms: dict) -> None:
    """Coordinates operations with the data from GFS sflux,
    an weather forecast model from the NOMADS.
    
    Keyword arguments:
    - prms: general Forcing Processor parameters read from 'init.json';
    - srcprms: Amseas parameters read from 'init.json'.
    """

    # Create operations object
    print("NOMADS GFS sflux Module")
    ops = ForcOps("gfsflux", prms, srcprms)
    ops.waitdata()
    status = ops.chkroot()
    if status > 0: return

    # Download inputs:
    rootout = path.join(ops.src.get("root"), "download")
    drange = ops.prms.get("drange")
    drange: Sequence[datetime]
    
    ctrldate = drange[0]

    # Clean download folder:
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/"
    print("Downloading GFS sflux...")
    if path.isdir(rootout): rmtree(rootout)
    makedirs(rootout)
    grbs = []

    while status == 0 and ctrldate <= drange[1]:
        # Output directory:
        outdir = path.join(rootout, ctrldate.strftime("%y%m%d"))
        print("", outdir)

        # Copy from database:
        iptdir = path.join(
            ops.src.get("root"), "database",
            ctrldate.strftime("%y%m%d"),
        )

        if path.isdir(iptdir):
            grbs += sorted(glob(path.join(iptdir, "*.grib2")))
            ctrldate += timedelta(1)
            continue

        makedirs(outdir)

        # Folder date in the server:
        if ctrldate.date() > datetime.today().date():
            srvdate = datetime.today().date()
        else:
            srvdate = ctrldate.date()

        # Server directory:
        srvdir = prfx + srvdate.strftime("gfs.%Y%m%d/00/atmos/")

        # Loop files:
        pos = 1

        while status == 0 and pos < 25:
            fpos = (ctrldate.date() - srvdate).days * 24 + pos
            fipt = srvdir + f"gfs.t00z.sfluxgrbf{fpos:03d}.grib2"

            fout = path.join(outdir, srvdate.strftime("%Y%m%d."))
            fout+= path.basename(fipt)
            print(" ", path.basename(fout))

            status = ops.webrqst(fipt, fout)
            pos += 1  # 24 files per day.

        if status > 0:
            # File download error.
            continue

        # Copy folder to database:
        grbs += sorted(glob(path.join(outdir, "*.grib2")))

        if ctrldate.date() <= datetime.today().date():
            copytree(outdir, iptdir)

        # Loop control var:
        ctrldate += timedelta(1)
    
    if status > 0:
        err = "[ERROR] m_forcsrc_gfsflux: " + ops.srcid
        err+= " download failed"
        print(err)
        # logentry and initmail are not necessar here,
        # as webrqst already runs them.
        return


    #
    # All operations should go here, that is,
    # before moving data to an external storage.
    # 


    # Move old downloaded folders to a NAS:
    status = ops.cpnas(path.join(ops.src.get("root"), "database"))
    if status > 0: return
    
    # Module ended successfully:
    print(ops.srcid.upper(), "operation COMPLETED")
    ops.logentry("[COMPLETED]\n", True)
    initmail(ops.srcid + " COMPLETED", "by Fernando's awesome Python code")
