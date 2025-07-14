# ###########################################################################
#
# File    : m_forcsrc_namca.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Mar. 2nd, 2023.
#
# Updated : May 23rd, 2024.
#
# Descrp. : Coordinates the data processing from NOMADS NAM 
#           Caribbean/Central America data.
#           (https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/)
#
# ###########################################################################

from datetime import datetime, timedelta
from glob import glob
from os import makedirs, path
from shutil import rmtree, copytree
from typing import Sequence

from m_forc_operations import ForcOps
from m_supp_mailing import initmail
from m_data_basic import convnam


def namca(prms: dict, srcprms: dict) -> None:
    """Coordinates operations with the data from NAM Caribbean/Central,
    a weather forecast model from NOMADS.
    
    Keyword arguments:
    - prms: general Forcing Processor parameters read from 'init.json';
    - srcprms: NAM CA parameters read from 'init.json'.
    """

    # Create operations object
    print("NOMADS NAM Caribbean/Central America Module")
    ops = ForcOps("namca", prms, srcprms)
    ops.waitdata()
    status = ops.chkroot()
    if status > 0: return

    # Download inputs:
    rootout = path.join(ops.src.get("root"), "download")
    drange = ops.prms.get("drange")
    drange: Sequence[datetime]
    
    ctrldate = drange[0]

    # Clean download folder:
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam."
    print("Downloading NOMADS NAM CA...")
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
        srvdir = prfx + srvdate.strftime("%Y%m%d")

        # Loop files:
        pos = 0
        fctdays = (ctrldate.date() - srvdate).days
        maxpos = 12 if fctdays == 3 else 24

        # NAM CA has a forecast of D+3,
        # but that last day only goes until noon.

        while status == 0 and pos < maxpos:
            fpos = fctdays*24 + pos
            fipt = srvdir + f"/nam.t00z.afwaca{fpos:02d}.tm00.grib2"

            fout = path.join(outdir, srvdate.strftime("%Y%m%d."))
            fout+= path.basename(fipt)
            print(" ", path.basename(fout))

            status = ops.webrqst(fipt, fout)
            pos += 3  # 8 files per day.

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
        err = "[ERROR] m_forcsrc_namca: " + ops.srcid
        err+= " download failed"
        print(err)
        # logentry and initmail are not necessar here,
        # as webrqst already runs them.
        return

    # Covnersion of grib files to netCDF:
    status = ops.grbtonc(grbs)
    if status > 0: return

    # Time series conversion:
    fipt = path.join(ops.src.get("root"), "conversion", "namca.nc")
    status = ops.nctots(fipt)
    if status > 0: return

    # Thredds conversion:
    if ops.src.get("tds", False):
        # Conversion set up for the BASIC interface.
        # Input file is the same for time series conversion.
        outdir = path.join(ops.src.get("root"), "thredds")
        convnam(fipt, outdir)

    # Conversion to MOHID HDF5:
    status = ops.mohid_nctohdf5(True)
    if status > 0: return

    # Interpolation to model:
    status = ops.mohid_convtomodel()
    if status > 0: return

    # Move old downloaded folders to a NAS:
    status = ops.cpnas(path.join(ops.src.get("root"), "database"))
    if status > 0: return

    # Module ended successfully:
    print(ops.srcid.upper(), "operation COMPLETED")
    ops.logentry("[COMPLETED]\n", True)
    initmail(ops.srcid + " COMPLETED", "by Fernando's awesome Python code")
