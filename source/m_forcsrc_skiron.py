# ###########################################################################
#
# File    : m_forcsrc_skiron.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Jul. 13th, 2023.
#
# Updated : May 23rd, 2024.
#
# Descrp. : Coordinates the data processing from Skiron.
#           (https://forecast.uoa.gr/archives/mfstep/index.html)
#           (https://forecast.uoa.gr/en/forecast-maps/skiron)
#
# ###########################################################################

from datetime import datetime, timedelta
from glob import glob
from os import makedirs, path
from shutil import copytree, rmtree
from typing import Sequence

from m_forc_operations import ForcOps
from m_data_sftp import FtpOps
from m_supp_mailing import initmail


def skiron(prms: dict, srcprms: dict) -> None:
    """Coordinates operations with the data from Skiron, the weather
    forecast model from the Atmospheric Modeling and Weather Forecasting
    Group of National and Kapodistrian University of Athens (NKUA).
    
    Keyword arguments:
    - prms: common parameters between all data sources;
    - srprms: specific parameters for NKUA Skiron operations.
    """

    # Setup source operation:
    print("NKUA Skiron Module")
    ops = ForcOps("skiron", prms, srcprms)
    ops.waitdata()
    status = ops.chkroot()
    if status > 0: return

    # Download inputs:
    rootout = path.join(ops.src.get("root"), "download")
    drange = ops.prms.get("drange")
    drange: Sequence[datetime]
    
    ctrldate = drange[0]

    # Open Skiron FTP:
    skironftp = FtpOps()
    status = skironftp.open("ftp.mg.uoa.gr", "mfstep", "!lam")

    if status > 0:
        err = "[ERROR] m_forcsrc_skiron: " + ops.srcid
        err+= " FTP login failed"
        print(err)
        ops.logentry(err + "\n", True)
        initmail(ops.srcid + " ERROR", err)
        return
    
    # Clean download folder:
    print("Downloading NKUA Skiron...")
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
            grbs += sorted(glob(path.join(iptdir, "*.grb")))
            ctrldate += timedelta(1)
            continue

        makedirs(outdir)

        # Folder date in the FTP:
        if ctrldate.date() > datetime.today().date():
            srvdate = datetime.today().date()
        else:
            srvdate = ctrldate.date()

        # Directory in FTP:
        srvdir = srvdate.strftime("/forecasts/Skiron/daily/005X005/%d%m%y")
        
        status = skironftp.chservdir(srvdir)
        if status > 0:
            # Directory is not there.
            continue

        # Loop files:
        pos = 0

        while status == 0 and pos < 24:
            # 24 files per day.
            fpos = (ctrldate.date() - srvdate).days * 24 + pos

            fipt = srvdate.strftime("MFSTEP005_00%d%m%y_")
            fipt+= f"{fpos:03d}.grb"

            fout = path.join(outdir, srvdate.strftime("MFSTEP005_00%y%m%d_"))
            fout+= f"{fpos:03d}.grb"
            print(" ", path.basename(fout))

            status = skironftp.getftpfile(fipt, fout)
            pos += 1

        if status > 0:
            # File download error.
            continue
        
        # Copy folder to database:
        grbs += sorted(glob(path.join(outdir, "*.grb")))

        if ctrldate.date() <= datetime.today().date():
            copytree(outdir, iptdir)

        # Loop control var:
        ctrldate += timedelta(1)

    # Close FTP session:
    skironftp.ftp.quit()

    if status > 0:
        err = "[ERROR] m_forcsrc_skiron: " + ops.srcid
        err+= " download failed"
        print(err)
        ops.logentry(err + "\n", True)
        initmail(ops.srcid + " ERROR", err)
        return

    # Covnersion of grib files to netCDF:
    status = ops.grbtonc(grbs)
    if status > 0: return

    # Conversion to time series:
    # TO BE DONE (check m_forc_operations, maybe the method is already there)

    # Conversion to MOHID HDF5:
    status = ops.mohid_nctohdf5(True)
    if status > 0: return

    # # Interpolation to model:
    status = ops.mohid_convtomodel()
    if status > 0: return

    # Move old downloaded folders to a NAS:
    status = ops.cpnas(path.join(ops.src.get("root"), "database"))
    if status > 0: return

    # Module ended successfully:
    print(ops.srcid.capitalize(), "operation COMPLETED")
    ops.logentry("[COMPLETED]\n", True)
    initmail(ops.srcid + " COMPLETED", "by Fernando's awesome Python code")
