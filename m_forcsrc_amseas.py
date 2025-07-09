# ###########################################################################
#
# File    : m_forcsrc_amseas.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Mar. 1st, 2023.
#
# Updated : May 23rd, 2024.
#
# Descrp. : Coordinates the data processing from NOMADS NCOM AMSEAS.
#           (https://nomads.ncep.noaa.gov/pub/data/nccf/com/ncom/prod/)
#
# ###########################################################################

from datetime import datetime, timedelta
from glob import glob
from os import makedirs, path, unlink
from shutil import rmtree, copytree
from subprocess import run
from typing import Sequence

from m_forc_operations import ForcOps
from m_supp_mailing import initmail


def amseas(prms: dict, srcprms: dict) -> None:
    """Coordinates operations with the data from NCOM AMSEAS,
    an ocean forecast model from NOMADS.
    
    Keyword arguments:
    - prms: general Forcing Processor parameters read from 'init.json';
    - srcprms: Amseas parameters read from 'init.json'.
    """

    # Create operations object
    print("NOMADS NCOM AMSEAS Module")
    ops = ForcOps("amseas", prms, srcprms)
    ops.waitdata()
    status = ops.chkroot()
    if status > 0: return

    # Download inputs:
    rootout = path.join(ops.src.get("root"), "download")
    drange = ops.prms.get("drange")
    drange: Sequence[datetime]
    
    ctrldate = drange[0]

    # Clean download folder:
    prfx = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/ncom/prod/ncom."
    print("Downloading NCOM AMSEAS...")
    if path.isdir(rootout): rmtree(rootout)
    makedirs(rootout)
    ncs = []

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
            ncs += sorted(glob(path.join(iptdir, "*.nc")))
            ctrldate += timedelta(1)
            continue

        makedirs(outdir)

        # Folder date in the server:
        if ctrldate.date() > datetime.today().date():
            srvdate = datetime.today().date()
        else:
            srvdate = ctrldate.date()

        # Server directory:
        srvdir = prfx + srvdate.strftime("%Y%m%d/amseas_u_ocn_")
        srvdir+= "ncout_grid1_" + srvdate.strftime("%Y%m%d00_t")

        # File position inside the server folder, AMSEAS has four
        # compressed files, one for each forecasted day, every day:
        fpos = (ctrldate.date() - srvdate).days
        
        # Server file sufix:
        if fpos == 0:
            fipt = "0000-0024.tgz"
        else:
            fipt = f"{fpos*24+1:04d}-{(fpos+1)*24:04d}.tgz"
        
        # I/O files and download:
        fipt = srvdir + fipt
        fout = path.join(outdir, path.basename(fipt))
        print(" ", path.basename(fout))
        status = ops.webrqst(fipt, fout)

        if status > 0:
            # File download error.
            continue
        
        # Extract netCDf files and remove compressed file:
        cmd = "tar -xzvf \"" + fout + "\" -C \"" + path.dirname(fout) + "\""
        run(cmd, shell=True)
        unlink(fout)

        ncs += sorted(glob(path.join(outdir, "*.nc")))

        # Copy folder to database:
        # ###################################################################
        # WARNING
        #
        # When using AMSEAS in operational mode, change the '<=' sign to '<'.
        # This way, the file at midnight on D+1, contained in the D+0 folder,
        # will not be deleted and this folder will not go to the database.
        #
        if ctrldate.date() <= datetime.today().date():
            unlink(sorted(glob(path.join(outdir, "*.nc")))[-1])
            copytree(outdir, iptdir)
        #
        # The download process is done this way so it doesn't have to run
        # every day, just once a week. Without this adjustment, data from D+0
        # a week later would be lost, as the server only keeps data up to D-6.
        # After a week D+0 would be D-7.
        # ###################################################################

        # Loop control var:
        ctrldate += timedelta(1)
    
    if status > 0:
        err = "[ERROR] m_forcsrc_amseas: " + ops.srcid
        err+= " download failed"
        print(err)
        # logentry and initmail are not necessar here,
        # as webrqst already runs them.
        return
    
    # All operations should go here, that is,
    # before moving data to an external storage.

    # Move folders in database to an external storage:
    status = ops.cpnas(path.join(ops.src.get("root"), "database"))
    if status > 0: return
    
    # Module ended successfully:
    print(ops.srcid.upper(), "operation COMPLETED")
    ops.logentry("[COMPLETED]\n", True)
    initmail(ops.srcid + " COMPLETED", "by Fernando's awesome Python code")
