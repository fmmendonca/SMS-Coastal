# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2022-01-25
#
# Updated : 2022-02-07
#

from datetime import datetime, timedelta
from glob import glob
from os import path
from shutil import copytree

import post_special
from sim_operations import SimOp
from support_mailing import mailreport
from support_mohid import outmerger, extract_outputs


def sim_forecast(inpts):
    """inpts = a copy of the dictionary with the treated inputs
       read from init.dat
    
       Module responsible to manage and to coordinate all operations
       related to a forecast simulation. The code is encapsulated in
       the function sim_forecast. The basic operations are imported
       from the class methods of the module sim_operations.py"""
    #
    # create an instance of the class SimOp and set up environment:
    #
    manager = SimOp(inpts, "Forecast")
    manager.environment()
    #
    # check forecast range:
    #
    fctrange = inpts.get("forecast")
    if fctrange < 1:
        print("Module " + __name__ + " ERROR: null forecast range")
        manager.logentry(datetime.today().isoformat() + ";ERR04\n")
        body = str(manager.opdate) + " - Null forecast range."
        mailreport(manager.mail, manager.sbj + "ERROR", body, ())
        return
    print("FORECAST RANGE:", fctrange, "day(s).", end="\n\n")
    #
    # check MOHID fin files:
    #
    status = manager.checkfins()
    if not status:
        return
    #
    # check model initial conditions:
    #
    manager.checkinitials(inpts.get("initials"))
    #
    # define simulation dates and time range:
    #
    manager.runini = datetime.fromordinal(manager.opdate.toordinal())
    manager.runfin = datetime.fromordinal(inpts.get("fin").toordinal())
    #
    # check external interpolated forcing data:
    #
    filt = inpts.get("forc")["hdfintp"] > 0
    srcs = inpts.get("forc")[filt]["src"].tolist()
    
    # loop all sources:
    for src in srcs:
        if fctrange < 1:
            continue

        # look for a file if fctrange >= 1:
        endloop = 0

        while fctrange >= 1 and endloop < 1:
            status = manager.checkhdf(src)
            
            # found forcing data (end loop):
            if status:
                endloop = 1
                continue

            # couldn't find data:
            fctrange -= 1
            manager.runfin = manager.runini + timedelta(fctrange)
    #
    # check external time series forcing data:
    #
    filt = inpts.get("forc")["tsconv"] > 0
    srcs = inpts.get("forc")[filt]["src"].tolist()
    
    # loop all sources:
    for src in srcs:
        if fctrange < 1:
            continue

        # look for a file if fctrange >= 1:
        endloop = 0

        while fctrange >= 1 and endloop < 1:
            status = manager.checktsdat(src)
            
            # found forcing data (end loop):
            if status:
                endloop = 1
                continue

            # couldn't find data:
            fctrange -= 1
            manager.runfin = manager.runini + timedelta(fctrange)
        
    if fctrange < 1:
        manager.logentry(datetime.today().isoformat() + ";ERR02\n")
        body = str(manager.opdate) + " - Missing forcing data."
        mailreport(manager.mail, manager.sbj + "ERROR", body, ())
        return
    #
    # check start time:
    #
    startime, diftime = manager.initime(inpts.get("forc")["src"].tolist())
    fctrange += diftime
    print("STARTIME:", startime)
    print("DIFTIME:", diftime)

    if fctrange < 1:
        print("ERROR: forecast range is less than one day.")
        manager.logentry(datetime.today().isoformat() + ";ERR05\n")
        body = str(manager.opdate) + " - Forecast range reduced due to "
        body += "external forcing sources with different start times."
        mailreport(manager.mail, manager.sbj + "ERROR", body, ())
        return

    # change forecast dates ranges to datetime.datetime object:
    manager.runini = manager.runini + timedelta(hours=startime)
    manager.runfin = manager.runfin + timedelta(hours=startime)
    # other simulation parameters:
    manager.runid = 1
    manager.rundt = inpts.get("fctdt")

    # run simulation:
    status = manager.runsim(inpts.get("gmtref"))
    if not status:
        return

    # write in log and send report:
    manager.goodreport(closelog=1)
    
    # copy fins:
    manager.savefins(int(inpts.pop("lastfin")))

    # save outputs:
    resdir = manager.saveres()
    #
    # merge hydrodynamic and water properties output files:
    #
    level = int(manager.modtype)  # loop start
    
    while status and (level <= manager.levels):
        hdfs = glob(resdir + f"\\LV{level:02d}_*_1.hdf5")
        fout = resdir + f"\\LV{level:02d}_Merged.hdf5"
        status = outmerger(hdfs, "1", fout)
        level += 1
    #
    # extract results for database:
    # if status is false in previos cycle, it wont do the next
    #  
    modid = str(path.basename(manager.smsc)).lower() + "_L"
    level = int(manager.modtype)  # loop start
    opdir = "\\Operations\\RESDB"
    manager.definedir(opdir)
    
    while status and (level <= manager.levels):
        # define output directory:
        outdir = opdir + manager.opdate.strftime("\\%y%m%d")
        manager.redefinedir(outdir)
        outdir = manager.root + outdir
        
        # define level in file name:
        lvid = str(level -1) if (manager.modtype == "2") else str(level)
        
        # copy timeseries folder
        tsdir = resdir + f"\\LV{level:02d}_TimeSeries_Run1"
        copytree(tsdir, outdir + "\\TimeSeries_L" + lvid)

        # extract file:
        hdf = resdir + f"\\LV{level:02d}_Merged.hdf5"
        status = extract_outputs(hdf, outdir, prefix=modid + lvid + "_")

        level += 1

    if not status:
        return
    #
    # post operations
    #
    
    #
    # forecast conversion using MOHID tools:
    #

    #
    # forecast conversion using SMS-Coastal tools:
    #

    #
    # run speceial operations:
    #
    special = inpts.pop("SPECIAL", "")

    # PDE conversion:
    if "PDESFTP" in special:
        outdir = manager.root + "\\Operations\\PDE"
        post_special.convertpde(resdir, outdir, manager.opdate)

    # BASIC thredds:
    if "BASIC_THREDD" in special:
        fmtdir = manager.root + "\\Operations\\RESDB"
        fmtdir += manager.opdate.strftime("\\%y%m%d")
        outdir = manager.root + "\\Operations\\Thredds"
        post_special.convertbasic(fmtdir, outdir, manager.opdate)
