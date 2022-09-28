# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2022-01-25
#
# Updated : 2022-02-07
#

from datetime import datetime, timedelta
from math import ceil

import numpy as np

from sim_operations import SimOp
from support_mailing import mailreport


def sim_restart(inpts):
    """inpts = a copy of the dictionary with the treated inputs
       read from init.dat
       
       Module responsible to manage and to coordinate all operations
       related to a restart simulation. The code is encapsulated in
       the function sim_restart. The basic operations are imported
       from the class methods of the module sim_operations.py"""
    #
    # create an instance of the class SimOp and set up the enrironment:
    #
    manager = SimOp(inpts, "Restart")
    manager.environment()
    #
    # check restart range:
    #
    rstrange = np.array(inpts.pop("restart"))
    if 0 in tuple(rstrange):
        print("Module " + __name__ + " ERROR: null restart range")
        manager.logentry(datetime.today().isoformat() + ";ERR04")
        body = str(manager.opdate) + " - Null forecast range."
        mailreport(manager.mail, manager.sbj + "ERROR", body, ())
        return        
    #
    # check model initial conditions:
    #
    manager.checkinitials(inpts.pop("initials"))
    #
    # check external forcing dada
    # define forcing data range in datetime.datetime:
    #
    manager.runfin = datetime.fromordinal(manager.opdate.toordinal())
    manager.runini = datetime.fromordinal(inpts.get("ini").toordinal())
    
    # store runini:
    runini = manager.runini

    # interpolated forcing data:
    forc = inpts.pop("forc")
    filt = forc["hdfintp"] > 0
    srcs = forc[filt]["src"].tolist()
    status = 1

    # loop all sources:
    for src in srcs:
        if not status:
            continue

        status = manager.checkhdf(src)

    # time series forcing data:
    filt = forc["tsconv"] > 0
    srcs = forc[filt]["src"].tolist()

    # loop all sources:
    for src in srcs:
        if not status:
            continue

        status = manager.checktsdat(src)

    if not status:
        manager.logentry(datetime.today().isoformat() + ";ERR02\n")
        body = str(manager.opdate) + " - Missing forcing data."
        mailreport(manager.mail, manager.sbj + "ERROR", body, ())
        return
    #
    # check start time:
    # diftime is not used in restart simulation:
    #
    startime, diftime = manager.initime(srcs)
    del diftime
    #
    # run restart stages:
    #
    for stage in range(len(rstrange)):
        if not status:
            continue

        # simulation parameters:
        manager.runid = stage + 1
        manager.rundt = inpts.pop(f"rstdt{stage + 1}")
        
        # ranges:
        inidt = timedelta(int(rstrange[:stage].sum()), hours=startime)
        findt = timedelta(int(rstrange[:stage + 1].sum()), hours=startime)

        # simulation dates and times:
        manager.runini = runini + inidt
        manager.runfin = runini + findt

        # run simulation:
        print(f"Ready for restart STAGE {stage+1}:")
        status = manager.runsim(inpts.get("gmtref"))
        if not status:
            continue
        #
        # send in new process a report email:
        #
        manager.goodreport(sbj=f"Stage {stage + 1} ")

    if not status:
        return
    #
    # in operation 1 check the neeed for an interface run:
    #
    dtday = datetime.fromordinal(manager.opdate.toordinal())
    dtday = ceil((datetime.today() - dtday).total_seconds() / (24 * 3600))
    
    # two-day tolerance:
    if dtday <= 2 and inpts.pop("optype") == "1":
        # simulation parameters:
        manager.runid = stage + 2
        manager.rundt = inpts.pop("fctdt")

        # simulation dates and times:
        manager.runini = manager.runfin
        manager.runfin = manager.runini + timedelta(dtday)

        # run simulation:
        print("Ready for restart INTERFACE stage:")
        status = manager.runsim(inpts.get("gmtref"))
        if not status:
            return

        # send report email:
        manager.goodreport(sbj="Interface ")
    #
    # close log:
    #
    manager.logentry(datetime.today().isoformat() + ";1\n")
    #
    # save results and fins:
    #
    manager.savefins(1)
    manager.saveres()
