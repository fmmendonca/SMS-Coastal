# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2022-01-26
#
# Updated : 2022-02-15
#
# python imports:
#
from multiprocessing import Process
from time import sleep
from copy import deepcopy

# program imports:
from sim_launcher_fct import sim_forecast
from sim_launcher_rst import sim_restart
from forc_launcher import forc_launcher


def sim_manager(inpts):
    """inpts = the dictionary with the treated inputs
       read from init.dat"""
    #
    # get optype and restart day
    #
    optype = inpts.get("optype")
    opdate = inpts.get("opdate")
    rstday = inpts.pop("rstday", 7)
    #
    # update dates ranges for the forcing layer:
    #
    if optype == "3" or (optype == "1" and rstday != opdate.weekday()):
        print("Range of dates updated.")
        inpts.update({"ini": opdate})
    #
    # run forcing layer
    #
    if inpts.pop("skipforc") == "0":
        forc_launcher(deepcopy(inpts))
    #
    # allocate processes:
    #
    pfct = Process(target=sim_forecast, args=(deepcopy(inpts),))
    prst = Process(target=sim_restart, args=(deepcopy(inpts),))
    #
    # initiate restart run:
    #
    if rstday == opdate.weekday() or optype == "4":
        prst.start()
        sleep(30)
    #
    # initiate forecast run:
    #
    if optype in ("1", "3"):
        pfct.start()
    #
    # wait for all processes to complete:
    #
    for pro in (pfct, prst):
        if pro.is_alive():
            pro.join()
    