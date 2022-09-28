# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-10-04
#
# Updated : 2022-02-28
#

from datetime import datetime
from multiprocessing.dummy import freeze_support
from subprocess import run
from time import time

# program imports:
from init_manager import init_manager
from forc_launcher import forc_launcher
from sim_manager import sim_manager
#
# Main program:
#
def main():
    print("<<>>"*19)
    print("SMS-COASTAL".center(76))
    print("<<>>"*19)
    print("\nSimulation Management System for Coastal Operational Models")
    print("\nUniversity of Algarve (UAlg)")
    print("Marine and Environmental Research Center (CIMA)")
    print("Laboratory of Hydrodynamics Numerical Modelling (HIDROTEC)")
    print("\nhttps://www.cima.ualg.pt/en/")
    print("hidrotec@ualg.pt")
    print("\nMOHID COUPLED VERSION v2.2.0")
    print("Last updated in 28 Feb 2022.\n")
    print("<<>>"*19)
    #
    # set timer and read .\init.dat:
    #
    start = time()    
    inpts = init_manager()
    optype = inpts.get("optype")
    #
    # available operations modules:
    #
    mode = {"1": "Full Simulation Cycle Managment",
            "2": "Forcing Managment",
            "3": "Forecast Managment",
            "4": "Restart Managment"}

    print("\nOPERATION TYPE:", mode.get(optype))
    print("OPERATION DATE:", inpts.get("opdate"))
    print("\n" + "<<>>"*19)
    #
    # run modules:
    #
    if optype in ("1", "3", "4"):
        sim_manager(inpts)
    elif optype == "2":
        forc_launcher(inpts)
    #
    # end program:
    #
    print("\nSYSTEM TIME:", datetime.today())
    print("ELAPSED TIME:", round((time()-start)/60, 2), "min")
    print("\n" + "<<>>"*19)
    run("pause", shell=True)
#
# run program guarded by if __name__=="__main__"
if __name__ == "__main__":
    freeze_support()
    main()
