# ###########################################################################
#
# File    : m_sim_manager.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Feb. 29th, 2024.
#
# Updated : Mar. 18th, 2024.
#
# Descrp. : Controls the MOHID staged simulation process and coordinates data
#           post-processing. In this module, the specific post-processing
#           model function for the Thredds server must be added. Can be used
#           as a stand-alone.
#
# ###########################################################################
import pdb
from datetime import datetime
from os import path, makedirs
from shutil import rmtree

from m_sim_operations import Model
from m_supp_mohid import Mohid


def sim_manager():
    """Runs a single simulation identified by an ID"""

    # Inputs:
    #
    domains = ("D:\\soma\\model\\soma_L0",)  # Model domains.
    ini = datetime(2025,12,1)         # Simulation start date and time.
    fin = datetime(2025,12,5)         # Simulation end date and time.
    mexe = "D:\\soma\\mohid"          # Location of MOHIDWater.exe.
    runid = 2                         # Simulation ID.

    continuous = False
    finsdir = []
    splitsim = False
    outdir = "D:\\soma\\outputs"
    submodels = True

    # #######################################################################
    # Clean previous simulation outputs:
    
    

    # #######################################################################
    # Copy FIN files to res folders:

    found = False
    pos = 0
    
    while continuous and not found and pos < len(finsdir):
        val = path.join(finsdir, ini.date().isoformat())
        pos += 1

    if continuous and not found:
        print("[ERROR] m_sim_manager.sim_manager: FileNotFoundError")
        print(f"\tFIN files not found.")
        # write to log, send report, write to trigger (if needed)
        raise SystemExit
    
    # #######################################################################
    # Crete Mohid object and run simulation:
    
    # mohid = Mohid(mexe, path.join(domains[0], "exe"))
    pdb.set_trace()


if __name__ == "__main__":
    sim_manager()
