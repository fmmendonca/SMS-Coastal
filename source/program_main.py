# ###########################################################################
#
# File    : program_main.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : May 13th, 2023.
#
# Updated : Mar. 18th, 2024.
#
# Descrp. : SMS-Coastal main source code.
#
# ###########################################################################

from datetime import datetime
from time import time

from m_forc_manager import forc_manager
from m_sim_manager import sim_manager


def main():
    """Run SMS-Coastal components."""

    print("""
******************************************************************************
                            SMS-COASTAL PROGRAM
******************************************************************************
Simulation Management System for Coastal Hydrodynamic Models
          
Center for Marine and Environmental Research - CIMA
University of Algarve - UAlg

Software version : 3.0.18
Support contact  : hidrotec@ualg.pt
******************************************************************************
""")

    timer01 = time()

    # Run Forcing Processor:
    forc_manager()
    timer02 = time()
    if timer02-timer01 > 1:
        print("*"*78)
        print("Forcing Processor [COMPLETED]")
        print(f"System time  : {datetime.today().isoformat()}")
        print(f"Elapsed time : {int(timer02-timer01)} s")
        print("*"*78, end="\n\n")

    # Run Simulation Manager:
    sim_manager()
    timer03 = time()
    if timer03-timer02 > 1:
        print("*"*78)
        print("Simulation Manager [COMPLETED]")
        print(f"System time  : {datetime.today().isoformat()}")
        print(f"Elapsed time : {int(timer03-timer02)} s")
        print("*"*78, end="\n\n")

    # Run Data Processor:
    # data_manager()
    timer04 = time()
    if timer04-timer03 > 1:
        print("*"*78)
        print("Data Processor [COMPLETED]")
        print(f"Elapsed time : {int(timer04-timer03)} s")
        print("*"*78, end="\n\n")

    print(f"System time  : {datetime.today().isoformat()}")
    print(f"Total time   : {int(timer04-timer01)} s")
    print("*"*78, end="\n\n")


if __name__ == "__main__":
    main()
