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

from os import path

from m_data_soma import soma_thredds
from m_sim_operations import SimOps
from m_supp_mailing import initmail
from m_supp_mohid import runsim


def sim_manager():
    if not path.isfile("initsim.json"): return
    ops = SimOps()
    if ops.chcksim() > 0: return
    
    for stage in range(len(ops.ini)):
        runid = stage + 1
        if ops.prepsim(runid) > 0: return
        
        print(f"Running MOHID stage {runid}...")
        wkdir = path.join(ops.inpts.get("domains")[0], "exe")
        status = runsim("mohid", wkdir)

        # Simulation error management:
        if status < 1: continue
        err = "[ERROR] MOHID simulation failed\n"
        ops.logentry(err, True)
        initmail("ERROR", err)
        return
    
    # Copy simulation outputs:
    outdir = ops.copyouts()
    print("Simulation [COMPLETED]")
    
    # #######################################################################
    # Post-processing section
    if not ops.inpts.get("postops", False): 
        ops.logentry("[COMPLETED]\n", True)
        initmail("COMPLETED", "by Fernando's awesome Python code")
        return
    else:
        # Log will contain status messages from post-processing.
        ops.logentry("simulation [COMPLETED].", True)
    
    # Create database HDF5 files:
    outdirs = ops.database(outdir)
    if not outdirs: 
        # All the follwing operations depend on this one.
        return

    # Conversion to netCDF:
    status, ncdirs = ops.hdftonc(outdirs)
    statushist = [status,]

    # Thredds conversion module:
    if ops.inpts.get("thredds", False) and status < 1:
        # Need more time to make thredds conversion a generic process.
        # The conversions are way too distinct.

        status = soma_thredds(ncdirs, ops.inpts.get("outdir"))
        statushist.append(status)
        
        if status > 0:
            err = "[ERROR] thredds covnersion failed."
            ops.logentry(err, False)
            initmail("ERROR", err)

    # Copy to NAS/external disk:
    status = ops.cpnas(outdirs)
    statushist.append(status)

    # End simualtion manager:
    if 1 in statushist:
        # Clse the execution log when there is a failure.
        # This way it is possible to add more post-processing operations
        # in this code, always with the error message in the log ending,
        # with a dot instead of a new line character.
        ops.logentry("\n", False)
        return
    
    print("Post-processing [COMPLETED]")
    ops.logentry("post-processing [COMPLETED]\n", False)
    initmail("COMPLETED", "by Fernando's awesome Python code")
    