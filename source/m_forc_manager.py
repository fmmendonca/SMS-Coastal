# ###########################################################################
#
# File    : m_forc_manager.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Apr. 22nd, 2023.
#
# Updated : Mar. 11th, 2024.
#
# Descrp. : Main source code of the Forcing Processor component. Coordinates
#           the reading of inputs and the execution of the specific processes
#           for each data source.
#
# ###########################################################################

from datetime import datetime, timedelta
from json import load
from os import path

# ocean data sources:
from m_forcsrc_amseas import amseas
from m_forcsrc_cmems import cmems

# atmospheric data sources:
from m_forcsrc_namca import namca
from m_forcsrc_skiron import skiron
from m_forcsrc_gfsflux import gfsflux

from m_supp_mailing import initmail


def forc_manager() -> None:
    """Coordinates the reading of inputs and the execution
    of the specific processes for each data source.
    """

    # Read inputs from the initialization file: 
    if not path.isfile("initforc.json"): return

    with open("initforc.json", "rb") as dat:
        inpts = load(dat)
        inpts: dict

    print("FORCING PROCESSOR")
    prms = {}

    # Check parameters:
    val = datetime.today().strftime("%Y %m %d")
    val = inpts.pop("opdate", val)
    val = datetime.strptime(val, "%Y %m %d")

    if val > datetime.today():
        err = "[ERROR] m_forc_manager: invalid 'opdate'"
        initmail("ERROR", err)
        return
    
    prms["opdate"] = val
    
    # Add data range:
    vals = [prms["opdate"] - timedelta(inpts.pop("hindcast", 0)),]
    vals.append(prms["opdate"] + timedelta(inpts.pop("forecast", 0)))
    prms["drange"] = vals

    # Add grid limits:
    vals = inpts.pop("grid", [])

    if not isinstance(vals, list) or len(vals) != 4:
        print("[ERROR] m_forc_manager: missing/invalid 'grid'")
        initmail("ERROR", err)
        return
    
    prms["grid"] = vals

    # Add number of files to keep:
    prms["keepold"] = inpts.pop("keepold", -99)
      
    # WARNING: MOHID .exes don't work very well with muliprocessing
    # module (problems releated to current working directory while
    # executing more than one .exes). Better to solve this problem
    # before trying to run processes in parallel!

    # Run each process:
    srcsid = list(inpts.keys())
    
    for sid in srcsid:
        # Had to turn keys into a list so is possible to pop
        # itens from de dictionaty inside the loop.

        match sid:
            case "mercator":
                cmems(prms, inpts.pop(sid, {}))
            case "amseas":
                amseas(prms, inpts.pop(sid, {}))
            case "skiron":
                skiron(prms, inpts.pop(sid, {}))
            case "namca":
                namca(prms, inpts.pop(sid, {}))
            case "gfsflux":
                gfsflux(prms, inpts.pop(sid, {}))
            case _:
                print(f"[WARNING] '{sid}' is not in SMS-Coastal library.")
    