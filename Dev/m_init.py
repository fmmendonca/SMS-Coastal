#
# File:        m_init.py
#
# Created:     October 28th, 2022
#
# Author:      Fernando Mendonça (fmmendonca@ualg.pt)
#
# Purpose:     Coordinate the process of reading inputs from
#              SMS-Coastal initialization file. This module can be
#              used in the python interpreter to read 'init.json' and
#              generate 'init.pkl'. The last will be used in other
#              modules of SMS-Coastal.
#

from os import path

from m_init_reader import Reader


def init() -> int:
    """Coordinate the reading of the inputs inside SMS-Coastal
    initialization file. Return a status code.
    """

    # Check initialization file:
    fname = "init.json"
    if not path.isfile(fname):
        print("ERROR: SMS-Coastal initialization file not found.")
        return 1

    # Create reader object and read public section:
    reader = Reader()
    status = reader.rdpublic()
    if status > 0: return 1

    # Get operation type:
    optype = reader.public.get("optype")

    # Read inputs according to opearation type:
    # Forcing processor:
    if optype in ("forc", "sim", "simr"):
        status = reader.rdforc()
    if status > 0: return 1

    # Simulaltion manager:
    if optype in ("sim", "simr"):
        status = reader.rdsim()
    if status > 0: return 1

    # Data manager:
    if optype == "data":
        status = reader.rddatm()
    if status > 0 : return 1
    
    # Save pickle input file:
    reader.svpkl()
    return 0
