#
# File:        m_init_reader.py
#
# Created:     October 4th, 2021
#
# Author:      Fernando Mendonça (fmmendonca@ualg.pt)
#
# Purpose:     Read and check inputs from SMS-Coastal initializion file.
#              Save inputs in a binary pickle file.
#

import json
import pickle
from os import getcwd, path
from time import time
from datetime import datetime, timedelta


class Reader:
    def __init__(self):
        """Class to read and check the inputs from the SMS-Coastal
        initialization file (init.json).
        """

        # Set attributes:
        self.forc = {}
        self.sim = {}
        self.datm = {}
        self.public = {
            "root": getcwd(),
            "base": path.basename(getcwd()),
            "timer": time()
        }

        # Read json file:
        self.inpts = json.load(open("init.json", "rb"))
        self.inpts: dict
    
    def rdpublic(self) -> int:
        """Check the inputs inside 'public' section of SMS-Coastal
        initialization file. Return a status code.
        """
        
        print("Check inputs from 'public' section of 'init.json'.")
        inpts = self.inpts.get("public")

        # Check operation type:
        key = "optype"
        val = inpts.get(key)
        vals = ("forc", "sim", "simr", "data")
        
        if not val:
            print("ERROR: operation type is not defined.")
            return 1
        elif not isinstance(val, str) or val.lower() not in vals:
            print("ERROR: invalid operation type.")
            return 1
        
        self.public.update({key: val.lower()})
        
        # Check operatrion date:
        key = "opdate"
        val = inpts.get(key)
        
        if not val:
            val = datetime.today().strftime("%Y %m %d")
        elif not isinstance(val, str):
            print("ERROR: invalid type of operation date.")
            return 1
        
        try:
            val = datetime.strptime(val, "%Y %m %d")
        except ValueError:
            print("ERROR: invalid format of operation date.")
            return 1
        
        if val.date() > datetime.today().date():
            print("ERROR: operation date can't be greater than today.")
            return 1
        
        self.public.update({key: val})

        # Get email addresses for report:
        key = "email"
        vals = inpts.get(key)
        if not vals: vals = []

        if not isinstance(vals, list):
            print("ERROR: invalid type of 'email'.")
            return 1

        for val in vals:
            if isinstance(val, str): continue
            print("ERROR: invalid type in 'email'.")
            return 1
        self.public.update({key: vals})

        # Check forecast range:
        key = "forecast"
        val = inpts.get(key)
        if val is None: val = 0
        
        if not isinstance(val, int) or val < 0:
            print("ERROR: invalid forecast range value.")
            return 1
        
        self.public.update({key: val})

        # Check restart ranges:
        key = "restart"
        vals = inpts.get(key)
        if not vals: vals = [0,]

        if not isinstance(vals, list):
            print("ERROR: invalid type of 'restart'.")
            return 1
        
        for val in vals:
            if isinstance(val, int) and val >= 0: continue
            print("ERROR: invalid type in 'restart'.")
            return 1
        
        self.public.update({key: vals})

        # Check model number of levels.
        # Same as 'forecast' but different error message:
        key = "levels"
        val = inpts.get(key)
        if val is None: val = 0

        if not isinstance(val, int) or val < 0:
            print("ERROR: invalid number of model levels.")
            return 1
        
        self.public.update({key: val})

        # Check type of the model in 'modtype':
        key = "modtype"
        val = inpts.get(key)
        
        if not val:
            val = "1"
        elif val not in ("1", "2"):
            print("ERROR: invalid type of model.")
            return 1
        
        self.public.update({key: val})

        # Check restart operation in 'rstday':
        vals = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
        val = inpts.get("rstday")
        
        if self.public.get("optype") == "simr":
            val = self.public.get("opdate").strftime("%a").lower()
            # This way rston will always be True.
        elif not val:
            val = self.public.get("opdate") - timedelta(1)
            val = val.strftime("%a").lower()
            # rston will always be False.

        if val not in vals:
            print("ERROR: invalid value of restart day.")
            return 1

        # Define restart switch:
        if self.public.get("opdate").strftime("%a").lower() == val:
            val = True
        else:
            val = False
        
        self.sim.update({"rston": val})
        return 0

    def svpkl(self) -> None:
        """Save class dictionary attributes in a binary pickle file."""
        
        data = {
            "public": self.public, "forc": self.forc,
            "sim": self.sim, "data": self.datm,
        }
        pickle.dump(data, open("init.pkl", "wb"))

    def rdforc(self) -> int:
        """Check the inputs inside 'forc' section of SMS-Coastal
        initialization file. Return a status code.
        """

        print("Check inputs from 'forc' section of 'init.json'.")
        # SMS-coastal sources library:
        lib = {
            "MERCATOR": {"fctrg": 9, "iniat": 12},
            "MERCATORH": {"fctrg": 8, "iniat": 23.5},
            "AMSEAS": {"fctrg": 3, "iniat": 0}, 
            "SKIRON": {"fctrg": 7, "iniat": 0},
            "NAM": {"fctrg": 2, "iniat": 0},
            "GFS": {"fctrg": 14, "iniat": 1},
        }
        # Forecast range ('fctrg') is the x of D+x.
        # if x = 2 -> D+2 -> run for 3 days.

        # Check sources:
        forc = self.inpts.get("forc", {})
        srcs = forc.get("sources")
        if not srcs: srcs = {}

        if not srcs:
            print("WARNING: no data source defined.")
            self.sim.update({"skip": True})
            return 0
        
        # Check each source:
        srcsout = {}

        for key in srcs:
            # Error if source is not in SMS-Coastal library:
            if key.upper() not in lib.keys():
                print(
                    f"ERROR: '{key}' is not available in the library.",
                )
                return 1

            # Check parameters:
            src = cksrc(key, srcs.get(key))
            if not src: return 1
            
            # Update sources output:
            srcsout.update({key.upper(): src})
        
        # Update forc attribute:
        self.forc.update({"sources": srcsout})

        # Check grid limits in 'latlim' and 'lonlim':
        grid = []
        keys = ("latlim", "lonlim")
        for key in keys:
            vals = forc.pop(key, None)
            if vals is None or not isinstance(vals, list):
                print(f"ERROR: invalid type of '{key}'.")
                return 1
            elif len(vals) != 2:
                print(f"ERROR: invalid shape of '{key}'.")
                return 1         
            # Check each value:
            for val in vals:
                if isinstance(val, (int, float)): continue
                print(f"ERROR: invalid type in '{key}'.")
                return 1
            # Update grid:
            vals.sort()
            grid += vals
        self.forc.update({"grid": grid})

        # Check switch to skip Forcing Layer at Simulation one:
        val = forc.pop("skip", False)
        if val not in (True, False): val = False
        self.sim.update({"skip": val})

        # Sort sources by download start time:
        val = {}
        for srcid in srcsout:
            val.update({srcid: srcsout.get(srcid).get("start")})
        val = list(dict(sorted(val.items(), key=lambda src: src[1])).keys())
        # val.items gives a list of tuples [(key1, val1), (key2, val2)]
        # and src in lambda function is each of that pairs.
        # so src[1] is to sort by the second position of the pairs.
        
        # Update attributes:
        self.forc.update({"order": val})
        self.sim.update({"sources": val})

        # Check data initial time and difference in forecast range
        # for Simulation Manager.
        # Get the latest time between sources:
        iniat = []
        fctrg = []
        fctdt = 0

        for srcid in srcsout:
            iniat.append(lib.get(srcid).get("iniat"))
            fctrg.append(lib.get(srcid).get("fctrg"))

        iniat.sort(reverse=True)
        if iniat.count(iniat[0]) != len(iniat):
            # Sources begin at different time, lose one forecast day:
            fctdt += -1
        iniat = iniat[0]

        # Get the lowest forecast range between sources:
        fctrg.sort()
        fctrg = fctrg[0]

        # Compare with public section:
        fct = self.public.get("forecast")
        opdate = self.public.get("opdate") + timedelta(hours=iniat)

        # Check forecast range:
        if fct > fctrg:
            fct = fctrg
            print(f"WARNING: forecast range updated to {fct} day(s).")
        if fctdt < 0:
            fct += fctdt
            print(
                "WARNING: data sources initial time don't match.",
                f"Forecast range updated to {fct} day(s).",
            )
        self.public.update({"opdate": opdate, "forecast": fct})

        # Build dates:
        rst = 0
        for val in self.public.get("restart"): rst += val
        self.forc.update({
            "ini": (opdate - timedelta(rst)).date(),
            "fin": (opdate + timedelta(fct)).date(),
        })

        return 0

    def rdsim(self) -> int:
        return 0

    def rddatm(self) -> int:
        return 0


def cksrc(srcid: str, params: dict) -> int:
    """Check the parameters of only one source.
    
    Keywords arguments:
    srcid -- name of the inputed source;
    params -- parameters of a source.
    """

    # Extract parameters:
    start = 0 if not params.get("start") else params.get("start")
    swtch_old = [
        params.pop("tohdf", False), params.pop("tomodel", False),
        params.pop("tots", False), params.pop("merge", False),
    ]
    swtch_new = []
    batims = [] if not params.get("batims") else params.get("batims")
    geomt = "" if not params.get("geomt") else params.get("geomt")
    tsloc = [0, 0] if not params.get("tsloc") else params.get("tsloc")
    special = [] if not params.get("special") else params.get("special")
    cred = ["u", "p"] if not params.get("cred") else params.get("cred")

    #
    # Check start:
    if not isinstance(start, (int, float)):
        print(f"ERROR: invalid type of 'start' for '{srcid}'.")
        return {}
    if not 0 <= start < 24:
        print(f"ERROR: 'start' is out of range for '{srcid}'.")
        return {}

    #
    # Check boolean parameters:
    for param in swtch_old:
        val = param
        if val not in (True, False): val = False
        swtch_new.append(val)
    
    # 'merge' is True if any other switch is True:
    if True in swtch_new[:-1]: swtch_new[-1] = True

    # Check bathymetry and geometry file paths.
    # Check instances:
    if not isinstance(batims, list) or not isinstance(geomt, str):
        print(
            f"ERROR: invalid type of 'batims' or 'geomt' for '{srcid}'.",
        )
        return {}

    for val in batims:
        if isinstance(val, str): continue
        print(f"ERROR: invalid type in 'batims' for '{srcid}'.")
        return {}
    
    # Check if they are mandatory and values:
    if True in swtch_new[:2]:
        if not batims:
            print(
                f"ERROR: missing values in 'batims' for '{srcid}'.",
            )
            return {}
        for val in batims:
            if path.isfile(val): continue
            print(
                f"ERROR: bathymetry file not found for '{srcid}'.",
            )
            return {}

    if swtch_new[1] and not path.isfile(geomt):
        print(f"ERROR: geometry file not found for '{srcid}'.")
        return {}

    # Check tsloc:
    if not isinstance(tsloc, list) or len(tsloc) != 2:
        print(
            f"ERROR: invalid type/shape of 'tsloc' for '{srcid}'.",
        )
        return {}
    for val in tsloc:
        if isinstance(val, int): continue
        print(f"ERROR: invalid type in 'tsloc' for '{srcid}'.")
        return {}

    #
    # Check special operations:
    if not isinstance(special, list):
        print(f"ERROR: invalid type of 'special' for '{srcid}'.")
        return {}
    for val in special:
        if isinstance(val, str): continue
        print(f"ERROR: invalid type in 'special' for '{srcid}'.")
        return {}

    #
    # Check download credentials:
    if not isinstance(cred, list) or len(cred) != 2:
        print(f"ERROR: invalid type/shape of 'cred' for '{srcid}'.")
        return {}
    for val in special:
        if isinstance(val, str): continue
        print(f"ERROR: invalid type in 'cred' for '{srcid}'.")
        return {}

    return {
        "start": start,
        "tohdf": swtch_new[0], "tomodel": swtch_new[1],
        "tots": swtch_new[2], "merge": swtch_new[3],
        "batims": batims, "geomt": geomt, "tsloc": tsloc,
        "special": special, "cred": cred,
    }
