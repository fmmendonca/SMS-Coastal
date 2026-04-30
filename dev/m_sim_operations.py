# ###########################################################################
#
# File    : m_sim_operations.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Feb. 29th, 2024.
#
# Updated : Feb. 29th, 2024.
#
# Descrp. : Contains the class with methods to perform operations related
#           to a simulation with MOHID and data post-processing.
#
# ###########################################################################
import pdb
from datetime import datetime, timedelta
from os import makedirs, path
from shutil import rmtree
from typing import Sequence


class Model:
    def __init__(self):
        # Test inputs:
        self.inpts = {
            "opdate": "2025-07-16",  # [optional|today] # Operation date.
            "start": -6,             # [optional|0]     # Days from opdate to start the simulation.
            "end": -4,                                  # Days from opdate to end the simulation.
            "splitsim": False,       # [optional|False] # Split simulation in 1-day stages.

            "domains": ("D:\\soma\\model\\soma_L0",),  # Model domains.
            "mohidexe": "D:\\soma\\mohid",             # Location of MOHIDWater.exe.
            "outdir": "D:\\soma\\outputs",             # Output directory.

            "runid": 2,                                # Simulation ID.
            "continuous": True, # [optional|False]
            "finsdir": [],
        }

        self.stderr = "[ERROR] m_sim_operatons."
        self.goterror = False

        # Attributes to be checked:
        self.ini, self.fin, self.stage = None, None, None
        self.domains, self.outdir, self.mohidexe = None, None, None
        self.runid = None
        self.continuous, self.finsdir = None, None

    def setdates(self) -> None:
        """Checks the simulation dates and assigns values
        to the attributes 'ini' and 'fin'. Splits the dates
        into days, when configured by the user.
        """

        stderr = self.stderr + "setdates: "
        
        # Check operation date:
        opdate = str(self.inpts.pop(
            "opdate", datetime.today().date().isoformat()
        ))

        try:
            opdate = datetime.fromisoformat(opdate)
        except ValueError:
            print(stderr + "ValueError")
            print(f"\tInvalid isoformat string: '{opdate}'.")
            self.goterror = True
            return

        # Check operaion days range:
        start = self.inpts.pop("start", 0)
        end = self.inpts.pop("end", 0)

        if not isinstance(start, int) or not isinstance(end, int):
            print(stderr + "ValueError")
            print(f"\tInvalid literal for integer in 'start' or 'end'.")
            self.goterror = True
        elif start >= end:
            # This ensures that the simulation will have at least one day.
            print(stderr + "ValueError")
            print(f"\tEnd date smaller than start date.")
            self.goterror = True

        if self.goterror: return

        # Check the need to split the simulation into 1-day stages:
        splitsim = self.inpts.pop("splitsim", False)

        if not isinstance(splitsim, bool):
            print(stderr + "ValueError")
            print(f"\tInvalid literal for boolean in 'splitsim'.")
            self.goterror = True
            return

        ini = opdate + timedelta(start)
        fin = opdate + timedelta(end)
        self.stage = 0
        print("Simulation dates:")
        
        if not splitsim:
            self.ini, self.fin = (ini,), (fin,)
            print(" START  :", ini.isoformat())
            print(" END    :", fin.isoformat())
            print(" STAGES :", 1)
            return
        
        # Split simulation into 1-day stages:
        dates = []

        while ini <= fin:
            dates.append(ini)
            ini += timedelta(1)

        self.ini, self.fin = tuple(dates[:-1]), tuple(dates[1:])

        stdout = " START  : "
        for val in model.ini: stdout += val.isoformat() + " "
        print(stdout)

        stdout = " END    : "
        for val in model.fin: stdout += val.isoformat() + " "
        print(stdout)

        print(" STAGES :", len(self.ini))

    def checkdir(self, dpath: str) -> None:
        """Checks if a directory exists.
        
        Keyword argument:
        - dpath: path to the tested directory.
        """

        if path.isdir(dpath): return

        self.goterror = True
        print(self.stderr + "checkdir: NotADirectoryError")
        print(f"\tDirectory '{dpath}' not found.")

    def checparams(self) -> None:
        """Checks some of the user-defined inputs: domains,
        mohidexe, outdir, runid, continuous, finsdir.
        """
        
        # Check output directory:
        #
        val = self.inpts.pop("outdir", "")
        self.checkdir(val)
        if self.goterror: return
        self.outdir = val
       
        # Mohid executable and the domains folders are tested in the
        # Mohid class (which can be used as standalone). Here the code
        # only ensures that they are from str type, and that they are
        # absolute paths. However the domains folders will be tested
        # here as well, as other methods from the class will try to
        # manage files inside them:
        #
        val = str(self.inpts.pop("mohidexe", ""))
        self.mohidexe = path.abspath(val)

        vals = self.inpts.pop("domains", ["",])
        if not isinstance(vals, Sequence): vals = [vals, ]
        vals = [path.abspath(str(val)) for val in vals]
        
        for val in vals:
            self.checkdir(val)
            if self.goterror: return
        
        self.domains = tuple(vals)

        # Check simulation ID:
        #
        val = self.inpts.pop("runid", "")

        # Setting the value as an empty string makes the input
        # mandatory when tested in the if statement below.

        if not isinstance(val, int) or val < 1:
            print(self.stderr + "checparams: ValueError")
            print(f"\tInvalid literal for integer in 'runid'.")
            self.goterror = True
            return
        
        # Check FINs directories:
        #
        val = self.inpts.pop("continuous", False)
        if not isinstance(val, bool): val = False
        self.continuous = val
        if not val: return

        # If it is a continuous simulation, then finsdir must exist:
        vals = self.inpts.pop("finsdir", None)

        if not isinstance(vals, Sequence) or len(vals) < 1:
            print(self.stderr + "checparams: ValueError")
            print(f"\tInvalid type/value for sequence of strings in 'runid'.")
            self.goterror = True
            return

        self.finsdir = tuple([path.abspath(str(val)) for val in vals])

    def getfins(self) -> None:
        if not self.continuous: return

        # Clean res directories:
        for val in self.domains:
            resdir = path.join(val, "res")
            if path.isdir(resdir): rmtree(resdir)
            makedirs(path.join(resdir, f"Run{self.runid}"))

        # Preciso testar cada pasta de fins, e se encontrar em uma delas,
        # então não preciso ir para as seguintes:
        found = False
        pos = 0
        # PAREI AQUI!!!
        while not found and pos <= len(self.finsdir):
            finsdir = self.ini[self.stage].date().isoformat()
            finsdir = path.join(self.finsdir[pos], )
            pos += 1

    def prepsim(self) -> None:
        # Remove previous results
        # Copy fins
        pass

if __name__ == "__main__":
    model = Model()
    model.setdates()
    model.checparams()
    pdb.set_trace()