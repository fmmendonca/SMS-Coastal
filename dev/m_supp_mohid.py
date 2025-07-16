# ###########################################################################
#
# File    : m_supp_mohid.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : 2023.07.12
#
# Updated : 2025.07.15
#
# Descrp. : Contains a class to perform operations with MOHID executables.
#           Each method represents an operation. A practical use case for
#           this module is included at the end of the code.
#           Available operations:
#           - runsim: run a simulation with MOHIDWater.exe;
#           - other operations soon.
#
# ###########################################################################

from datetime import datetime
from os import chdir, getcwd, path
from subprocess import run
from typing import Sequence


class Mohid:
    def __init__(self, exedir: str, wkdir: str) -> None:
        """Class for running and managing operations with MOHID executables.
        The executable and working directories should be different to avoid
        any conflict when MOHID is running the same task across instances.
        
        Keyword arguments:
        - exedir: path to the directory where the executable is located;
        - wkdir: path to the working directory, where the MOHID tool will
        be launched.
        """

        self.exedir = exedir   # Executable directory.
        self.wkdir = wkdir     # Working directory.
        self.goterror = False  # Operation status.
        self.stderr = "[ERROR] m_supp_mohid."

        # Standard output from MOHID exe:
        self.mlog = path.join(wkdir, "mohid_stdout.txt")

    def chkfile(self, filename: str) -> None:
        """Checks if a file exists.
        
        Keyword argument:
        - filename: name and path of the file to be verified.
        """

        if not path.isfile(filename):
            print(self.stderr + "chkfile: FileNotFoundError")
            print(f"\tFile '{filename}' not found.")
            self.goterror = True

    def chklog(self) -> None:
        """Looks for the success message in the standard
        output file of the MOHID executable."""

        self.chkfile(self.mlog)
        if self.goterror: return

        # Iterate lines:
        found = False  # message found. 
        eof = False    # end of file.
    
        with open(self.mlog, "r") as dat:
            while not found and not eof:
                line = dat.readline()
                if not line:
                    eof = True
                elif ("Program" and "successfully terminated") in line:
                    found = True

        if not found:
            print(self.stderr + "chklog: RuntimeError")
            print(f"\tMOHID execution error.")
        
        self.goterror = found

    def update_keyword(self, datfile: str, key: str, newval: str) -> None:
        """Updates the value of a keyword in a MOHID .dat file.
        When writing the new value, it adds a blank after the ':'
        symbol and the new line character in the end.
        
        Keyword arguments:
        - datfile: name and path to the .dat file;
        - key: keyword to be updated;
        - newval: new value to be assigned.
        """

        # Check file and read lines:
        #
        self.chkfile(datfile)
        if self.goterror: return

        with open(datfile, "r") as dat:
            lines = dat.readlines()

        # Search for the keyword:
        # 
        found = False  # keyword found. 
        eol = 0        # end of lines.
    
        while not found and eol < len(lines):
            if key in lines[eol]:
                found = True
            else:
                eol += 1
        
        if not found:
            self.goterror = True
            print(self.stderr + "update_keyword: ValueError")
            print(f"\tKeyword '{key}' not found in '{datfile}'.")
            return
        
        # Change line and overwrite file:
        #
        lines[eol] = lines[eol].split(":")[0] + f": {newval}\n"
        
        with open(datfile, "w") as dat:
            for line in lines: dat.write(line)

    def runsim(
            self, domains: Sequence[str], runid: int,
            ini: datetime, fin: datetime) -> None:
        """Runs a MOHID Water simulation. Pre-processing consists of
        checking the Nomfich.dat file and updating the dates in the
        Model.dat file, for each model domain. Then, it writes the
        Tree.dat file in the working directory.
        
        Keyword arguments:
        - domains: list of paths of each domain of the model;
        - runid: simulation id, identified by the number of the .dat files;
        - ini: simulation start date and time;
        - fin: simulation end date and time.
        """

        # First domain executable folder should
        # be the same directory of self.wkdir:
        #
        exedirs = [path.join(dom, "exe") for dom in domains]

        if exedirs[0] != self.wkdir:
            print(self.stderr + "runsim: ValueError")
            print("\tWorking and domain directories don't match.")
            self.goterror = True
            return
            
        # Check domains:
        #
        pos = 0

        while not self.goterror and pos < len(domains):
            # Nomfich.dat file:
            datfile = path.join(exedirs[pos], "Nomfich.dat")
            self.chkfile(datfile)
            if self.goterror: continue

            # Update Model.dat:
            datfile = path.join(domains[pos], "data", f"Model_{runid}.dat")
            self.update_keyword(
                datfile, "START", ini.strftime("%Y %m %d %H %M %S"),
            )
            if self.goterror: continue
            self.update_keyword(
                datfile, "END", fin.strftime("%Y %m %d %H %M %S"),
            )

            # Loop control variable:
            pos+=1
        
        if self.goterror: return

        # Write Tree.dat:
        #
        tree = path.join(self.wkdir, "Tree.dat")

        with open(tree, "w") as dat:
            dat.write("MOHID Model and Submodel Configuration File\n")
            dat.write("Generated by Fernando's most awesome Python code\n")
            
            # The tree file must have two comments lines,
            # or MOHID will raise an error.

            for pos, dom in enumerate(exedirs):
                dat.write("+"*(pos+1) + dom + "\n")

        # Run MOHID:
        #
        curdir = getcwd()
        chdir(self.wkdir)
        
        exefile = path.join(self.exedir, "MOHIDWater.exe")
        self.chkfile(exefile)
        if self.goterror: return

        cmd = f"{exefile} > {self.mlog}"
        print(cmd)
        run(cmd, shell=True)
        chdir(curdir)  # Return to the original directory.
        self.chklog()


if __name__ == "__main__":
    # To test or use this module as a standalone:
    #
    
    # Run a simulation:
    #
    domains = ("D:\\soma\\soma_L0", "D:\\soma\\soma_L1")  # Model domains.
    ini = datetime(2025,12,1)         # Simulation start date and time.
    fin = datetime(2025,12,5)         # Simulation end date and time.
    mexe = "D:\\soma\\mohid"          # Location of MOHIDWater.exe.
    wkdir = "D:\\soma\\soma_L0\\exe"  # Simulation working directory.
    runid = 1                         # Simulation ID.

    mohid = Mohid(mexe, wkdir)              # Create mohid object.
    mohid.runsim(domains, runid, ini, fin)  # run the simulation.

    # Run a sequence of simulations:
    for runid in (1, 2, 3):
        # escape loop:
        if mohid.goterror: break
            
        mohid.runsim(domains, runid, ini, fin)
    
    # Other operations soon:
    #
