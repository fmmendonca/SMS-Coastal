# ###########################################################################
#
# File    : m_supp_mohid.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : 2023.07.12
#
# Updated : 2024.04.11
#
# ###########################################################################
import pdb
from datetime import datetime
from os import path
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
            self, domains: Sequence[str],
            ini: datetime, fin: datetime) -> None:
        """
        Keyword arguments:
        - domains:;
        - ini:
        - fin:
        """

        pass


x = Mohid("D:\\soma\\mohid", "D:\\soma\\soma_L0\\exe")
x.update_keyword("D:\\soma\\soma_L0\\data\\Model_1.dat", "START", "2025 12 01 00 00 00")
x.update_keyword("D:\\soma\\soma_L0\\data\\Model_1.dat", "END", "2026 01 31 00 00 00")