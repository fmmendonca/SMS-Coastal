# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-10-04
#
# Updated : 2022-01-26

from subprocess import run
from os import path, getcwd
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


class InitReader:
    def __init__(self):
        """Class with the methods to read and update the keywords 
           inputted by the user in init.dat. The lines are transformed 
           into a dictionary."""
        #
        # set attributes:
        #
        self.inpts = {"smsc": getcwd()}
        self.messg = "Module ini_read.py ERROR:"
        #
        # check .\\init.dat file:
        #
        initdat = "init.dat"
        if not path.isfile(initdat):
            InitReader.endread(self, ".\\init.dat file not found.")
        #
        # update self.inpts with the lines in .\\init.dat:
        #
        with open(initdat, "r") as dat:
            for row in dat:
                # remove blanks and enter/returns:
                line = row.strip()
                
                # skip blank and commented lines (starting with !):
                if (not line) or (":" not in line) or (line[0] == "!"):
                    continue
                
                # update inpts keyword=(left of 1st :) and value=(right):
                line = line.split(':', maxsplit=1)
                self.inpts[line[0].strip()] = line[1].strip()
        #
        # check optype:
        #
        InitReader.checkseq(self, "OPTYPE", "", ("1", "2", "3", "4"))
        #
        # check opdate:
        #
        key = "OPDATE"
        val = self.inpts.pop(key, datetime.today().strftime("%Y %m %d"))
        try:
            val = datetime.strptime(val, "%Y %m %d")
        except ValueError:
            InitReader.endread(self, key + " reading error.")
        self.inpts[key.lower()] = val.date()
    
    def endread(self, messg):
        """messg = string with a message to print on screen.
        
           Method to terminate the reading of .\\init.dat
           and SMS-Coastal execution."""

        print(self.messg, messg)
        run("pause", shell=True)
        raise SystemExit
    
    def checkseq(self, key, std, seq):
        """key = string of a key in self.inpts
           std = standard value of the key if not found in self.inpts
           seq = sequence (list/tuple) with accepted values

           Method to check values in a sequence."""
        
        val = self.inpts.pop(key, std)
        if val not in seq:
            InitReader.endread(self, key + " reading error.")
        self.inpts[key.lower()] = val
        
    def checkdec(self, key, std, to_tuple):
        """key = string of a key in self.inpts
           std = standard value for the key if not found in self.inpts
           to_tuple = boolean switch to store values in self.inpts as a tuple

           Method to check if a keyword value contains only integers
           equal o greater than zero."""
        
        val = self.inpts.pop(key, std)
        if not val.replace(" ", "").isdecimal:
            InitReader.endread(self, key + " reading error.")
        
        if not to_tuple:
            self.inpts[key.lower()] = int(val)
            return
        
        self.inpts[key.lower()] = tuple([int(num) for num in val.split()])

    def checkfloat(self, key, std, to_tuple):
        """key = string of a key in self.inpts
           std = standard value for the key if not found in self.inpts
           to_tuple = boolean switch to store values in self.inpts as a tuple

           Method to check if a keyword value contains only floats numbers."""
        
        try:
            val = np.array(self.inpts.pop(key, std).split()).astype("f4")
        except ValueError:
            InitReader.endread(self, key + " reading error.")

        if to_tuple:
            self.inpts[key.lower()] = tuple(val)
        else:
            self.inpts[key.lower()] = float(val)
        
    def forc(self):
        """Method to check the keywords of the Forcing Layer:
           FORECAST, RESTART, FORC, HDFINTP, TSCONV, FSTART,
           TSLOCY, TSLOCX, LATLIM, LONLIM, BATIMS, GEOMT"""
        #
        # opdate defined in init:
        #
        opdate = self.inpts.get("opdate")
        #
        # check FORECAST and define fin keyword:
        # 
        InitReader.checkdec(self, "FORECAST", "0", False)
        self.inpts["fin"] = opdate + timedelta(self.inpts.get("forecast"))
        #
        # check RESTART and define ini keyword:
        #
        InitReader.checkdec(self, "RESTART", "0", True)
        val = int(np.array(self.inpts.get("restart")).sum())
        self.inpts["ini"] = opdate - timedelta(val)
        #    
        # allocate forc dictionary and check FORC:
        #
        forc = 'src', 'hdfintp', 'tsconv', 'fstart', 'tsloc'
        forc = {key:[] for key in forc}
        vals = self.inpts.pop("FORC", None)

        # no further keywords are necessary if no source is defined in FORC:
        if not vals:
            # save in inputs as an empty DataFrame
            self.inpts["forc"] = pd.DataFrame(forc)
            return
            
        # check if chosen source(s) is in SMS-Coastal library:
        lib = "Mercator", "MercatorH", "AMSEAS", "NAM", "GFS"
        for val in vals.split():
            if val not in lib:
                InitReader.endread(self, "FORC unrecognized source.")
        
        # update forc dictionary:
        forc.update({"src": [val for val in vals.split()]}) 
        #
        # check HDFINTP, TSCONV, FSTART:
        #
        nsrcs = len(forc.get("src"))  # number of sources
        
        for key in ("HDFINTP", "TSCONV", "FSTART"):
            # standard values are 0 for every source:
            InitReader.checkdec(self, key, nsrcs * "0 ", True)
            
            # get and check values and size:
            vals = np.array(self.inpts.pop(key.lower())).astype("i1")

            if len(vals) != nsrcs:
                messg = key + " must have same number of entrys as FORC."
                InitReader.endread(self, messg)
            
            if key == "FSTART" and vals.max() > 23:
                InitReader.endread(self, key + " maximum value is 23.")

            elif key != "FSTART" and vals.max() > 1:
                InitReader.endread(self, key + " accepted values are 0 or 1.")
                
            forc.update({key.lower(): tuple(vals)})
        #
        # time series location cells if TSCONV > 0:
        #
        check = 1 in forc.get("tsconv")
        tsloc = []
        
        # Y = latitude, X = longitude:
        for key in ("TSLOCY", "TSLOCX"):
            if check and not self.inpts.get(key):
                InitReader.endread(self, key + " missing.")

            InitReader.checkdec(self, key, nsrcs * "0 ", True)
            vals = self.inpts.pop(key.lower())

            if len(vals) != nsrcs:
                messg = key + " must have same number of entrys as FORC."
                InitReader.endread(self, messg)
            
            tsloc.append(vals)
        #
        # update forc and store in self.inpts as DataFrame:
        #
        forc.update({"tsloc": list(zip(*tsloc))})
        self.inpts["forc"] = pd.DataFrame(forc)
        #
        # check grid limits:
        #
        grid = []

        for key in ("LATLIM", "LONLIM"):           
            # std = string also checks if user didnt define keyword:
            InitReader.checkfloat(self, key, "None", True)

            vals = list(self.inpts.pop(key.lower()))
            vals.sort()

            if len(vals) != 2:
                InitReader.endread(self, key + " reading error.")

            grid += vals

        # update self.inpts with grid:
        self.inpts["grid"] = tuple(grid)
        #  
        # check bathymetry and geometry paths:
        #
        check = 1 in forc.get("hdfintp")
        if not check:
            return

        for key in ("BATIMS", "GEOMT"):
            val = self.inpts.pop(key, "")
            
            if not path.exists(val):
                InitReader.endread(self, key + " reading error.")
            
            self.inpts[key.lower()] = val
    
    def levels(self):
        """Method to check MODTYPE and LEVELS keywords."""
        #
        # check the need for the keywords
        # skip in operation 2 with no interpolation:
        #
        optype = self.inpts.get("optype")
        check = self.inpts.get("forc")["hdfintp"].max()
        if optype == "2" and (check < 1 or pd.isna(check)):
            return
        #
        # check MODTYPE:
        #
        InitReader.checkseq(self, "MODTYPE", "", ("1", "2"))
        #
        # check LEVELS:
        #
        InitReader.checkdec(self, "LEVELS", "", False)

        if self.inpts.get("levels") < 1:
            InitReader.endread(self, "LEVELS reading error.")

    def siminpts(self):
        """Method to check the simulation layer keywords that are common
           to operations 1, 3 and 4: GMTREF, SKIPFORC, INTIALS."""
        #
        # check GMTREF (optional keyword):
        #
        key = "GMTREF"
        InitReader.checkfloat(self, key, "0", False)
        self.inpts[key.lower()] = str(int(self.inpts.get(key.lower())))
        #
        # check SKIPFORC (optional keyword):
        #
        InitReader.checkseq(self, "SKIPFORC", "0", ("0", "1"))
        #
        # check INITIALS (optional keyword):
        #    
        InitReader.checkseq(self, "INITIALS", "", ("", "monthly", "weekly"))

    def rstday(self):
        """Method to check RSTDAY keyword for operation 1."""
        #
        # check RSTDAY:
        #
        key = "RSTDAY"  # restart day
        val = self.inpts.pop(key, "")

        # define weekdays values:
        rst = "mon", "tue", "wed", "thu", "fri", "sat", "sun"
        rst = {key:day for (key, day) in zip(rst, range(1, len(rst) + 1))}
        #
        # range from 1, because rst == 0 = True in if not rst
        #

        # get value from input:
        rst = rst.get(val.lower())

        if not rst:
            InitReader.endread(self, key + " reading error.")
        
        self.inpts[key.lower()] = rst - 1

    def opfct(self):
        """Method to check the simulation layer keywords that are common
           to operations 1 and 3: FCTDT, LASTFIN, MOHIDNC, SMSCNC."""
        #
        # check FCTDT: dt_lv1, dt_lv2, dt_lv3...
        #
        InitReader.checkdec(self, "FCTDT", "None", True)
        if len(self.inpts.get("fctdt")) != self.inpts.get("levels"):
            InitReader.endread(self, "FCTDT reading error.")
        #
        # check optional keywords:
        #
        InitReader.checkseq(self, "LASTFIN", "0", ("0", "1"))
        InitReader.checkseq(self, "MOHIDNC", "0", ("0", "1"))
        InitReader.checkseq(self, "SMSCNC", "0", ("0", "1"))

    def oprst(self):
        """Method to check the simulation layer keywords that are common
           to operations 1 and 4: RSTDTi, being i the number of stages."""
        #
        # get reatart numebr of stages and the model number of levels:
        #        
        nstages = len(self.inpts.get("restart"))
        nlevels = self.inpts.get("levels")
        #
        # iterate stages:
        #
        for stage in range(nstages):
            key = f"RSTDT{stage+1}"
            InitReader.checkdec(self, key, "None", True)
            if len(self.inpts.get(key.lower())) != nlevels:
                InitReader.endread(self, key + " reading error.")
