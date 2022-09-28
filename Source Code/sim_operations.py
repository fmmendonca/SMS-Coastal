# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2022-01-25
#
# Updated : 2022-02-05
#

from os import path, mkdir, chdir
from shutil import rmtree, copyfile, copytree
from datetime import datetime, timedelta
from glob import glob
from subprocess import run

import pandas as pd
import numpy as np

from support_mohid import readlog
from support_mailing import mailreport


def extsrcs(src):
    """src = string with the name of the external source
       
       Function containing information about the external sources for forcing
       data, such as maximum amount of days of forecast and the start time for
       each of the sources that SMS-Coastal can process."""

    # SMS-Coastal sources library:
    forc = {"Mercator": (10, 12),
            "MercatorH": (10, 23.5),
            "AMSEAS": (4, 0),
            "Skiron": (8, 0),
            "NAM": (3, 0),
            "GFS": (16, 1)}

    return dict(zip(("maxfct", "start"), forc.get(src)))


class SimOp:
    def __init__(self, inpts, root):
        self.smsc = inpts.get("smsc")
        self.root = self.smsc + "\\" + root
        self.opdate = inpts.get("opdate")
        self.modtype = inpts.get("modtype")
        self.levels = inpts.get("levels")
        self.mail = inpts.get("MAILTO")

        self.smsclog = self.root + "\\Operations\\LOGS\\smsc_run.log"
        self.msg = "Module " + __name__ + " ERROR: "
        self.sbj = path.basename(self.smsc) + f" {root} "

        # attributes to be updated by upper script:
        self.runini = None
        self.runfin = None
        self.runid = None
        self.rundt = None
    
    def redefinedir(self, dirpath):
        folder = self.root + dirpath
        if path.isdir(folder):
            rmtree(folder)
        mkdir(folder)

    def definedir(self, dirpath):
        folder = self.root + dirpath
        if not path.isdir(folder):
            mkdir(folder)

    def logentry(self, entry):
        with open(self.smsclog, "a") as log:
            log.write(entry)

    def environment(self):
        print("Preparing simulation environment...")
        
        # remove last simulation forcing data:
        SimOp.redefinedir(self, "\\General Data\\Operational Forcing")

        # remove last simulation results and executables:
        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level+1}"
            SimOp.redefinedir(self, lvpath + "\\exe")
            SimOp.redefinedir(self, lvpath + "\\res")

        # create operations and LOGS folders:
        SimOp.definedir(self, "\\Operations")
        SimOp.definedir(self, "\\Operations\\LOGS")

        # open SMS-Coastal run log:
        if not path.isfile(self.smsclog):
            SimOp.logentry(self, "Date;Time;Status\n")
        SimOp.logentry(self, str(self.opdate) + ";")

        # write tree.dat MOHID file:
        tree = "Automatic Generated Tree File\n"
        tree += "by FERNANDOs AWESOME PYTHON BASED PROGRAM\n"
        lvpath = ""
        
        for level in range(self.levels):
            lvpath += f"\\Level {level+1}"
            tree += "+"*(level + 1) + self.root + lvpath + "\\exe\n"
        
        with open(self.root + "\\Level 1\\exe\\Tree.dat", "w") as dat:
            dat.write(tree)

    def checkfins(self):
        # check fins folder:
        finsdir = self.opdate.strftime("\\Restart\\Operations\\FINS\\%y%m%d")
        if not path.isdir(self.smsc + finsdir):
            finsdir = finsdir.replace("Restart", "Forecast")
        finsdir = self.smsc + finsdir
        print("FIN files at:", finsdir)

        if not path.isdir(finsdir):
            entry = datetime.today().isoformat() + ";ERR01\n"
            SimOp.logentry(self, entry)
            print("<<>>"*19)
            print(self.msg + "FINS files missing")
            body = str(self.opdate) + " FINS files missing."
            mailreport(self.mail, self.sbj + "ERROR", body, ())
            print("<<>>"*19)
            return
        
        # output files names:
        fids = {"Hydrodynamic": "\\Hydrodynamic_0.fin",
                "WaterProperties": "\\WaterProperties_0.fin5",
                "GOTM": "\\GOTM_0.fin"}

        # copy to each level:
        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level+1}"
            fipt = glob(finsdir + f"\\LV{level+1:02d}_*.fin*")

            for file in fipt:
                print("Copying .\\" + path.basename(file))
                fout = self.root + lvpath + "\\res"
                fout += fids.get(path.basename(file).split("_")[1])
                copyfile(file, fout)
        print()
        return 1

    def checkhdf(self, src):
        """Method to search and copy MOHID hdf5 forcing files based on a
           single data source (src) and on a date range between self.runini
           and self.runfin dates. Dates come from the inputs dictionary."""

        print("Checking", src, "interpolated data...")
        # hdf files:
        srcpath = self.smsc + "\\FORC\\" + src + "\\Data"
        srchdfs = glob(srcpath + "\\*_LV" + self.modtype + ".hdf5")
        srchdfs = [path.basename(hdf) for hdf in srchdfs]

        # create a data frame with files names and dates:
        df = pd.DataFrame({"file": srchdfs,
                           "process": [hdf.split("_")[1] for hdf in srchdfs],
                           "initial": [hdf.split("_")[2] for hdf in srchdfs],
                           "final": [hdf.split("_")[3] for hdf in srchdfs]})

        # change dates columns to datetime.datetime objects:
        for col in df.columns:
            if col == "file":
                continue   
            df[col] = pd.to_datetime(df[col], format="%y%m%d")

        # sort files from newer from older:
        df.sort_values(by="process", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # filter files to date range:
        filt = (df["initial"] <= self.runini) & (df["final"] >= self.runfin)
        df = df.loc[filt].reset_index(drop=True)

        # ruturn if no file is found for the date range:
        if df.empty:
            return

        # get input files names:
        # file name as: Mercator_220107_220101_220110_LV2.hdf5
        filein = df.loc[0, "file"].split("_")[:-1]
        filein = sorted(glob(srcpath + "\\" + "_".join(filein) + "_LV*.hdf5"))

        # output folder:
        outpath = self.root + "\\General Data\\Operational Forcing\\" + src
        # copy files to folder:
        for file in filein:
            print(file)
            fileout = outpath + path.basename(file).split("_")
            copyfile(file, fileout)

        print()
        return 1

    def checktsdat(self, src):
        """Method to search and copy MOHID time series forcing files based on
           a single data source (src) and on a date range between inital
           (self.ini) and final (self.fin) dates."""

        print("Checking", src, "time series data...")
        # time series files (just basename without path):
        # e.g. file name: path\\NAM_YYMMDD_YYMMDD_YYMMDD.dat
        # NAM_opdate_ini_fin.dat
        srcpath = self.smsc + "\\FORC\\" + src + "\\Data"
        srcts = [path.basename(dat) for dat in glob(srcpath + "\\*.dat")]
        srcts_info = [path.splitext(dat)[0].split("_")[1:] for dat in srcts]

        # create a data frame with files names and dates:
        df = pd.DataFrame({"file": srcts,
                           "process": [tsinfo[0] for tsinfo in srcts_info],
                           "initial": [tsinfo[1] for tsinfo in srcts_info],
                           "final": [tsinfo[2] for tsinfo in srcts_info]})

        # change dates columns to datetime.datetime objects:
        for col in df.columns:
            if col == "file":
                continue   
            df[col] = pd.to_datetime(df[col], format="%y%m%d")

        # sort files from newer from older:
        df.sort_values(by="process", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # filter files to date range:
        filt = (df["initial"] <= self.runini) & (df["final"] >= self.runfin)
        df = df.loc[filt].reset_index(drop=True)

        # ruturn if no file is found for the date range:
        if df.empty:
            return

        # get input file name:
        # file name as: Mercator_220107_220101_220110.dat
        filein = srcpath + "\\" + df.loc[0, "file"]
        print(filein)

        # output file:
        fileout = self.root + "\\General Data\\Operational Forcing\\"
        fileout += src + ".dat"

        copyfile(filein, fileout)
        print()
        return 1

    def checkinitials(self, initials):
        """"Method to check if model have initial conditions in
            self.root\\General Data\\Initial Conditions"""

        print("Checking inital conditions files...")
        if not initials:
            # when initials == ""
            return

        dirpath = self.root + "\\General Data\\Initial Conditions"

        if initials == "monthly":
            strdate = self.opdate.strftime("%m")
        elif initials == "weekly":
            strdate = self.opdate.strftime("%U")
            # U: Week number of the year (Sunday as the first day of the week)
            # as a zero padded decimal number. All days in a new year preceding
            # the first Sunday are considered to be in week 0.
            strdate = "01" if strdate == "00" else strdate
        elif initials == "daily":
            strdate = self.opdate.strftime("%d")

        filesin = glob(dirpath + "\\*_" + strdate + ".dat")
        
        # copy file without strdate:
        for file in filesin:
            print(file)
            fileout = path.basename(file).split("_")[:-1]
            fileout = dirpath + "\\" + "_".join(fileout) + ".dat"
            copyfile(file, fileout)
        
        print()

    def initime(self, srcs):
        """srcs = iterable (list/tuple) containing the names of the external
           forcing sources used
        
           Method to determine the simulation start time based on the list of
           external forcing sources used. It will return the hour of the day
           to start the simulation."""

        # standar values if no sources are defined:
        if not srcs:
            return 0, 0

        # list all sources start time:
        startime = np.array([extsrcs(src).get("start") for src in srcs])

        # range subtractor:
        diftime = 0 if np.all(startime == startime[0]) else -1
        
        # return start time (int) and if all of them are the same (bool):
        return int(startime.max()), diftime

    def runsim(self, gmt):
        """gmt: integer indicating the gmt reference of the model
           got from init.dat

           Method to copy and create simulation files for each level and run
           MOHID executable. After the simulation code checks if the run was
           successful."""

        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level + 1}"
            
            # copy nopmfich:
            nfich = self.root + lvpath + f"\\data\\Nomfich_{self.runid}.dat"
            copyfile(nfich, self.root + lvpath + "\\exe\\Nomfich.dat")

            # create time series folder:
            SimOp.definedir(self, lvpath + f"\\res\\Run{self.runid}")

            # create Model_rundid.dat:
            nfich = path.dirname(nfich) + f"\\Model_{self.runid}.dat"
            with open(nfich, "w") as dat:
                dat.write("START        : ")
                dat.write(self.runini.strftime("%Y %m %d %H %M %S\n"))
                dat.write("END          : ")
                dat.write(self.runfin.strftime("%Y %m %d %H %M %S\n"))
                dat.write("DT           : " + str(self.rundt[level]) + "\n")
                dat.write("VARIABLEDT   : 0\n")
                dat.write("GMTREFERENCE : " + gmt + "\n")

        # change working directory and run MOHID:
        chdir(self.root + "\\Level 1\\exe")
        cmd = self.root + f"\\Operations\\LOGS\\mohid_"
        logs = cmd + f"run{self.runid}.txt", cmd + f"err{self.runid}.txt"
        cmd = self.smsc + "\\MOHID\\MOHIDWater.exe > "
        print("Running MOHID executable...")
        run(cmd + logs[0] + " 2> " + logs[1], shell=True)
        chdir(self.smsc)
        print("Simulation COMPLETED.", end="\n\n")

        # check mohid log file
        status = readlog(logs[0])
        if status:
            return 1

        SimOp.logentry(self, datetime.today().isoformat() + ";ERR03\n")
        print("<<>>"*19)
        print(self.msg + "Simulation stopped")
        body = str(self.opdate) + " Simulation ERROR."
        mailreport(self.mail, self.sbj + "ERROR", body, logs)
        print("<<>>"*19)

        # save failue files:
        SimOp.savefail(self, logs)
        
    def goodreport(self, closelog=0, sbj=""):
        """closelog = switch to write in SMS-Coastal log file

            Method to send a success report email. If configured, it will
            also write the last input to the SMS-Coastal log file."""
        
        # MOHID logfiles:
        logs = self.root + f"\\Operations\\LOGS\\mohid_"
        logs = logs + f"run{self.runid}.txt", logs + f"err{self.runid}.txt"
        
        # send mail report:
        body = str(self.opdate) + " Simulation COMPLETED."
        sbj = self.sbj + sbj + "COMPLETED"
        mailreport(self.mail, sbj, body, logs)

        if closelog < 1:
            return
        
        # write in SMS-Coastal log file:
        SimOp.logentry(self, datetime.today().isoformat() + ";1\n")

    def savefins(self, lastfin):
        """lastfin = integer boolean switch to get the last instant fins 
           
           Method to save the MOHID fins files for a subsequent simulation.
           The switch default value can be changed in operations 1 and 3, full
           simulation management and forecast management, if the keyword
           'LASTFIN' is set to 1 in init.dat."""
        #
        # check fin files date:
        #
        findate = self.runfin if (lastfin>0) else (self.opdate+timedelta(1))
        #
        # define fin files directory:
        #
        findir = "\\Operations\\FINS"
        SimOp.definedir(self, findir)
        findir += findate.strftime("\\%y%m%d")
        SimOp.redefinedir(self, findir)
        findir = self.root + findir
        #
        # iterate model levels and copy fins:
        #
        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level + 1}"

            # fin files location:
            finloc = self.root + lvpath + f"\\res\\*_{self.runid}"

            # glob opdate + timedelta(1) fins as input files:
            fipt = glob(finloc + findate.strftime("_%Y%m%d-*.fin*"))

            # glob last fins if 'lastfin' is on or if there is no files
            # in fipt (in case of a one-day simulation):
            if (lastfin > 0) or (not fipt):
                fipt = glob(finloc + ".fin*")

            # copy files:
            for file in fipt:
                fout = findir + f"\\LV{level + 1:02d}_" + path.basename(file)
                copyfile(file, fout)

    def saveres(self):
        """Method to save MOHID forecast and restart simulation output files,
           which are hydrodynamic and water properties HDF files and time
           series folder."""
        
        # define results directory:
        resdir = "\\Operations\\RES"
        SimOp.definedir(self, resdir)
        resdir = self.root + resdir + self.opdate.strftime("\\%y%m%d_")
        outdate = datetime.today()
        resdir += str(outdate.toordinal()) + outdate.strftime("T%H%M")
        mkdir(resdir)

        # iterate model levels and copy results:
        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level + 1}"

            # files location:
            floc = self.root + lvpath + "\\res\\"

            # hdfs:
            hdfs = glob(floc + "Hydrodynamic_*.hdf5")
            hdfs += glob(floc + "WaterProperties_*.hdf5")
            # time series folders:
            tsdir = glob(floc + "Run*")

            # copy hdfs:
            for file in hdfs:
                fout = resdir + f"\\LV{level + 1:02d}_" + path.basename(file)
                copyfile(file, fout)
            
            #c copy timeseries folders:
            for folder in tsdir:
                fout = resdir + f"\\LV{level + 1:02d}_TimeSeries_"
                copytree(folder, fout + path.basename(folder))
                
        return resdir

    def savefail(self, logs):
        """Method to save MOHID output files when a simulation finishes with
           error. Copies all data from the 'res' folder of each level to an
           output directory in 'Operations'."""

        # define output directory:
        outdir = "\\Operations\\FAILS"
        SimOp.definedir(self, outdir)
        outdir = self.root + outdir + self.opdate.strftime("\\%y%m%d_")
        outdate = datetime.today()
        outdir += str(outdate.toordinal()) + outdate.strftime("T%H%M")
        mkdir(outdir)

        # iterate model levels and copy results:
        lvpath = ""
        for level in range(self.levels):
            lvpath += f"\\Level {level + 1}"

            # input folder:
            iptdir = self.root + lvpath + "\\res"
            # output folder:
            optdir = outdir + f"\\LV{level+1}_" + path.basename(iptdir)
            # copy folder:
            copytree(iptdir, optdir)

        forc = self.root + "\\General Data\\Operational Forcing"
        copytree(forc, outdir + "\\Operational Forcing")

        [copyfile(log, outdir + "\\" + path.basename(log)) for log in logs]
