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

from datetime import datetime, timedelta
from glob import glob
from json import load
from os import makedirs, path, unlink
from shutil import copyfile, copytree, rmtree

import pandas as pd

from m_data_hdftonc import hdftonc
from m_supp_mailing import initmail
from m_supp_mohid import extract, gluehdfs


class SimOps:
    def __init__(self) -> None:
        self.ini = []
        self.fin = []
        self.pterr = "[ERROR] m_sim_operations:"
        self.inpts = {}

        # Test the module:
        # self.inpts = {
        #     "domains": [".\\sim\\soma_L0", ".\\sim\\soma_L0\\soma_L1", ".\\sim\\soma_L0\\soma_L1\\soma_L2"],
        #     "outdir": ".\\outputs",
        #     "modtype": 2,
        #     "postops": True,
        # }
        # self.ini = [datetime(2024, 1, 26, 12), datetime(2024, 1, 28, 12)]
        # self.fin = [datetime(2024, 1, 28, 12), datetime(2024, 2, 1, 12)]

    def chcksim(self) -> int:
        """Reads the initialization file (init.json), checks some
        of the user inputs and prepares the simulation directories.
        Returns the value 1 when a problem is found and 0 otherwise.
        """

        # Check and read JSON file:
        with open("initsim.json", "rb") as dat:
            inpts = load(dat)
            inpts: dict

        # Check inputs:
        # Some of them are checked later, when they are needed.
        print("SIMULATION MANAGER")
        print("Reading input file...")

        for key in ("outdir", "generaldata"):
            val = inpts.get(key, "")

            if not path.isdir(val):
                print(self.pterr, f"'{key}' directory not found")
                return 1

        # Operation date and simulation ranges:
        opdate = inpts.pop("opdate", datetime.today().strftime("%Y %m %d"))
        opdate = datetime.strptime(opdate, "%Y %m %d")
        # Here 'opdate' is at 00:00.

        hct = inpts.pop("hindcast", [])
        fct = inpts.pop("forecast", [])
        
        if opdate > datetime.today():
            print(self.pterr, "invalid operation date")
            return 1
        
        if not hct and not fct:
            print(self.pterr, "null simulation range")
            return 1
        
        # 'opdate' at model start time:
        opdate += timedelta(hours=inpts.pop("startime", 0)) 
        
        # Get simulation stages dates:
        simdates = []
        sumd = 0  # sum of days
        hct.reverse()
        
        for deltad in hct:
            # 'deltad' is the days range of each stage
            if not isinstance(deltad, int) or deltad < 1:
                print(self.pterr, "invalid hindcast step time")
                return 1
            sumd += deltad
            simdates.append(opdate - timedelta(sumd))

        simdates.reverse()       # sort in ascending order of days
        simdates.append(opdate)  # add final date
        sumd = 0
        
        for deltad in fct:
            if not isinstance(deltad, int) or deltad < 1:
                print(self.pterr, "invalid forecsat step time")
                return 1
            sumd += deltad
            simdates.append(opdate + timedelta(sumd))

        self.ini = simdates[:-1]
        self.fin = simdates[1:]
        stages = len(self.ini)
        
        print("Operation date        :", opdate.isoformat())
        print("Simualtion stages     :", stages)
        print("Stage(s) start date(s):", end=" ")
        for val in self.ini: print(val.strftime("%Y-%m-%d"), end=" ")
        print()
        print("Stage(s) end date(s)  :", end=" ")
        for val in self.fin: print(val.strftime("%Y-%m-%d"), end=" ")
        print()

        # Check domains directories:
        print("Model domains:")
        doms = inpts.get("domains", [])
        status = 0
        idom = 0

        if not doms:
            print(self.pterr, "missing model directories")
            return 1
        
        while status == 0 and idom < len(doms):
            print("", doms[idom])
            
            if not path.isdir(doms[idom]):
                print(self.pterr, "missing domain directory")
                status = 1
                continue
            
            # Check exe, data and res of each domain:
            outdir = path.join(doms[idom], "exe")

            if idom == 0 and not path.isdir(outdir):
                # First domain must already have 'exe'
                # directory with Tree.dat file.
                print(self.pterr, "missing 'exe' directory")
                status = 1
                continue
            else:
                makedirs(outdir, exist_ok=True)

            outdir = path.join(doms[idom], "data")
            
            if not path.isdir(outdir):
                print(self.pterr, "missing 'data' directory")
                status = 1
                continue

            # Clean previous simulation results:
            outdir = path.join(doms[idom], "res")    
            if path.isdir(outdir): rmtree(outdir)
            makedirs(outdir)

            idom += 1
        
        if status > 0: return 1

        # Check simualtion step time:
        vals = inpts.get("domdts", [])

        if not vals or len(vals) != len(doms):
            print(self.pterr, "invalid/missing domain Δts")
            return 1
        
        print("Iteration step time (Δt) in seconds:")
        for val in vals:
            # The amount of Δts must be the same of stages:
            if len(val) != stages:
                print(self.pterr, "invalid/missing stage Δt")
                return 1
            print("", val)

        self.inpts = inpts.copy()
        self.logentry(self.ini[0].strftime("%Y%m%d->"), True)
        self.logentry(self.fin[-1].strftime("%Y%m%d;"), False)
        return 0
    
    def logentry(self, entry: str, addate: bool) -> None:
        """Adds an entry to the execution log, which is created
        in the outputs directory.

        Keyword argument:
        - entry: text to write in the simulation log file;
        - addate: adds the date and time before the entry.
        """

        logfile = path.join(self.inpts.get("outdir"), "smslog.dat")
        
        if not path.isfile(logfile):
            dat = open(logfile, "w")
            dat.write("runtime;simdates;endtime;status\n")
        else:
            dat = open(logfile, "a")

        if addate:
            dat.write(datetime.today().isoformat(timespec="seconds") + ";")

        dat.write(entry)
        dat.close()

    def prepsim(self, runid) -> int:
        """Prepares single-stage simulation files by setting up the Model.dat
        file and copying Nomfich.dat to each domain's working directory.
        
        Keyword argument:
        - runid: simulation stage index, which is equivalent to the index
        of the model data files in MOHID, as in e.g. WaterProperties_2.dat.
        """
    
        # Copy nomfich.dat file and write model.dat:
        print("Setting up simulation stage " + str(runid) + "...")
        ini = self.ini[runid-1]  # stage initial date
        fin = self.fin[runid-1]  # stage final date
        domains = self.inpts.get("domains")
        dts = self.inpts.get("domdts")
        status = 0
        idom = 0

        while status == 0 and idom < len(domains):
            # The folders exe, data and res are checked before,
            # so at this point they all exist.
            datadir = path.join(domains[idom], "data")
            
            # Copy nomfich:
            fipt = path.join(datadir, f"Nomfich_{runid}.dat")
            
            if not path.isfile(fipt):
                status = 1
                continue

            fout = path.join(domains[idom], "exe", "Nomfich.dat")
            copyfile(fipt, fout)

            # Write model.dat:
            fout = path.join(datadir, f"Model_{runid}.dat")
            
            with open(fout, "w") as dat:
                dat.write("START        : ")
                dat.write(ini.strftime("%Y %m %d %H %M %S\n"))
                dat.write("END          : ")
                dat.write(fin.strftime("%Y %m %d %H %M %S\n"))
                dat.write(f"DT           : {dts[idom][runid-1]}\n")
                dat.write("VARIABLEDT   : 0\n")
                dat.write("GMTREFERENCE : 0\n")

            # Create time series directory:
            outdir = path.join(domains[idom], "res", f"Run{runid}")
            makedirs(outdir, exist_ok=True)

            idom += 1

        if status > 0:
            err = self.pterr + " nomfich file not found: " + fipt
            self.logentry(err + "\n", True)
            print(err)
            initmail("ERROR", err)
            return 1
        
        # Run other methods that were encapsulated for better debugging:
        fbox = (self.continuous, self.initials, self.getforc)
        item = 0

        while status == 0 and item < len(fbox):
            status = fbox[item](runid)
            item += 1

        return status
        
    def getforc(self, runid: int) -> int:
        """Searches for external forcing data according to the start
        and end dates of the simulation stage, and copies it to the
        'General Data/Boundary Conditions' directory. The forcing file name
        must be in the following format: 'prefix-date1_date2.extension',
        where date1 is the start date of the data in the file and date2,
        the end date, both in the format 'YYYMMDD'.
        
        Keyword argument:
        - runid: simulation stage index, which is equivalent to the index
        of the model data files in MOHID, as in e.g. WaterProperties_2.dat.
        """

        # 'runid' matches the MOHID simulation ID, which starts at 1.
        # Must subtract 1 from it to get the correct index in Python,
        # which starts at 0:
        runid -= 1

        # The program may continue searching for forcing data if the
        # simulation runs in only one stage. Also, the default is to
        # not continue if forcing data isn't found:
        alive = self.inpts.get("keepalive", False)
        alive = alive and len(self.ini) == 1
        
        ini = self.ini[runid]
        fin = self.fin[runid]
        fsrch = self.inpts.get("fsrch", [])

        if not fsrch:
            print("[WARNING] external forcing data not defined")
            return 0
        
        # Function inside the method to be reused ###########################
        #
        def searchfiles(fprfx: str) -> str:
            # Search for forcing data:
            files = glob(fprfx)
            mdate = []  # modification date
            dini = []  # data initial date
            dfin = []  # data final date
            
            if not files: return ""
            
            for file in files:
                # Data dates:
                ddates = path.splitext(path.basename(file))[0]
                ddates = ddates.split("-")[-1].split("_")
                dini.append(datetime.strptime(ddates[0], "%Y%m%d"))
                dfin.append(datetime.strptime(ddates[1], "%Y%m%d"))

                mdate.append(datetime.fromtimestamp(path.getmtime(file)))

            # Build a pandas.DataFrame to filter files:
            dfin = pd.DataFrame({
                "file": files, "mdate": mdate,
                "ini": dini, "fin": dfin,
            })

            # Sort file from newest to oldest:
            dfin.sort_values(by='mdate', inplace=True, ascending=False)
            
            # At this point the date columns are in datetime64[ns] format.
            # Filter files to desired range:
            dfin = dfin[(ini.date() >= dfin["ini"].dt.date)]
            dfin = dfin[(fin.date() <= dfin["fin"].dt.date)]
            dfin: pd.DataFrame
            
            # 'ini' and 'fin' are outside this function.
            # dt.date makes sure to compare only the date portion.

            if dfin.empty: return ""
            return dfin.reset_index(drop=True)["file"][0]
        #
        # ###################################################################
        
        for prfx in fsrch:
            print("Searching forcing data at", prfx)
            manbreak = False  # manual break
            
            while ini < fin and not manbreak:
                # The loop assumes 'fin' is always > 'ini' in the first
                # iteration of the first 'prfx'. This must be checked before,
                # as a simulation range can't be less than one day (chcksim).

                filein = searchfiles(prfx)

                # Stop looping if data is found or if isn't a
                # single stage simualtion (usually forecast):
                if filein or not alive:
                    manbreak = True
                    continue
                
                fin -= timedelta(1)
                self.fin = [fin,]
                print("[WARNING] final simulation date updated to", fin)

                # When 'fin' == 'ini', the loop ends and 'filein' is still
                # an empty string from the last iteration.
            
            if not filein:
                err = self.pterr + " forcing data not found"
                self.logentry(err + "\n", True)
                print(err)
                initmail("ERROR", err)
                return 1
            
            # Copy data to Boundary Conditions in General Data:
            fileout = path.splitext(path.basename(filein))
            fileout = fileout[0].split("-")[0] + fileout[1]
            fileout = path.join("Boundary Conditions", fileout)
            fileout = path.join(self.inpts.get("generaldata", ""), fileout)
            makedirs(path.dirname(fileout), exist_ok=True)
            copyfile(filein, fileout)
            
        return 0
    
    def initials(self, runid: int) -> int:
        """Searches for initial conditions data, saved in monthly files,
        for a single simulation stage, and copies them without the month
        suffix to the "Initial Conditions" folder in general data. The
        name of the monthly file must be 'prefix_MM.extension', where 'MM'
        is the two-digit month number (leading zero). This module should
        be used only by calling 'prepsim'.
        
        Keyword argument:
        - runid: simulation stage index, which is equivalent to the index of
        the model data files in MOHID, as the 2 in e.g. Hydrodynamic_2.dat.
        """

        if not self.inpts.get("initials", False):
            # Search for initial conditions disabled.
            return 0

        # 'runid' to match Python index (check method 'getforc'):
        ini = self.ini[runid - 1]

        files = self.inpts.get("generaldata", "")
        files = path.join(files, "Initial Conditions", "*_")
        files = glob(files + ini.strftime("%m.*"))

        if len(files) < 1:
            err = self.pterr + " inital conditions not found"
            self.logentry(err + "\n", True)
            print(err)
            initmail("ERROR", err)
            return 1
        
        for fipt in files:
            fout = path.splitext(fipt)
            # Remove month number and add extension:
            fout = fout[0].rsplit("_", 1)[0] + fout[1]
            copyfile(fipt, fout)

        return 0
    
    def continuous(self, runid: int) -> int:
        """Searches for the first stage of a simulation, initial conditions
        data generated in a previous simulation. Copies the files to the
        'res' folder of each domain, with the simulation index (runid) equal
        to zero, e.g. 'GOTM_0.fin'. This module should be used only by
        calling 'prepsim'.
        
        Keyword argument:
        - runid: simulation stage index, which is equivalent to the index of
        the model data files in MOHID, as the 2 in e.g. Hydrodynamic_2.dat.
        """

        if runid > 1: return 0
        
        opdate = self.ini[0]
        findirs = self.inpts.get("continuous", [])
        
        if not findirs:
            print("[WARNING] not a continuous simulation")
            return 0
        
        # Function inside the method to be reused ###########################
        #
        def itefiles(domdir: str) -> int:
            """Iterate the files inside a model domain.
            
            Keyword argument:
            - domdir: domain directory path.
            """
            
            iptdir = path.join(findir, opdate.strftime("%y%m%d"))
            iptdir = path.join(iptdir, path.basename(domdir))
            files = glob(path.join(iptdir, "*.fin*"))            

            # 'findir' and 'opdate' come from one level above,
            # from 'continuous' method.

            if not files:
                print("[WARNING] FIN files not found at", iptdir)
                return 1
            
            outdir = path.join(domdir, "res")
            makedirs(outdir, exist_ok=True)  # just to be sure

            for file in files:
                fout = path.splitext(path.basename(file))
                fout = fout[0].split("_", 1)[0] + "_0" + fout[1]
                fout = path.join(outdir, fout)
                print(fout)
                copyfile(file, fout)

            return 0
        #
        # ###################################################################

        # Function inside the method to be reused ###########################
        #
        def itedomains() -> int:
            """Iterate the model domains directories."""

            instatus = 0
            idom = 0
            domains = self.inpts.get("domains")
            
            while instatus == 0 and idom < len(domains):
                instatus = itefiles(domains[idom])
                idom += 1

            return instatus
        #
        # ###################################################################

        # Iterate directories:
        status = 1
        idir = 0

        while status == 1 and idir < len(findirs):
            findir = findirs[idir]  # to be used in itefiles.
            status = itedomains()
            idir += 1

        if status < 1: return 0

        err = self.pterr + " FIN files not found"
        self.logentry(err + "\n", True)
        print(err)
        initmail("ERROR", err)
        return 1
    
    def rmold(self, dirpath: str) -> None:
        """Remove items (file or folder) from a directory, keeping
        the number of items specified in the initialization file by
        the keyword 'keepres'.
        
        Keyword argument:
        - dirpath: path to the directory to be cleaned.
        """

        keepres = self.inpts.get("keepres", -99)
        if keepres <= 0: return
        
        vals = sorted(glob(path.join(dirpath, "*")), reverse=True)
        
        for val in vals[keepres:]:
            if path.isdir(val):
                rmtree(val)
            else:
                unlink(val)

    def copyouts(self) -> str:
        """Copies the simulation results to one folder inside the output
        directory defined in the initialization file, with one subfolder
        for each domain of the model. Returns the path to that folder.
        """
        
        # Inputs:
        rootout = self.inpts.get("outdir")   # checked in 'chksim'
        domains = self.inpts.get("domains")  # checked in 'chksim'
        lastfin = self.inpts.get("lastfins", True)
        ini = self.ini[0]
        fin = self.fin[-1]

        # HDFs and FINs directories:
        results = path.join(rootout, "results")
        finsdir = path.join(rootout, "fins")

        # Clean directories:
        for outdir in (results, finsdir): self.rmold(outdir)

        # HDFs folder:
        val = datetime.today().strftime("%Y%m%dT%H%M")
        results = path.join(results, val + ini.strftime("_%Y%m%d"))
        makedirs(results)

        # FINs folder:
        if lastfin:
            val = fin.strftime("%y%m%d")
        else:
            val = (ini + timedelta(1)).strftime("%y%m%d")

        finsdir = path.join(finsdir, val)
        if path.isdir(finsdir): rmtree(finsdir)
        makedirs(finsdir)  # finsdir is overwritten

        # Copy outputs to the directories:
        def cpouts(fipts: list, foutdir: str, ftype: bool) -> None:
            for fipt in fipts:
                fout = path.join(foutdir, path.basename(fipt))
                print("", fout)
                if ftype:
                    copyfile(fipt, fout)
                else:
                    copytree(fipt, fout)

        print("Copying outputs...")

        for dom in domains:
            iptdir = path.join(dom, "res")
            outdir = path.join(results, path.basename(dom))
            makedirs(outdir)
            
            # Copy HDFs:
            cpouts(glob(path.join(iptdir, "*.hdf5")), outdir, True)
            # Copy time series:
            cpouts(glob(path.join(iptdir, "Run*")), outdir, False)

            outdir = path.join(finsdir, path.basename(dom))
            makedirs(outdir)

            if lastfin:
                # Last simulation stage:
                fins = f"*_{len(self.ini)}.fin*"
            else:
                fins = (ini + timedelta(1)).strftime("*_%Y%m%d-%H%M%S.fin*")

            # Copy FIN files:
            cpouts(glob(path.join(iptdir, fins)), outdir, True)

        return results

    def database(self, iptdir: str) -> tuple:
        """Post-processing of the data from the last simulation stage,
        that is, it coordinates the extraction of instants from the
        hydrodynamics and water properties HDF5 files and then glues
        them together using the name of the respective domain as the
        prefix. Returns a tuple with the paths of the daily directories
        where the files were created.
        
        Keyword argument:
        - iptdir: input directory, to where the method 'copyouts'
        copied the simulations results, divided by domains.
        """

        # Inputs:
        rootout = path.join(self.inpts.get("outdir"), "database")
        domains = self.inpts.get("domains")
        
        if self.inpts.get("level0", False):
            # First level is for tides only.
            domains = domains[1:]

        # Remove old data:
        self.rmold(rootout)

        # Temporary folder:
        tmpdir = path.join(rootout, "tmp")
        if path.isdir(tmpdir): rmtree(tmpdir)
        makedirs(tmpdir)

        # Output daily directories:
        control = self.ini[-1]  # last stage initial date
        outdirs = []

        while control < self.fin[-1]:
            outdirs.append(path.join(rootout, control.strftime("%y%m%d")))
            control += timedelta(1)

        for outdir in outdirs:
            # Clean output dirs:
            if path.isdir(outdir): rmtree(outdir)

        # Iterate domains:
        idom = 0
        status = 0
        stage = len(self.ini)  # last stage id

        while status == 0 and idom < len(domains):
            domprfx = path.basename(domains[idom])
            
            # Extract hydrodynamic (HD) file:
            hdfin = path.join(iptdir, domprfx, f"Hydrodynamic_{stage}.hdf5")
            status = extract("mohid\\extractor", hdfin, tmpdir, "HD")
            if status > 0: continue

            # Extract water properties (WP) file:
            hdfin = path.join(iptdir, domprfx)
            hdfin = path.join(hdfin, f"WaterProperties_{stage}.hdf5")
            status = extract("mohid\\extractor", hdfin, tmpdir, "WP")
            if status > 0: continue
    
            # Merge HDF5 files:
            status = gluehdfs(
                "mohid\\merger", tmpdir, "HD", "WP", domprfx, False,
            )
            if status > 0: continue

            # Copy files to daily folders:
            print("Copying to database...")
            hdfs = sorted(glob(path.join(tmpdir, domprfx + "*.hdf5")))
            
            strdate = "%Y%m%dT%H%M.hdf5"
            step = datetime.strptime(hdfs[1].split("-")[-1], strdate)
            step-= datetime.strptime(hdfs[0].split("-")[-1], strdate)
            step = step.total_seconds()

            fpday = int((24*3600)/step)  # files/day

            for iday in range(len(outdirs)):
                outdir = outdirs[iday]
                makedirs(outdir, exist_ok=True)  # all domains in same dir

                for hdf in hdfs[iday*fpday:(iday+1)*fpday]:
                    fout = path.join(outdir, path.basename(hdf))
                    print("", fout)
                    copyfile(hdf, fout)

            idom += 1  # loop control variable

        if status < 1:
            # Post-processing success.
            rmtree(tmpdir)
            return tuple(outdirs)
        
        err = self.pterr + " database pos-processing failed"
        self.logentry(err + "\n", False)
        print(err)
        initmail("ERROR", err)
        return ()
    
    def hdftonc(self, dailydirs: tuple) -> tuple:
        """Coordinates the conversion of data obtained in the 'database'
        method to the netCDF format. Returns a tuple with a status code
        and another tuple with the paths of the folders containing the
        output netCDFs. If conversion fails, returns an empty tuple.
        
        Keyword arguments:
        - dailydirs: tuple with the paths of the folders containing the
        HDF5 generated by the 'database' method.
        """
        
        # Inputs:
        fjson = self.inpts.get("convncjson", "")
        
        if not path.isfile(fjson):
            # Conversion is set if file exists:
            return (0, ())
        
        rootout = path.join(self.inpts.get("outdir"), "netcdf")
        outdirs = [path.basename(val) for val in dailydirs]
        outdirs = [path.join(rootout, val) for val in outdirs]
        
        # Remove old data:
        self.rmold(rootout)
          
        # Loop directories
        idir = 0
        status = 0
        print("HDF5 conversion to netCDF...")

        while status == 0 and idir < len(outdirs):
            # Glob all respective HDFs:
            hdfs = glob(path.join(dailydirs[idir], "*.hdf5"))
            
            # Output directory:
            outdir = outdirs[idir]
            if path.isdir(outdir): rmtree(outdir)
            makedirs(outdir)
            
            # Iterate HDFs:
            ihdf = 0

            while status == 0 and ihdf < len(hdfs):
                print("", hdfs[ihdf])
                fout = path.splitext(path.basename(hdfs[ihdf]))[0]
                fout = path.join(outdir, fout + ".nc")
                status = hdftonc(hdfs[ihdf], fout, fjson, False)
                ihdf += 1  # loop control var
            
            idir += 1  # Loop control var

        if status < 1: return (0, tuple(outdirs))

        # ERROR manageent:
        err = self.pterr + " netcdf pos-processing failed"
        self.logentry(err + ".", False)
        print(err)
        initmail("ERROR", err)
        return (1, ())
    
    def cpnas(self, dailydirs: tuple) -> int:
        """Copies the files obtained with the 'database' method
        to a local or network external disk.
        
        Keyword arguments:
        - dailydirs: tuple with the paths of the folders containing the
        HDF5 generated by the 'database' method.
        """

        # Check if it is to move daily results to nas:
        hdpaths = self.inpts.get("extdisk", [])
        if not hdpaths: return 0

        # Loop daily directories:
        print("Copying files to external disk...")
        status = 0
        iday = 0

        while status == 0 and iday < len(dailydirs):
            dirdate = path.basename(dailydirs[iday])
            dirdate = datetime.strptime(dirdate, "%y%m%d")
            rootout = dirdate.strftime("%Y\\%B\\%y%m%d")
            
            # Loop external disks:
            ihdd = 0
            
            while status == 0 and ihdd < len(hdpaths):
                outdir = path.join(hdpaths[ihdd], rootout)
                
                try:
                    if path.isdir(outdir): rmtree(outdir)
                    makedirs(path.dirname(outdir), exist_ok=True)
                    copytree(dailydirs[iday], outdir)
                except Exception as err:
                    print(err)
                    status = 1
                    continue
            
                # Loop control variable:
                ihdd += 1
            # Loop control variable:    
            iday += 1

        if status < 1: return 0
        # ERROR manageent:
        err = self.pterr + " copy to external disk failed"
        self.logentry(err + ".", False)
        print(err)
        initmail("ERROR", err)
        return 1
