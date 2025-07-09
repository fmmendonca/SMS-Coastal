# ###########################################################################
#
# File    : m_forc_operations.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Mar. 1st, 2023.
#
# Updated : May 17th, 2024.
#
# Descrp. : Contains the processes that can be used in any data source.
#
# ###########################################################################

from datetime import datetime, timedelta
from glob import glob
from os import makedirs, path, unlink
from shutil import copyfile, copytree, move, rmtree
from time import sleep
from tqdm import tqdm
from typing import Sequence

import requests
import numpy as np
import xarray as xr
from h5py import File

from m_data_grbtonc import grbtonc
from m_supp_mailing import initmail
from m_supp_mohid import convert2hdf5
from m_supp_xarray import mergencs


class ForcOps:
    def __init__(self, srcid: str, prms: dict, srcprms: dict) -> None:
        """The class containing operations that can be used with forcing
        data from external providers, such as conversion and interpolation.
        
        Keyword arguments:
        - srcid: name of the data sorce;
        - prms: general parameters, such as dates and grid limits,
        checked in m_forc_manager.py;
        - srcprms: specific parameters for working with a particular
        data source.
        """

        self.srcid = srcid
        self.prms = prms.copy()
        self.src = srcprms.copy()
        self.pterr = "[ERROR] m_forc_operations: " + self.srcid + " "

        # Used 'copy' to create new dictionaries that can be
        # manipulated in this class at will.

        # 'prms' is already checked before the class is started.
    
    def waitdata(self) -> None:
        """Waits the forecast data to be available, according to the
        start time specified by the user in the initialization file.
        """

        start = self.src.pop("start", 0)

        if not isinstance(start, (int, float)) or start < 0:
            print("[WARNING] invalid start time in", self.srcid)
            return
        
        # The program will wait only if the dates range contains
        # contains forecasted dates:
        fin = self.prms.get("drange")[1]
        fin: datetime

        if fin.date() < datetime.today().date():
            return
        
        # The waiting time is the difference between now
        # and today's date at 00:00 plus start:
        startime = datetime.fromordinal(datetime.today().toordinal())
        startime+= timedelta(hours=start)
        timewait = (startime - datetime.today()).total_seconds() + 1

        if timewait < 0:
            return
        
        print("Waiting dada availability...")
        sleep(timewait)

    def logentry(self, entry: str, addate: bool) -> None:
        """Adds an entry to the execution log, which is created
        in the source root directory.

        Keyword argument:
        - entry: text to write in the simulation log file;
        - addate: adds the date and time before the entry.
        """

        logfile = path.join(self.src.get("root"), "smslog.dat")
        
        if not path.isfile(logfile):
            dat = open(logfile, "w")
            dat.write("runtime;dates;endtime;status\n")
        else:
            dat = open(logfile, "a")

        if addate:
            dat.write(datetime.today().isoformat(timespec="seconds") + ";")

        dat.write(entry)
        dat.close()

    def chkroot(self) -> int:
        """Checks source root directory and opens the log file."""
        
        root = self.src.get("root", "")

        if not(isinstance(root, str) and path.isdir(root)):
            # To avoid path problems the code doesn't
            # create the rot directory
            err = self.pterr + "invalid/missing root directory"
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1

        drange = self.prms.get("drange")
        drange: Sequence[datetime]
        
        entry = drange[0].strftime("%Y%m%d->")
        entry+= drange[1].strftime("%Y%m%d;")
        self.logentry(entry, True)
        return 0
    
    def rmold(self, itemspath: str) -> None:
        """Remove items (file or folder) from a directory, keeping
        the number of items specified in the initialization file by
        the keyword 'keepold'.
        
        Keyword argument:
        - itemspath: generic path to the itens to be cleaned.
        """

        keep = self.prms.get("keepold", -99)
        if keep <= 0: return
        
        # Sort itens by creation/modificationd date from newest to oldest:
        vals = glob(itemspath)
        dates = [path.getctime(val) for val in vals]
        vals = [pair[1] for pair in sorted(zip(dates, vals), reverse=True)]
        
        # Remove old files:
        for val in vals[keep:]:
            if path.isdir(val):
                rmtree(val)
            else:
                unlink(val)

    def cpnas(self, iptdir: str) -> int:
        """Moves daily folders to a local or network external disk.
        When moving a folder, it is overwritten if it already exists
        on the destination.
        
        Keyword arguments:
        - iptdir: path to a directory containing daily folders named
        in the format YYMMDD.
        """

        # Inputs:
        keep = self.prms.get("keepold", -99)
        hdpaths = self.src.get("extdisk", [])
        folders = sorted(glob(path.join(iptdir, "*")), reverse=True)

        # Check the need to move:
        if keep < 0:
            # Keep all folders.
            return 0
        
        # Select folders to be copied or removed:
        folders = folders[keep:]
        
        if not hdpaths:
            # Just clean directory.
            for folder in folders:
                if path.isfile(folder):
                    unlink(folder)
                else:
                    rmtree(folder)
            return 0
        
        # Sort dailydirs from newest to oldest:
        print("Movind data to external disk...")
        status = 0
        iday = 0

        while status == 0 and iday < len(folders):
            # Check folder:
            folder = folders[iday]

            if path.isfile(folder):
                iday += 1
                continue

            # Date of the files:
            dirdate = datetime.strptime(path.basename(folder), "%y%m%d")
            
            # Standard path for output:
            rootout = dirdate.strftime("%Y\\%B\\%y%m%d")
            
            # Loop external disks:
            ihdd = 0
            
            while status == 0 and ihdd < len(hdpaths):
                outdir = path.join(hdpaths[ihdd], rootout)
                print("", outdir)
                
                try:
                    # Overwrite data:
                    if path.isdir(outdir): rmtree(outdir)
                    makedirs(path.dirname(outdir), exist_ok=True)
                    copytree(folder, outdir)
                except Exception as err:
                    print(err)
                    status = 1
                    continue
            
                # Loop control variable:
                ihdd += 1
            # Loop control variable:    
            iday += 1

        if status < 1:
            # Clean directory.
            for folder in folders:
                if path.isfile(folder):
                    unlink(folder)
                else:
                    rmtree(folder)
            return 0
        
        # ERROR manageent:
        err = self.pterr + "copy to external disk failed"
        self.logentry(err + "\n", True)
        print(err)
        initmail(self.srcid + " ERROR", err)
        return 1

    def mohid_nctohdf5(self, buildbatim: bool) -> int:
        """Coordinates the execution of Convert2Hdf.exe tool
        for netCDF conversion to HDF5.

        Keyword argument:
        - buildbatim: switch to build a bathymetry file from the
        land-sea mask property insed te converted HDF5 file. This
        field should be in the group /Result/Bathymetry of the
        HDF5 file. WARNING: land cells should be equal to 1, and
        sea equal to 0.
        """
        
        # Conversion inputs:
        outdir = path.join(self.src.get("root"), "conversion")
        mohidir = ".\\mohid\\convert2hdf5"
        inpts = self.src.get("cfconv", {})
        inpts: dict
        makedirs(outdir, exist_ok=True)
        
        if not inpts:
            # MOHID conversion disabled.
            return 0

        mdat = inpts.get("mdat", "")
        fout = inpts.get("fout", "")

        if not path.isfile(mdat):
            err = self.pterr + "missing MOHID cfconv input file"
            self.logentry(err + "\n", True)
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        status = convert2hdf5(mohidir, mdat, outdir)
        
        if status > 0:
            # Error managment:
            err = self.pterr + "MOHID cfconv failed"
            self.logentry(err + "\n", True)
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1

        if not path.isfile(fout):
            # Error managment:
            err = self.pterr + "MOHID cfconv failed, HDF not found: " + fout
            self.logentry(err + "\n", True)
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        # Build bathymetry file in the same directory of the output file:
        #
        if not buildbatim: return 0

        # Read grid from output file:
        hdf = File(fout, "r")
        lat = hdf["/Grid/Latitude"][...][0]
        lon = hdf["/Grid/Longitude"][...].transpose()[0]

        if "bathymetry" not in hdf["/Results"].keys():
            # Error managment:
            hdf.close()
            err = self.pterr + "MOHID cfconv failed, "
            err+= "land-sea mask field not found"
            self.logentry(err + "\n", True)
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        # Get land-sea mask dataset:
        batim = hdf["/Results/bathymetry/bathymetry_00001"][...]
        batim = batim.astype("i2")
        # from land=1, sea=0 to land=-99, sea=10.000:
        batim = (batim - 1) * (-10000) + batim * (-99)

        # Build bathymetry string:
        hdf.close()
        
        # Helping function:
        def formatval(val) -> str:
            if isinstance(val, int):
                fmt = f" {float(val):0<14}\n"
            else:
                fmt = f" {val:0<14}\n"
            return fmt
        
        batimstr = f"ILB_IUB    : 1  {len(lat)-1}\n"
        batimstr+= f"JLB_JUB    : 1  {len(lon)-1}\n"
        batimstr+= "COORD_TIP  : 4\nORIGIN     : 0.0  0.0\n"
        batimstr+= "GRID_ANGLE : 0.0\nFILL_VALUE : -99.0\n\n<BeginXX>\n"

        for val in lon: batimstr+=formatval(val)
        batimstr+= "<EndXX>\n\n<BeginYY>\n"
        for val in lat: batimstr+=formatval(val)
        batimstr+= "<EndYY>\n\n<BeginGridData2D>\n"
        for val in np.transpose(batim).flatten(): batimstr+=formatval(val)
        batimstr+= "<EndGridData2D>\n"
        
        # Write bathymetry file:
        fout = path.join(path.dirname(fout), self.srcid + "_batim.dat")
        with open(fout, 'w') as dat:
            dat.write(batimstr)

        return 0
    
    def mohid_convtomodel(self) -> int:
        """Coordinates the execution of Convert2Hdf.exe tool
        for HDF5 interpolation to model bathymetry.
        """
        
        # Interpolation inputs:
        outdir = path.join(self.src.get("root"), "interpolation")
        simdata = path.join(self.src.get("root"), "simdata")
        mohidir = ".\\mohid\\convert2hdf5"
        prms = self.src.pop("tomodel", {})
        
        drange = self.prms.get("drange")
        drange: Sequence[datetime]
        
        if not(isinstance(prms, dict) and prms):
            # MOHID interpolation disabled.
            return 0
        
        makedirs(outdir, exist_ok=True)
        makedirs(simdata, exist_ok=True)

        # Loop interpolation processes:
        status = 0
        iproc = 1  # process id
        intprms = prms.get(str(iproc))

        while status == 0 and intprms != {}:
            mdat = intprms.get("mdat")
            fout = intprms.get("fout")
            batim = intprms.get("batim", "")
            newbatim = path.splitext(batim)[0] + "_v01.dat"
        
            status = convert2hdf5(mohidir, mdat, outdir)

            if status > 0 and path.isfile(batim) and path.isfile(newbatim):
                # MOHID sometimes fixes the bathymetry file,
                # so try the interpolation one more time.
                move(newbatim, batim)
                status = convert2hdf5(mohidir, mdat, outdir)
            
            if status > 0: continue
            
            # Save in simulation folder and remove old data:
            prfx, sufx= path.splitext(path.basename(fout))
            self.rmold(path.join(simdata, prfx + "*" + sufx))
            
            fsim = prfx + drange[0].strftime("-%Y%m%d_")
            fsim+= drange[1].strftime("%Y%m%d") + sufx
            fsim = path.join(simdata, fsim)

            copyfile(fout, fsim)

            # Loop control variables:
            iproc += 1
            intprms = prms.get(str(iproc), {})
        
        if status < 1: return 0

        # Error managment:
        err = self.pterr + "MOHID interpolation to model grid failed"
        print(err)
        self.logentry(err + "\n", True)
        initmail(self.srcid + " ERROR", err)
        return 1
    
    def grbtonc(self, grbs: Sequence[str]) -> int:
        """Coordinates the conversion of all GRIB files
        contained in the download folder to the netCDF format.
        
        Keyword argument:
        - grbext: grib file extension.
        """
        
        if not grbs:
            err = self.pterr + "empty list of grib files."
            print(err)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        # Check conversion file:
        prmsfile = self.src.pop("grbtonc", "")
        
        if not path.isfile(prmsfile):
            err = self.pterr + "missing GRIB parameters file: " + prmsfile
            print(err)
            self.logentry(err + "\n", True)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        # Output directory:
        print("Conversion from GRIB to netCDF...")
        outdir = path.join(self.src.get("root"), "conversion")
        makedirs(outdir, exist_ok=True)
        for ntc in glob(path.join(outdir, "*.nc")): unlink(ntc)
        
        for grb in tqdm(grbs):
            fout = path.splitext(path.basename(grb))[0] + ".nc"
            fout = path.join(outdir, fout)
            grbtonc(grb, fout, self.prms.get("grid"), prmsfile)

        # Merge all netCDFs:
        ncs = sorted(glob(path.join(outdir, "*.nc")))
        fout = path.join(outdir, self.srcid + ".nc")
        mergencs(ncs, fout)
        return 0
    
    def nctots(self, ntcin:str) -> int:
        """Conversion of a netCDf file to a ASCII time series file,
        using MOHID format.

        Keyword argument:
        - ntcin: name and path of the input netCDF file.
        """

        tsloc = self.src.get("tsloc", [])
        if not tsloc: return 0

        # Time series location:
        if not isinstance(tsloc, list):
            err = "missing/invalid time series conversion inputs"
            err = self.pterr + err
            print(err)
            self.logentry(err + "\n", True)
            initmail(self.srcid + " ERROR", err)
            return 1
        
        # Output directory and input dataset:
        outdir = path.join(self.src.get("root"), "conversion")
        makedirs(outdir, exist_ok=True)

        print("Conversion to time series...")
        dset = xr.open_dataset(ntcin, use_cftime=True)
        
        for pos in tsloc:
            if not isinstance(pos, list) or len(pos) != 2:
                err = "missing/invalid time series conversion inputs"
                err = self.pterr + err
                print(err)
                self.logentry(err + "\n", True)
                initmail(self.srcid + " ERROR", err)
                return 1
            
            fout = self.srcid + f"_ts{pos[0]:03d}x{pos[1]:03d}.dat"
            fout = path.join(outdir, fout)
            
            df = dset.isel(latitude=pos[0]-1, longitude=pos[1]-1)
            df = df.to_dataframe()

            # pos-1 as the index start at 0 in python.

            # Define DS column with delta time in seconds:
            dtime = (df.index - df.index[0]).total_seconds()
            df["DS"] = dtime.to_numpy().astype("i4")

            # Inital time in MOHID string format:
            tsdata = df.index[0].strftime("%Y %m %d %H %M %S\n")
            tsdata = "SERIE_INITIAL_DATA : " + tsdata
        
            # Get cell lat, lon info and remove from dataframe:
            tsdata += "! CELL LATITUDE    : "
            tsdata += str(df["latitude"].iloc[0]) + "\n"
            tsdata += "! CELL LONGITUDE   : "
            tsdata += str(df["longitude"].iloc[0]) + "\n"
            df.drop(columns=["latitude", "longitude"], inplace=True)

            # Variables list sorted alphabetically, but 'DS' 1st:
            varlist = ["DS",] + sorted(list((df.columns).drop("DS")))
            # Sort dataframe columns:
            df = df[varlist]

            # Nuber of digitis of last instant:
            ndigits = len(str(df["DS"].iloc[-1]))

            # Write time series:
            with open(fout, "w") as dat:
                dat.write("TIME_UNITS         : SECONDS\n")
                dat.write(tsdata)
                dat.write(" ".join(varlist) + "\n<BeginTimeSerie>\n")
        
                for row in range(df.shape[0]):
                    linein = df.iloc[row].tolist()
                    lineout = []

                    # When a row is extracted all values turn to float,
                    # so to extract DS from first position:
                    dstime = int(linein.pop(0))

                    lineout.append(f"{dstime:0{ndigits}d}")
                    lineout += [f"{val:.5f}" for val in linein]

                    dat.write(" ".join(lineout) + "\n")
        
                dat.write("<EndTimeSerie>\n")
        
            # Save in simulation folder and remove old data:
            prfx, sufx = path.splitext(path.basename(fout))
            simdata = path.join(self.src.get("root"), "simdata")
            drange = self.prms.get("drange")
            
            self.rmold(path.join(simdata, prfx + "*" + sufx))
            
            fsim = prfx + drange[0].strftime("-%Y%m%d_")
            fsim+= drange[1].strftime("%Y%m%d") + sufx
            fsim = path.join(simdata, fsim)

            makedirs(path.dirname(fsim), exist_ok=True)
            copyfile(fout, fsim)
        return 0

    def webrqst(self, url: str, fout: str) -> int:
        """Downloads file from a URL.
        
        Keyword arguments:
        - url: URL link;
        - fout: name and path of the output file.
        """
        
        # Error status:
        status = False

        # Download file:
        resp = requests.get(url)
        size = resp.headers.get("Content-Length")
        webstatus = resp.status_code

        # Check erros:
        if webstatus != 200:
            # Dowload error.
            status = True
        else:
            with open(fout, "wb") as dataout:
                dataout.write(resp.content)
            
            status = str(path.getsize(fout)) != size
            # sizes must be the same.

        if not status: return 0

        # Error management:
        err = self.pterr + "web request failed for: " + url
        print(err)
        self.logentry(err + "\n", True)
        initmail(self.srcid + " ERROR", err)
        return 1
    