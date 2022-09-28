# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-09-14
#
# Updated : 2022-04-22


from datetime import datetime, timedelta
from os import path, mkdir, unlink, rename
from glob import glob
from shutil import move, rmtree, copyfile, copytree
from subprocess import run

import requests
import xarray as xr
import numpy as np
import pygrib

import support_xrdset
import forc_lib
from support_initkey import readkey


class ForcOp:
    def __init__(self, forc_inpts):
        """forc_inpts = the dictionary with the inputs read from init.dat"""

        self.smsc = forc_inpts.pop("smsc")
        self.ini = forc_inpts.pop("ini")
        self.modtype = forc_inpts.pop("modtype", "1")
        self.nlvls = forc_inpts.pop("levels", 1)
        self.grid = forc_inpts.pop("grid")
        self.batims = forc_inpts.pop("batims", "")
        self.geomt = forc_inpts.pop("geomt", "")
        
        self.today = datetime.today().date()
        self.mesg = "Module " + __name__ + " ERROR:"
        
        self.src = None
        self.root = None        
        self.fin = None
        self.fcth = None  # forecast in hours
    
    def updateattrs(self, src, fin):
        self.src = src
        self.root = self.smsc + "\\FORC\\" + src
        self.fin = fin
    
    def prodinfo(self):
        cmemsid = "CMEMS global-analysis-forecast-phy-001-024"
        prodid = {"Mercator": cmemsid,
                  "MercatorH": cmemsid + "-hourly-t-u-v-ssh",
                  "AMSEAS": "NOAA ncom amseas",
                  "NAM": "NOAA nam Caribbean/Central America",
                  "GFS": "NOAA gfs sflux"}
        
        print("PRODUCT INFO\n" + prodid.get(self.src), end="\n\n")

    def download_output(self):
        """Method for naming the merged output after the source download."""

        fout = {"Mercator": "\\Download\\Mercator.nc",
                "MercatorH": "\\Download\\Mercator.nc",
                "AMSEAS": "\\Conversion\\AMSEAS.nc",
                "NAM": "\\Conversion\\NAM.nc",
                "GFS": "\\Conversion\\GFS.nc"}
        return self.root + fout.get(self.src)
    
    def delforc(self, folder, exts, all=0):
        """folder = folder inside source root dir
           ext = str or iterable of the extensions to remove
           all = switch to remove the folder itself

           Removes all forcing data in a folder inside source root dir."""
        
        folder = self.root + "\\" + folder
        if all > 0:
            rmtree(folder)
            mkdir(folder)
            return

        if isinstance(exts, str):
            exts = exts.split()

        files = []
        for ext in exts:
            files += glob(folder + "\\*." + ext)
        [unlink(file) for file in files]

    def src_backup(self):
        # sources parameters:
        # max forecast days, skip backup, extension, keep hourly files untill
        parms = {"Mercator": [10, 1],
                 "MercatorH": [10, 1],
                 "AMSEAS": [4, 0, ".nc", 21],
                 "Skiron": [8, 1],
                 "NAM": [3, 0, ".grib2", 21],
                 "GFS": [16, 0, "grib2", 24]}        
        parms = parms.get(self.src)
        
        # check maximum forecast date:
        fin = self.fin
        maxfin = self.today + timedelta(parms.pop(0))
        if self.fin > maxfin:
            print("WARNING:", self.src, "maximum forecast date", str(maxfin))
            self.fin = maxfin
        # hours between ini and fin:
        self.fcth = ((self.fin - self.ini).days + 1)*24
            
        # skip backup?:
        if parms.pop(0) > 0:
            return
        print("Running data backup...")
            
        # create directory if does not exist:
        inptdir = self.root + "\\BKUP"  # files in
        if not path.isdir(inptdir):
            mkdir(inptdir)
            
        # get old forecast data:
        outdirs = []  # files out
        files = []
        
        ext = parms[0]
        keep = parms[1]
        
        for folder in glob(self.root + "\\Download\\*"):
            if path.isfile(folder):
                continue
            
            # leave current forecast folder in downloads:
            dirdate = datetime.strptime(path.basename(folder), "%y%m%d")
            if dirdate.toordinal() == self.today.toordinal():
                continue
                
            outdirs.append(folder)
            files += glob(folder + "\\*" + ext)
        
        # remove out-of-range files:
        for file in files:
            finst = path.splitext(path.basename(file))[0]  # file instant
            finst = int(finst.split('_')[-1])
            # all donwloaded files must be saved as 
            # smsc\\FORC\\src\\Download\\YYMMDD\\SRC_YYMMDD_HH.ext
            
            if finst > keep:
                unlink(file)
        
        # backup folders:
        for folder in outdirs:
            dirdate = datetime.strptime(path.basename(folder), "%y%m%d")
            
            # leave folders in range:
            if self.ini <= dirdate.date() <= fin:
                continue
                
            # remove if folder is already in backup:
            elif path.isdir(inptdir + "\\" + path.basename(folder)):
                rmtree(folder)
                continue
            
            # move out of range folder to backup:
            print(folder)
            move(folder, inptdir)
        print()

    def download(self):
        # sources parameters:
        parms = {"Mercator": ForcOp.downloadmercator,
                 "MercatorH": ForcOp.downloadmercatorh,
                 "AMSEAS": ForcOp.downloadamseas,
                 "Skiron": None,
                 "NAM": ForcOp.downloadnam,
                 "GFS": ForcOp.downloadgfs,}

        return parms.get(self.src)(self)
          
    def downloadmercator(self):  
        # output directory:
        outdir = self.root + "\\Download"
        if path.isfile(outdir + "\\Mercator.nc"):
            unlink(outdir + "\\Mercator.nc")
        
        # set download variables:
        cred = readkey(self.smsc, "CMEMS", "").split()
        if len(cred) != 2:
            print(self.mesg, 'wrong values for CMEMS credentials\n')
            return
        
        logs = outdir + '\\run.log', outdir + '\\run_error.log'

        # set command line and run download:
        cmd = 'python -m motuclient --motu https://nrt.cmems-du.eu/motu-web'
        cmd += '/Motu --service-id GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS '
        cmd += '--product-id global-analysis-forecast-phy-001-024 --longitude'
        cmd += f'-min {self.grid[2]} --longitude-max {self.grid[3]} --latitude'
        cmd += f'-min {self.grid[0]} --latitude-max {self.grid[1]} --date-min '
        cmd += f'"{str(self.ini)} 12:00:00" --date-max "{str(self.fin)} '
        cmd += '12:00:00" --depth-min 0.493 --depth-max 5727.918000000001 '
        cmd += '--variable so --variable thetao --variable uo --variable vo '
        cmd += f'--out-dir "{outdir}" --out-name "Mercator.nc" --user '
        cmd += f'"{cred[0]}" --pwd "{cred[1]}" > {logs[0]} 2> {logs[1]}'
        
        print('Downloading Mercator netCDF file...\n' + cmd, end="\n\n")
        run(cmd, shell=True)

        # check downloaded file:
        if not path.isfile(outdir + '\\Mercator.nc'):
            print(self.mesg, 'CMEMS download error\n')
            return

        # check error log:
        lines = [line.strip() for line in open(logs[1], "r")]
        if lines:
            print(self.mesg, 'CMEMS download error\n')
            return

        # check datasets size:
        dset = xr.open_dataset(outdir + '\\Mercator.nc')['time'].data
        if len(dset) != int((self.fin - self.ini).days + 1):
            print(self.mesg, 'CMEMS download error\n')
            return
        
        return 1

    def downloadmercatorh(self):  
        # output directory:
        outdir = self.root + "\\Download"
        if path.isfile(outdir + "\\Mercator.nc"):
            unlink(outdir + "\\Mercator.nc")
        
        # set download variables:
        cred = readkey(self.smsc, "CMEMS", "").split()
        if len(cred) != 2:
            print(self.mesg, 'wrong values for CMEMS credentials\n')
            return
        
        logs = outdir + '\\run.log', outdir + '\\run_error.log'
        
        # set command line and run download:
        dep = 0.49402499198913574
        ini = datetime.fromordinal(self.ini.toordinal())
        ini = ini + timedelta(hours=23.5)
        fin = ini + timedelta(hours=self.fcth)

        cmd = "python -m motuclient --motu https://nrt.cmems-du.eu/motu-web/Mo"
        cmd += "tu --service-id GLOBAL_ANALYSIS_FORECAST_PHY_001_024-TDS --pro"
        cmd += "duct-id global-analysis-forecast-phy-001-024-hourly-t-u-v-ssh "
        cmd += f"--longitude-min {self.grid[2]} --longitude-max {self.grid[3]}"
        cmd += f" --latitude-min {self.grid[0]} --latitude-max {self.grid[1]} "
        cmd += f'--date-min "{str(ini)}" --date-max "{str(fin)}" --depth-min '
        cmd += f'{dep} --depth-max {dep} --variable thetao --variable uo '
        cmd += f'--variable vo --variable zos --out-dir "{outdir}" --out-name '
        cmd += f'"Mercator.nc" --user "{cred[0]}" --pwd "{cred[1]}" > '
        cmd += f'"{logs[0]}" 2> "{logs[1]}"'
        
        print('Downloading Mercator netCDF file...\n' + cmd, end="\n\n")
        run(cmd, shell=True)

        # check downloaded file:
        if not path.isfile(outdir + '\\Mercator.nc'):
            print(self.mesg, 'CMEMS download error\n')
            return

        # check error log:
        lines = [line.strip() for line in open(logs[1], "r")]
        if lines:
            print(self.mesg, 'CMEMS download error in log\n')
            return

        # check datasets size:
        dset = xr.open_dataset(outdir + '\\Mercator.nc')['time'].data
        ndsets = int((fin - ini).total_seconds()//3600) + 1
        if len(dset) != ndsets:
            print(self.mesg, 'CMEMS download error in datasets number\n')
            return
        
        return 1

    def ncmerge(self, ncsloc):
        print("Merging files...")
        # get the name of output file:
        fout = ForcOp.download_output(self)
        ncs = glob(self.root + f"\\{ncsloc}\\*.nc")
        xrdset = support_xrdset.xrmerge(ncs)
        xrdset.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(xrdset))

    def downloadamseas(self):
        print("Get NOMADS NCOM AMSEAS data")
        # Forecast in days:
        fct = int(self.fcth/24)
        # CHECK IF THIS IS ACTUALLY NEEDED
        
        # Iterate forecast days:
        for inst in range(fct + 1):
            # fct + 1 because range starts at 0 and last day must
            # be included, like in date + timedelta(n)
            # file date:
            fdate = self.ini + timedelta(inst)
            print("Download date:", fdate)
            
            # Get folder from backup folder:
            status = forc_lib.getbkup(fdate, self.root)

            # Download from web:
            if status > 0:
                nouts = 1 if fdate < self.today else min(4, fct)
                status = forc_lib.amseas(fdate, self.root, nouts)

            if status > 0:
                print(self.mesg, 'AMSEAS download error')
                return

        # merge netcdfs:
        ForcOp.amseasmerge(self)
        print()
        return 1
             
    def amseasmerge(self):
        # clean netCDFs in Conversion folder:
        ForcOp.delforc(self, "Conversion", "nc")
        
        # glob all netCDF files inside Download folder:
        ncs = []
        for folder in glob(self.root + "\\Download\\*"):
            ncs += glob(folder + "\\*.nc")
        ncs.sort()

        print("Cutting domains...")
        for ntc in ncs:
            print(ntc)
            xrdset = xr.open_dataset(ntc, use_cftime=True)
            
            # convert dimensions:
            support_xrdset.xrtime(xrdset["time"].data, xrdset)
            xrdset = support_xrdset.xrlat(xrdset["lat"].data, xrdset)
            xrdset = support_xrdset.xrlon(xrdset["lon"].data, xrdset, 180)
            support_xrdset.xrdep(xrdset["depth"].data, xrdset)

            # cut domain:
            args = "latitude", "longitude"
            xrdset = support_xrdset.xrcut(self.grid, xrdset, *args)

            # save in Conversion folder:
            fout = self.root + "\\Conversion\\" + path.basename(ntc)
            xrdset.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(xrdset))
        
        # merge files:
        ForcOp.ncmerge(self, "Conversion")

    def grib2tonc(self):
        # clean conversion folder:
        ForcOp.delforc(self, "Conversion", "nc")

        # grab all grib2 files:
        grbs = []
        for folder in glob(self.root + "\\Download\\*"):
            grbs += glob(folder + "\\*.grib2")

        # convertible variables (SHOULD BE A JSON OR SIMILAR):
        convars = {
            "NAM": {
                "t": (
                    {'typeOfLevel': 'surface', "stepType": "instant"},
                    lambda x: x - 273.15, "degC",
                    ),
                "sp": ({'typeOfLevel': 'surface', "stepType": "instant"},),
                "snfalb": (
                    {'typeOfLevel': 'surface', "stepType": "instant"},
                    ),
                "tp": ({'typeOfLevel': 'surface', "stepType": "accum"},),
                "tcc": (
                    {'typeOfLevel': 'atmosphereSingleLayer', "stepType": "instant"},
                    lambda x: x/100, "-",
                    ),
                "r2": (
                    {'typeOfLevel': 'heightAboveGround', 'level': 2},
                    lambda x: x/100, "-",
                    ),
                "u10": ({'typeOfLevel': 'heightAboveGround', 'level': 10},),
                "v10": ({'typeOfLevel': 'heightAboveGround', 'level': 10},)
            }
        }
        convars = convars.get(self.src)

        # atmospheric files dimensions:
        dims = "time", "latitude", "longitude"
        print("Converting GRIB2 to netCDF...")
        
        for grb in grbs:
            print(grb)
            # open empty dataset for new grb:
            xrdset = xr.Dataset({})

            for var in convars.keys():
                # variable params:
                vals = convars.get(var)
                # variable grib location:
                args = {'filter_by_keys': vals[0]}
                # open grib at given location:
                dset = xr.open_dataset(grb, engine='cfgrib',
                                       backend_kwargs=args)
                # get variable data and add one dimension (time):
                darr = np.array([dset[var].data])
                attrs = dset[var].attrs
                
                # change units:
                if len(vals) > 1:
                    darr = vals[1](darr)  # lambda function
                    attrs["GRIB_units"] = vals[2]
                    attrs["units"] = vals[2] 
                
                # update variable in xrdset:
                xrdset = xrdset.assign({var: (dims, darr, attrs)})

            # update dimensions in xrdset:
            support_xrdset.xrtime(dset["valid_time"].data, xrdset)
            xrdset = support_xrdset.xrlat(dset["latitude"].data, xrdset)
            xrdset = support_xrdset.xrlon(dset["longitude"].data, xrdset, 180)

            # file out:
            fout = self.root + "\\Conversion\\" + path.basename(grb)
            fout = path.splitext(fout)[0] + ".nc"
            xrdset.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(xrdset))

            # clean cfgrib aux file:
            idxs = glob(path.dirname(grb) + "\\*.idx")
            [unlink(idx) for idx in idxs]

        # merge netCDFs:
        fout = ForcOp.download_output(self)
        ncs = sorted(glob(self.root + "\\Conversion\\*.nc"))
        xrdset = support_xrdset.xrmerge(ncs)
        xrdset.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(xrdset))

    def downloadnam(self):
        print("Downloading NAM GRIB files...")
        # download directory:
        root = self.root + "\\Download"
        grid = list(self.grid[:2]) + list(np.array(self.grid[2:]) % 360)

        # url download:
        prefix = "https://nomads.ncep.noaa.gov/cgi-bin/"
        prefix += "filter_nam_crb.pl?file=nam.t00z.afwaca"
        sufix = f".tm00.grib2&subregion=&leftlon={grid[2]}&rightlon={grid[3]}"
        sufix += f"&toplat={grid[1]}&bottomlat={grid[0]}&dir=%2Fnam."
        
        # download missing folders:
        for inst in range(self.ini.toordinal(), self.fin.toordinal() + 1):            
            fdate = datetime.fromordinal(inst).date()  # file date
            outdir = fdate.strftime(root + "\\%y%m%d")  # output directory
            bkpdir = self.root + "\\BKUP\\" + path.basename(outdir)
            
            if path.isdir(outdir) or fdate > self.today:
                continue
            elif path.isdir(bkpdir):
                copytree(bkpdir, outdir)
                continue
        
            # set download range (number of output files):
            nouts = 22 if fdate < self.today else min(85, self.fcth + 1)
            nouts = 22 if nouts == 24 else nouts
            
            mkdir(outdir)

            for out in range(0, nouts, 3):
                # output file name:
                fname = outdir
                fname += fdate.strftime(f"\\NAM_%y%m%d_{out:03d}.grib2")
                print(path.basename(fname))
                
                url = prefix + f"{out:02d}" + sufix + fdate.strftime("%Y%m%d")
                
                # print(url)
                fweb = requests.get(url)  # file in web
                status = fweb.status_code
                if status != 200:
                    print(self.mesg, 'NAM download error', status)
                    rmtree(outdir)
                    return
                
                # get bytes from fweb and wrtite in fout:
                with open(fname, 'wb') as fout:
                    fout.write(fweb.content)
        
            if len(glob(outdir + "\\*.grib2")) < (nouts - 1)/3:
                print(self.mesg, 'NAM downloaded files missing')
                rmtree(outdir)
                return
            
        # conversion to netCDF:
        ForcOp.grib2tonc(self)
        print()
        return 1
        
    def downloadgfs(self):
        print("Get NOMADS GFS Sflux data")
        # Forecast in days:
        fct = int(self.fcth/24)
        # CHECK IF THIS IS ACTUALLY NEEDED
        
        # Iterate forecast days:
        for inst in range(fct + 1):
            # fct + 1 because range starts at 0 and last day must
            # be included, like in date + timedelta(n)
            # file date:
            fdate = self.ini + timedelta(inst)
            
            # Get folder from backup folder:
            status = forc_lib.getbkup(fdate, self.root)

            # Download from web:
            if status > 0:
                nouts = 24 if fdate < self.today else min(384, (fct*24))
                status = forc_lib.gfs(fdate, self.root, nouts)

            if status > 0:
                print(self.mesg, 'GFS data not found')
                return
            
        # This is just a fix, from this line until the end of the method
        # this script needs to be updated:
        
        # Initiate conversion to netCDF
        # Remove old files:
        outdir = self.root + "\\Conversion"
        if path.isdir(outdir): rmtree(outdir)
        mkdir(outdir)

        # List all files in download:
        grbs = []
        for folder in glob(self.root + "\\Download\\*"):
            grbs += glob(folder + "\\*.grib2")

        # Convertible variables:
        keys = (
            "var", "unit", "long_name", "GRIB_typeOfLevel",
            "GRIB_stepType", "GRIB_level", "conversion",
        )

        bands = {
            6: (
                "surface_air_pressure", "Pa", "Surface pressure",
                "surface", "instant", 0, lambda x: x,
            ),  
            33: (
                "air_temperature", "degC", "2 metre temperature",
                "heightAboveGround", "instant", 2, lambda x: x - 273.16,
            ),
            34: (
                "specific_humidity", "kg kg-1", "2 metre specific humidity",
                "heightAboveGround", "instant", 2, lambda x: x,
            ),
            39: (
                "eastward_wind", "m s-1", "10 metre U wind component",
                "heightAboveGround", "instant", 10, lambda x: x,
            ),
            40: (
                "northward_wind", "m s-1", "10 metre V wind component",
                "heightAboveGround", "instant", 10, lambda x: x,
            ),
            68: (
                "pwat", "kg m-2", "Precipitable water",
                "atmosphereSingleLayer", "instant", 0, lambda x: x,
            ),
            84: (
                "tcc", "-", "Total cloud cover",
                "convectiveCloudLayer", "instant", 0, lambda x: x/100,
            ),
            # 113: (
            #     "surface_albedo", "%", "Albedo",
            #     "surface", "avg", 0, lambda x: x,
            # ),
        }
        
        dims = ("time", "latitude", "longitude")
        
        # Iterate grib files:
        print("Converting GFS files to netCDF...")
        for grb_name in grbs:
            print(grb_name)
            grb = pygrib.open(grb_name)
            dsout = xr.Dataset()
            
            # Iterate variables:
            for msg in bands.keys():
                # Grib message attributes and data as np.ndarray:
                attrs = dict(zip(keys, bands.get(msg)))
                print(" /" + attrs.get("var"))
                
                data = grb.message(msg).data(
                    lat1=self.grid[0], lat2=self.grid[1],
                    lon1=self.grid[2]%360, lon2=self.grid[3]%360,
                )[0]

                # Update unit and add one dimension to data (time):
                data = np.array([attrs.pop("conversion")(data)])
                # Update output xr.Dataset:
                dsout.update({attrs.pop("var"): (dims, data, attrs)})

            # Get dimensions information from last GRIB message:
            band = grb.message(msg)
            data, lat, lon = band.data(
                lat1=self.grid[0], lat2=self.grid[1],
                lon1=self.grid[2]%360, lon2=self.grid[3]%360,
            )
            
            print(" /time")
            support_xrdset.xrtime([band.validDate,], dsout)
            print(" /latitude")
            dsout = support_xrdset.xrlat(lat.transpose()[0], dsout)
            print(" /longitude")
            dsout = support_xrdset.xrlon(lon[0], dsout, 180)

            grb.close()

            # Output file
            fout = path.splitext(path.basename(grb_name))[0] + ".nc"
            fout = path.join(outdir, fout)
            dsout.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(dsout))

        # merge netCDFs:
        fout = ForcOp.download_output(self)
        ncs = sorted(glob(self.root + "\\Conversion\\*.nc"))
        xrdset = support_xrdset.xrmerge(ncs)
        xrdset.to_netcdf(fout, encoding=support_xrdset.xrencode_simple(xrdset))

        print()
        return 1
        
    def save_download(self):
        dwnld = ForcOp.download_output(self)
        
        if not path.isfile(dwnld):
            return
             
        fout = self.root + "\\Data\\" + self.src + self.ini.strftime("_%Y%m%d")
        fout += self.fin.strftime("_%Y%m%d") + path.splitext(dwnld)[1]
        
        copyfile(dwnld, fout)

    def tsconv(self, tsloc, opdate):
        print("Time series conversion...")
        finp = ForcOp.download_output(self)
        fout = self.root + "\\Data\\" + self.src + opdate.strftime("_%y%m%d")
        fout += self.ini.strftime("_%y%m%d") + self.fin.strftime("_%y%m%d.dat")

        # next: if file_in == hdf5 do oneway else nc do otherway
        # for now, do it only for netCDF:

        # file dataset:
        ds = xr.open_dataset(finp, use_cftime=True)
        # dataframe located at tsloc:
        df = (ds[dict(latitude=tsloc[0], longitude=tsloc[1])]).to_dataframe()
        # define DS column with delta time in seconds:
        df["DS"] = (df.index - df.index[0]).total_seconds().astype("i4")
        # inital time in MOHID string format:
        dfini = df.index[0].strftime("%Y %m %d %H %M %S\n")
        dfini = "SERIE_INITIAL_DATA : " + dfini
        
        # get cell lat, lon info and remove from dataframe:
        lat = "! CELL LATITUDE    : " + str(df["latitude"][0]) + "\n"
        lon = "! CELL LONGITUDE   : " + str(df["longitude"][0]) + "\n"
        df.drop(columns=["latitude", "longitude"], inplace=True)

        # get variables list and sort alphabetical with "DS" first:
        varlist = ["DS",] + sorted(list((df.columns).drop("DS")))
        # sort dataframe columns:
        df = df[varlist]
        # change list to string:
        varlist = " ".join(varlist)
        # last time number of digits:
        ndigits = len(str(df["DS"][-1]))

        # write time series:
        with open(fout, "w") as dat:
            dat.write("TIME_UNITS         : SECONDS\n")
            dat.write(dfini + lat + lon)
            dat.write(varlist + "\n<BeginTimeSerie>\n")
    
            for row in range(df.shape[0]):
                line = df.iloc[row].tolist()
                dstime = int(line.pop(0))
                dstime = '{num:0{width}} '.format(num=dstime, width=ndigits)
                line = [f"{val:.5f}" for val in line]
                dat.write(dstime + " ".join(line) + "\n")
    
            dat.write("<EndTimeSerie>\n")
