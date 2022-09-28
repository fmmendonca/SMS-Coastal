# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2018
#
# Updated : 2022-01-31
#

from os import path, unlink, mkdir
from glob import glob
from datetime import timedelta, datetime
from shutil import rmtree
from time import sleep

import paramiko
import numpy as np
import xarray as xr
from cftime import num2pydate

from support_conv2nc import hdf2xrdset
from support_xrdset import xrencode, xrtime, xrencode_simple
from support_mohid import mergeintime, hdf2nc


def uploadsftp(finp, outdir, serv, user, pwrd):
    print("Uploading files to SFTP...")
    print("SERVER:", serv)
    print("OUTPUT DIRECTORY:", outdir)
    with paramiko.SSHClient() as client:
        #
        # set up connection:
        #
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(serv, username=user, password=pwrd)
        #
        # open SFTP:
        #
        sftp = client.open_sftp()
        #
        # create remote output directory:
        #
        try:
            sftp.mkdir(outdir)
        except OSError:
            print("WARNING: output directory already exists.")
        #
        # upload files and close connection:
        #
        for file in finp:
            print(file)
            sftp.put(file, outdir + "/" + path.basename(file))
            # overwrites files with same name
        sftp.close()


def convertpde(resdir, outdir, opdate):
    #
    # Create ouput directory and clean old netCDFs:
    #
    # #outdir = getcwd() + "\\Forecast\\Operations\\PDE"
    if not path.isdir(outdir):
        mkdir(outdir)
    [unlink(ntc) for ntc in glob(outdir + "\\*.nc")]
    #
    # PDE variables, dimensionsns:
    #
    pdevars = "uo", "vo", "wo", "thetao", "so", "rho"
    pdedims ={"time": {"CoordinateAxisType": "Time"},
              "latitude": {"CoordinateAxisType": "Lat"},
              "longitude": {"CoordinateAxisType": "Lon"},
              "depth": {"long_name": "Vertical distance below the surface",
              "CoordinateAxistype": "Height", "CoordinateZisPositive": "down"}
              }
    #
    # iterate each forcast level:
    #
    for level in range(2,4):
        # check file:
        finp = resdir + f"\\LV{level:02d}_Merged.hdf5"
        if not path.isfile(finp):
            print("WARNING: MOHID merged file not found at", str(finp))
            continue

        # Open HDF5 and convert to xarray:
        xrdset = hdf2xrdset(resdir + f"\\LV{level:02d}_Merged.hdf5")
        #
        # drop non-pde variables/fields:
        #
        for varid in xrdset.data_vars:
            if varid not in pdevars:
                xrdset = xrdset.drop_vars(varid)
                continue
        #
        # enconde variables:
        #
        encd = xrencode(xrdset, 8)
        #
        # update dimensions attributes:
        #     
        for varid in xrdset.dims:
            attrs = pdedims.get(varid)
            
            # upload step attribute for lat and lon:
            if varid in ("latitude", "longitude"):
                darr = xrdset[varid]
                attrs["step"] = np.array([darr[1] - darr[0]]).astype("f4")
                
            xrdset[varid].attrs.update(attrs)
        #
        # Ouput files prefix and sufix:
        #
        prfx = outdir + f"\\SOMA-L{level-1}_"
        sufx = opdate.strftime("-B%Y%m%d12-FC.nc")
        #
        # get time DataArray and its units:
        #
        darr = xrdset["time"]
        cale = darr.attrs.get("calendar")
        unit = darr.attrs.get("units")
        #
        # save 1 3D file for each forecasted day:
        # (level - 1) * 24 = ammount of outputs/day
        # range 3D is the ammount of days
        # 
        for inst in range(len(darr)//((level - 1) * 24)):
            # dates range to calculate mean:
            slce = np.array([inst, inst+1]).astype("i2")*24*(level-1)
            
            # output xarray (daily mean):
            xrout = xrdset.isel(time=slice(slce[0], slce[1]))
            xrout = xrout.mean(dim="time", keep_attrs=True)

            # initial time and date in dates range:
            inital = darr[slce[0]]

            # give back time dimension after mean operation:
            xrout = xrout.expand_dims("time", axis=0)
            xrout.update({"time": ("time", np.array([inital]), darr.attrs)})

            # output file name and write netCDF:
            fout = num2pydate(inital, unit, cale).strftime("dm-%Y%m%d%H")
            xrout.to_netcdf(prfx + fout + sufx, encoding=encd)
        #
        # save 1 2D for each forecasted hour:
        #
        rge = len(darr) - 1 if level == 2 else len(darr)
        #
        # rge is used to remove the last output of level 2.
        # e.g. 4-day forecast gives 97 hourly outputs in level 2, that is 97
        # output files. In level 3, 132 half hourly outputs gives 96 files
        # 
        # range 2D is the ammount of hours
        # rge = ammount of outputs; (level - 1) = amount of outputs/hour
        #
        for inst in range(rge//(level - 1)):
            # dates range to calculate mean:
            slce = np.array([inst, inst+1]).astype("i2")*(level-1)

            # output xarray (daily mean):
            xrout = xrdset.isel(time=slice(slce[0], slce[1]))
            xrout = xrout.mean(dim="time", keep_attrs=True)

            # initial time and date in dates range:
            inital = darr[slce[0]]

            # give back time dimension after mean operation:
            xrout = xrout.expand_dims("time", axis=0)
            xrout.update({"time": ("time", np.array([inital]), darr.attrs)})

            # output file name and write netCDF at surface:
            fout = num2pydate(inital, unit, cale).strftime("hv-%Y%m%d%H")
            xrout.isel(depth=0).to_netcdf(prfx + fout + sufx, encoding=encd)
    #
    # upload files after midnight
    # calculate sleep time:
    #    
    slpdt = datetime.fromordinal((opdate + timedelta(1)).toordinal())
    slpdt = int((slpdt - datetime.today()).total_seconds()) + 1
    sleep(slpdt if slpdt > 0 else 0)
    #
    # upload files to PDE SFTP:
    #
    ncs = glob(outdir + "\\*.nc")
    serv =  "ualg-ocaso.ualg.pt"
    user = "userftpocaso"
    pwrd = "iK7re8baYXpsEGLxjkWx"
    uploadsftp(ncs, opdate.strftime("/PDE/PDE_%y%m%d"), serv, user, pwrd)


def convertbasic(fmtdir, outdir, opdate):
    """fmtdir = string with path of the directory with database formated
       outputs
       outdir = string with path of the output directory
       opdate = datetime.date object with operation date"""
    #
    # Create ouput directory and clean old files:
    #
    print("BASIC FORECAST THREDDS CONVERSION MODULE")
    if path.isdir(outdir):
        rmtree(outdir)
    mkdir(outdir)
    print("INPUT DIRECTORY:", fmtdir)
    print("OUTPUT DIRECTORY:", outdir)
    #
    # hdf5 files:
    #
    hdfs = sorted(glob(fmtdir + "\\*.hdf5"))
    #
    # merge daily files:
    #
    print("Merging hourly outputs into daily files...")
    for inst in range(len(hdfs)//24):
        print("from", hdfs[inst*24].replace(fmtdir, ".\\"), end=" ")
        print("to", hdfs[(inst+1)*24-1].replace(fmtdir, ".\\"))
        fout = outdir + (opdate + timedelta(inst)).strftime("\\%Y%m%d00.hdf5")
        mergeintime(hdfs[inst*24:(inst+1)*24], fout)
    #
    # coversion to netCDF:
    #
    print("Converting daily files to netCDF...")
    hdfs = glob(outdir + "\\*.hdf5")
    for hdf in hdfs:
        print(hdf.replace(outdir, ".\\"))
        hdf2nc(hdf, path.splitext(hdf)[0] + ".nc")
    # #
    # # upload to SFTP:
    # #
    # ncs = glob(outdir + "\\*.nc")
    # serv =  "193.136.227.143"
    # user = "basic_admin"
    # pwrd = "basic2019_2021"
    # ondir = "/storage/thredds/MOHID_WATER/CARTAGENA_100M_22L_1H/"
    # uploadsftp(ncs, ondir, serv, user, pwrd)


def basicattrs():
    """Returns the variables and respective attributes for BASIC thredds"""
    
    # keys for the values: "var", "long_name", "standard_name", "valid_max", 
    # "valid_min", "units", "coordinates"
    
    var = "precipitation"
    valid = np.array([10, 0]).astype('f4')
    vals = var, var, var, valid[0], valid[1] , "kg m-2"
    attrs = {"tp": (*vals, "lat lon")}
    attrs["pwat"] = *vals, "lat lon"
    
    var = "air_pressure_at_mean_sea_level"
    valid = np.array([110000, 85000]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "Pa"
    attrs["sp"] = *vals, "lat lon"
    attrs["surface_air_pressure"] = *vals, "lat lon"
    
    var = "relative_humidity"
    valid = np.array([0, 1]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "1"
    attrs["r2"] = *vals, "lat lon"
    
    var = "cloud_cover"
    valid = np.array([0, 1]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "1"
    attrs['tcc'] = *vals, "lat lon"

    var = "air_temperature"
    valid = np.array([60, -90]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "degC"
    attrs['t'] = *vals, "lat lon"
    attrs["air_temperature"] = *vals, "lat lon"
    
    var = "x_wind"
    valid = np.array([100, -100]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "m s-1"
    attrs["u10"] = *vals, "lat lon"
    attrs["eastward_wind"] = *vals, "lat lon"
    
    var = "y_wind"
    valid = np.array([100, -100]).astype('f4')
    vals = var, var.replace("_", " "), var, valid[0], valid[1], "m s-1"
    attrs["v10"] = *vals, "lat lon"
    attrs["northward_wind"] = *vals, "lat lon"
    
    var = "sea_water_salinity"
    valid = np.array([50, 20]).astype('f4')
    vals = "salinity", var.replace("_", " "), var, valid[0], valid[1], "psu"
    attrs["salinity"] = *vals, "lat lon"
    attrs["so"] = *vals, "lat lon"

    var = "sea_water_temperature"
    valid = np.array([60, -90]).astype('f4')
    vals = "temperature", var.replace("_", " "), var, valid[0], valid[1]
    attrs["water_temp"] = *vals, "degC", "lat lon"
    attrs["thetao"] = *vals, "degC", "lat lon"
    
    var = "eastward_sea_water_velocity"
    valid = np.array([100, -100]).astype('f4')
    vals = "u", var.replace("_", " "), var, valid[0], valid[1], "m s-1"
    attrs["water_u"] = *vals, "lat lon"
    attrs["uo"] = *vals, "lat lon"
    
    var = "northward_sea_water_velocity"
    valid = np.array([100, -100]).astype('f4')
    vals = "v", var.replace("_", " "), var, valid[0], valid[1], "m s-1"
    attrs["water_v"] = *vals, "lat lon"
    attrs["vo"] = *vals, "lat lon"
    
    var = "upward_sea_water_velocity"
    valid = np.array([100, -100]).astype('f4')
    vals = "w", var.replace("_", " "), var, valid[0], valid[1], "m s-1"
    attrs["water_w"] = *vals, "lat lon"
    
    return attrs


def convertbasicforc(forcdir):
    #
    # set output directory:
    #
    outdir = forcdir + "\\Thredds"
    
    if path.isdir(outdir):
        rmtree(outdir)
    mkdir(outdir)
    #
    # input file:
    #
    if "Mercator" in str(path.basename(forcdir)):
        ntc = forcdir + "\\Download\\Mercator.nc"
    else:
        ntc = forcdir + f"\\Conversion\\{path.basename(forcdir)}.nc"

    dset = xr.open_dataset(ntc, use_cftime=True)
    #
    # update datasets
    # time:
    #
    xrtime(dset["time"].data, dset)

    # update lat, lon attributes:
    ref = "geographical coordinates, WGS84 projection"
    dset["latitude"].attrs["reference"] = ref
    dset["longitude"].attrs["reference"] = ref

    # rename dims:
    dset = dset.rename_dims({"latitude": "lat", "longitude": "lon"})
    dset = dset.rename_vars({"latitude": "lat", "longitude": "lon"})
    #
    # define basic thredds attributes:
    #
    attrs = basicattrs()
    
    # attributes keys:
    keys = "var", "long_name", "standard_name", "valid_max", "valid_min"
    keys = *keys, "units", "coordinates", "lat lon"
    #
    # update variables in dataset
    #
    for var in dset.data_vars:
        # remove non-basic variables/fields:
        if var not in attrs.keys():
            dset = dset.drop_vars(var)
            continue
        
        # variable attribures:
        var_attrs = dict(zip(keys, attrs.get(var)))
        var_id = var_attrs.pop("var")

        # add maximum and minimum:
        var_attrs["maximum"] = dset[var].max().data.astype('f4')
        var_attrs["minimum"] = dset[var].min().data.astype('f4')
        
        # update attributes and variable name:
        dset[var].attrs = var_attrs
        dset = dset.rename_vars({var: var_id})

    del ref, attrs, keys
    #
    # sources number of outputs/day and online paths:
    #
    prfxhyd = "/storage/thredds/HYDRODYNAMICS/"
    prfxatm = "/storage/thredds/METEO/"

    parms = {"NAM": (8, prfxatm + "NAM/NAM_0.12DEG_1L_3h"),
             "GFS": (24, prfxatm + "GFS/GFS_0.13DEG_1L_1H"),
             "AMSEAS": (8, prfxhyd + "AMSEAS/AMSEAS_0.03DEG_40L_3H"),
             "Mercator": (1, prfxhyd + "MERCATOR/MERCATOR_0.08DEG_50L_1D"),
             "MercatorH": (24, prfxhyd + "MERCATOR/MERCATOR_0.08DEG_50L_1D")}

    # source number of datasets per day, online path:
    ndsets, ondir = parms.get(path.basename(forcdir))
    #
    # file dates and time:
    #
    ncdates = dset["time"].data  # time in hours (gregorian)

    # calendar parameters:
    unit = "hours since 1950-01-01 00:00:00"
    cale = "gregorian"
    #
    # iterate number of days inside the file:
    #
    for day in range(len(ncdates)//ndsets):
        # output netCDF name:
        fout = num2pydate(ncdates[day*ndsets], unit, cale)         
        fout = outdir + fout.strftime("\\%Y%m%d%H.nc")
        
        # dataset output:
        xrout = dset.isel(time=slice(day*ndsets, (day+1)*ndsets))
        xrout.to_netcdf(fout, encoding=xrencode_simple(xrout)) 
    # #
    # # upload to SFTP:
    # #
    # ncs = glob(outdir + "\\*.nc")
    # serv =  "193.136.227.143"
    # user = "basic_admin"
    # pwrd = "basic2019_2021"
    # uploadsftp(ncs, ondir, serv, user, pwrd)
