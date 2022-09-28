# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-10-11
#
# Updated : 2022-01-25


from datetime import datetime

import numpy as np
import xarray as xr
from cftime import date2num


def xrtime(dset, xrdset):
    """dset = time data in datetime.datetime list, cftime array or
       np.datetime64[ns] array
       xrdset = xarray.Dataset to be updated with gregorian time dataset
       
       Method to write gregorian time data into a given xarray.Dataset
       object. Updates 'xrdset' in-place."""

    # check if data is scalar an change to vectorial:
    if np.isscalar(dset) or (not isinstance(dset, list) and not dset.shape):
        dset = np.array([dset])    

    # check if data is numpy.datetime64 with nanoseconds:
    cond_a = isinstance(dset, np.ndarray)
    if cond_a and (dset.dtype != np.dtype('datetime64[s]')):
        dset = dset.astype('datetime64[s]')
        dset = [datetime.fromisoformat(str(inst)) for inst in dset]
    # list of datetime.datetime or array of cftime are fine

    # print("Time dataset type:", type(dset))
    # if np.isscalar(dset): dset = np.array([dset])

    # time attributes:
    attrs = {"units": "hours since 1950-01-01 00:00:00",
             "long_name": "Time",
             "standard_name": "time",
             "axis": "T",
             "calendar": "gregorian"}

    dset = date2num(dset, attrs["units"], attrs["calendar"]).astype("f4")
    if np.isscalar(dset): dset = np.array([dset])

    attrs["valid_min"] = dset.min()
    attrs["valid_max"] = dset.max()

    # write time dataset in xarray.Dataset
    xrdset.update({"time": ("time", dset, attrs)})


def xrlat(dset, xrdset):
    """dset = latitude array
       xrdset = xarray.Dataset to be updated
              
       Method to write latitude array into a given xarray.Dataset object.
       Method always change the name 'lat' to 'latitude'."""
    
    dset = dset.astype("f4")
    # create xrdset copy to not change original:
    xrdset = xrdset.copy()
    # by this the method does not update in-place

    # latitude attributes:
    attrs = {"valid_min": np.array([-90,]).astype('f4')[0],
             "valid_max": np.array([90,]).astype('f4')[0],
             "units": "degrees_north",
             "unit_long": "Degrees North",
             "long_name": "Latitude",
             "standard_name": "latitude",
             "axis": "Y",
             "maximum": dset.max(),
             "minimum": dset.min()}

    try:
        len(xrdset["lat"])
        xrdset = xrdset.rename_dims({"lat": "latitude"})
        xrdset = xrdset.rename({"lat": "latitude"})
    except KeyError:
        pass
    
    # write latitude in xarray.Dataset:
    xrdset.update({"latitude": ("latitude", dset, attrs)})
    return xrdset


def xrlon(dset, xrdset, convlon=None):
    """dset = longitude array
       xrdset = xarray.Dataset to be updated
       convlon = longitude conversion switch
       
       Method to write longitude array into a given xarray.Dataset object.
       Longitude array can be converted from 0~360 to -180~180 (convlon=180)
       and vice versa (convlon=360). Method always change the name 'lon' to
       'longitude'."""
    
    dset = dset.astype("f4")
    # create xrdset copy to not change original:
    xrdset = xrdset.copy()

    # correct longitude coordinate:
    vals = np.array([-180, 180]).astype('f4')
    if convlon == 180:
        dset = (dset + 180) % 360 - 180
    elif convlon == 360:
        dset = dset % 360
        vals = np.array([0, 360]).astype('f4')

    # longitude attributes:
    attrs = {"valid_min": vals[0],
             "valid_max": vals[1],
             "units": "degrees_east",
             "unit_long": "Degrees East",
             "long_name": "Longitude",
             "standard_name": "longitude",
             "axis": "X",
             "maximum": dset.max(),
             "minimum": dset.min()}

    try:
        len(xrdset["lon"])
        xrdset = xrdset.rename_dims({"lon": "longitude"})
        xrdset = xrdset.rename({"lon": "longitude"})
    except KeyError:
        pass

    # write longitude array in xarray.Dataset:
    xrdset.update({"longitude": ("longitude", dset, attrs)})
    return xrdset


def xrdep(dset, xrdset):
    """dset = depth array
       xrdset = xarray.Dataset to be updated
    
       Method to write depth array into a new or a given xarray.Dataset
       object. Updates 'xrdset' in-place."""
    
    dset = dset.astype("f4")

    # depth attributes:
    attrs = {"valid_min": dset.max(),
             "valid_max": dset.min(),
             "units": "m",
             "unit_long": "Meters",
             "long_name": "Depth",
             "standard_name": "depth",
             "axis": "Z",
             "positive": "down"}

    # write depth array in xarray.Dataset:
    xrdset.update({"depth": ("depth", dset, attrs)})


def xrmerge(ncs):
    """ncs = sorted list/tuple of the ncs to merge
       
       Merges the netCDF files given in 'ncs'."""

    xrdset = xr.Dataset()
    
    for ntc in ncs:
        xrdset = xrdset.merge(xr.open_dataset(ntc, use_cftime=True))

    xrtime(xrdset["time"].data, xrdset)
    return xrdset


def xrcut(grid_lims, xrdset, lat, lon):
    """grid_lims = list/tuple of the new grid limits as in
       (min latitude, max latitude, min longitude, max longitude)
       xrdset = xarray.Dataset to be updated
       lat = string with the name of latitude variable in dataset
       lon = string with the name of longitude variable in dataset
       
       Cuts 'xrdset' domain using the limits given in 'grid_lims'. Based on
       dimensions named as 'latitude' and 'longitude'."""

    slice_lat = slice(*grid_lims[:2])
    slice_lon = slice(*grid_lims[2:])
    xrdset.copy()
    xrdset = xrdset.loc[{lat:slice_lat, lon:slice_lon}]
    return xrdset


def xrencode_simple(xrdset):
    """xrdset = xarray.Dataset object
    
       Creates the encoding dictionary for the variables 
       and dimensions in 'xrdset'."""
    
    # variables encoding dictionary:
    encd = {}

    # dimensions:
    for var in xrdset.dims:
        encd[var] = {"dtype": np.dtype('f4'), '_FillValue': None}

    # variables:    
    for var in xrdset.data_vars:
        encd[var] = {"dtype": np.dtype('f4'), "_FillValue": -32768,
                     "zlib": True}
        
    return encd


def xrencode(xrdset, nrate):
    """xrdset = xarray.Dataset object
       nrate = compression rate (n)
    
       Creates the encoding dictionary for the variables 
       and dimensions in 'xrdset'."""
    
    # variables encoding dictionary:
    encd = {}

    # dimensions:
    for varid in xrdset.dims:
        encd[varid] = {"dtype": np.dtype('f4'), '_FillValue': None}

    # variables:    
    for varid in xrdset.data_vars:
        #
        # Stretch/compress data to the available packed range:
        #
        vmax = xrdset[varid].max()  # xarray.DataArray object
        vmin = xrdset[varid].min()
        
        # define scale factor:
        scf = ((vmax - vmin)/(2**(nrate - 1))).astype("f4")
        scf = scf.round(4) if (-1 < scf < 1) else scf.round()

        # define add offset,
        # translate the range to be symmetric about zero:
        ofs = (vmin + 2**(nrate - 1)*scf).astype("f4")
        ofs = ofs.round(4) if (-1 < ofs < 1) else ofs.round()
        
        # update encoding variable:
        encd[varid] = {"dtype": np.dtype('i2'),
                        "_FillValue": -32768,
                        "scale_factor": scf.data,
                        "add_offset": ofs.data,
                        "zlib": True}   
    return encd