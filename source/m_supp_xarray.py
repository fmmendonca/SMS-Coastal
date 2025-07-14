# ###########################################################################
#
# File    : m_supp_xarray.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Feb. 22nd, 2024.
#
# Updated : May 17th, 2024.
#
# Descrp. : Code with common operations using xarray.
#           - dimencd and fldencd: dictionaries with encoding parameters
#           for dimensions and fields, respectively.
#           - buildtime: builds a DataArray with date and time data.
#           - extractdate: converts date and time data, in a DataArray
#           object, from floats to cftime.
#           - buildz: builds a DataArray with depth data.
#           - buildlat and buildlon: builds DataArrays with latitude and
#           longitude grid values.
#           - encdset: encondes a Dataset with the parameters in dimencd and
#           fldencd.
#           - ncs2Dmean: creates an hourly averaged 2D Dataset from a list
#           of netCDF files.
#           - ncsmean: creates an averaged 3D Dataset from a list of netCDF
#           files.
#           - mergencs: merges in time a sequence of netCDF files.
#
# ###########################################################################

from tqdm import tqdm
from typing import Sequence

import numpy as np
import xarray as xr
from cftime import date2num, num2date


# Global variables:
dimencd = {
    "dtype": np.dtype("f8"),
    "_FillValue": None,
    "zlib": True,
}
fldencd = {
    "dtype": np.dtype("f8"),
    "_FillValue": -32768,
    "zlib": True,
    "complevel": 9,
}

# This code uses zlib to compress data,
# instead of using offset and scale factor.


def buildtime(data: Sequence) -> xr.DataArray:
    """Creates a standard time xarray.DataArray from a sequence of dates.
    
    Keyword argument:
    - data: a sequence with dates with or without times. Can be a list of
    datetimes or an array of cftimes. It can't be an array with datetime64
    dtype.
    """

    attrs = {
        "long_name": "Time", "standard_name": "time",
        "units": "hours since 1950-01-01 00:00:00",
        "axis": "T", "calendar": "gregorian",
    }

    data = date2num(data, attrs["units"], attrs["calendar"])
    attrs["valid_min"] = data.min()
    attrs["valid_max"] = data.max()
    darr = xr.DataArray(data, dims="time", name="time", attrs=attrs)
    darr.encoding = dimencd
    return darr


def extractdate(darr: xr.DataArray) -> np.ndarray:
    """Extracts from a xarray.DataArray, in the form like in 'buildtime',
    the datetime values in an array of cftime objects.
    
    Keyword argument:
    - darr: time coordinate xarray.DataArray.
    """
    data = darr.data
    attrs = darr.attrs
    data = num2date(data, attrs["units"], calendar=attrs["calendar"])
    return data


def buildz(data: np.ndarray) -> xr.DataArray:
    """Creates a standard depth xarray.DataArray from an
    array with depths values.
    
    Keyword argument:
    - data: 1D or 3D depth array. Invalid cells or with less
    than -30000 are considered land cells.
    """
    
    attrs = {
        "long_name": "Depth", "standard_name": "depth",
        "units": "m", "unit_long": "Meters",
        "axis": "Z", "positive": "down",
        "_CoordinateAxisType": "Height", "_CoordinateZisPositive": "down",
    }

    # Mask land cells:
    data = np.ma.masked_invalid(data)
    data = np.ma.masked_less(data, -30000)
    
    attrs["valid_min"] = data.min()
    attrs["valid_max"] = data.max()

    if data.ndim == 1:
        attrs["coord_type"] = "regular"
        data = np.ma.compressed(data)
        darr = xr.DataArray(data, dims="depth", name="depth", attrs=attrs)
        darr.encoding = dimencd
        return darr
    
    # When depth is not 1D, can't create the field with the same name of the
    # dimension. So depth won't have coordinates. Well, it is possible
    # to create, but other programs, such as Panoply, won't be able to read
    # fields with depth dimension.

    dims = ("depth", "latitude", "longitude")
    attrs["coord_type"] = "generic"
    darr = xr.DataArray(data, dims=dims, name="z3d", attrs=attrs)
    darr.encoding = fldencd
    return darr


def buildlat(data: np.ndarray) -> xr.DataArray:
    """Creates a standard latitude xarray.DataArray from a
    1D array with cartesian degrees values.
    
    Keyword argument:
    - data: latitude array.
    """
        
    key = "latitude"
    
    attrs = {
        "long_name": key.capitalize(), "standard_name": key,
        "units": "degrees_north", "unit_long": "Degrees north",
        "valid_min": -90.0, "valid_max": 90.0, "axis": "Y",
    }
    
    attrs["maximum"] = data.max()
    attrs["minimum"] = data.min()
    
    darr = xr.DataArray(data, dims=key, name=key, attrs=attrs)
    darr.encoding = dimencd
    return darr


def buildlon(data:np.ndarray, conv: bool) -> xr.DataArray:
    """Creates a standard longitude xarray.DataArray from a
    1D array with cartesian degrees values.
    
    Keyword arguments:
    - data: longitude array.
    - conv: switch to convert array from [0, 360] to [-180, 180].
    """
        
    key = "longitude"

    attrs = {
        "long_name": key.capitalize(), "standard_name": key,
        "units": "degrees_east", "unit_long": "Degrees east",
        "valid_min": -180.0, "valid_max": 180.0, "axis": "X",
    }
    
    if conv:
        data = (data + 180)%360 - 180
    
    attrs["maximum"] = data.max()
    attrs["minimum"] = data.min()

    darr = xr.DataArray(data, dims=key, name=key, attrs=attrs)
    darr.encoding = dimencd
    return darr


def encdset(dset: xr.Dataset) -> None:
    """Adds the encoding parameters to a dataset.
    
    Keyword argument: 
    - dset: xarray.Dataset to be updated
    """

    for dim in dset.dims:
        dset[dim].encoding = dimencd

    for fld in dset.data_vars:
        dset[fld].encoding = fldencd


def ncs2Dmean(ncs: Sequence) -> xr.Dataset:
    """Averages each hour at the surface, of the fields of a sequence
    of netCDF files.
    
    Keyword argument:
    - ncs: list with the name and path of netCDF files.
    """

    # Open netCDFs at the surface, keeping depth dimension (slcie(0,1)):
    dset = []
    
    for ntc in ncs:
        dstmp = xr.open_dataset(ntc, use_cftime=True)
        dset.append(dstmp.isel(depth=slice(0,1)))

    # Hourly average:
    dset = xr.merge(dset)
    dset = dset.resample(time="1h").mean(dim="time")

    # Fix time dimension, which is in cftime dtype:
    data = dset["time"].data
    dset.update({"time": buildtime(data)})
    encdset(dset)
    return dset


def ncsmean(ncs: Sequence) -> xr.Dataset:
    """Averages in time a sequence of netCDF files, sorted in time.
    
    Keyword argument:
    - ncs: list with the name and path of netCDF files.
    """

    # Get instant of the 1st file:
    dset = xr.open_dataset(ncs[0], use_cftime=True)
    data = dset["time"].data  # Should be a 1D array.
    
    # Average fields only with two 3D files at a time,
    # to avoid running out of RAM.

    for ntc in ncs[1:]:
        # Open temporary dataset and merge with old one:
        dstmp = xr.open_dataset(ntc, use_cftime=True)
        dset = xr.merge([dset, dstmp], compat="override")
        
        # Override: after the first cycle the datasets have
        # different attributes and encodings.

        dset = dset.mean(dim="time", keep_attrs=True)
        
        # The mean method removes the dimension time, which must
        # be added to the dataset so it can be averaged again:
        for fld in dset.data_vars:
            dset[fld] = dset[fld].expand_dims(dim="time", axis=0)

    # Fix time dimension:
    dset.update({"time": buildtime(data)})
    encdset(dset)
    return dset


def mergencs(ncs: Sequence[str], fout: str) -> None:
    """Merges a list of netCDF files.
    WARNING: the computer may run out of RAM.
    
    Keyword argument:
    - ncs: list with the names an paths of the netCDF files;
    - fout: name and path of the output file.
    """
    
    print("Merging netCDF files...")
    
    # PC may run out of RAM if doing like the following:
    # dset = xr.merge([xr.open_dataset(ntc, use_cftime=True) for ntc in ncs])
    
    dset = xr.Dataset({})
    
    for ntc in tqdm(ncs):
        dstmp = xr.open_dataset(ntc, use_cftime=True)
        dset = xr.merge([dset, dstmp])
        dstmp.close()
        del dstmp
    
    dset.update({"time": buildtime(dset["time"].data)})
    encdset(dset)
    dset.to_netcdf(fout)
