# ###########################################################################
#
# File    : m_data_hdftonc.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Oct. 31st, 2023.
#
# Updated : Feb. 22nd, 2024.
#
# Descrp. : Program to convert a MOHID HDF5 file to netCDF format.
#           Only for 3D files.
#
# ###########################################################################

from datetime import datetime
from json import load
from os import path
from typing import Union

import numpy as np
import xarray as xr
from h5py import File

from m_supp_xarray import buildtime, buildz, buildlat, buildlon
from m_supp_xarray import fldencd, dimencd


def hdftonc(hdfin: str, ncout: str, convprms: str, gridout: bool) -> int:
    """Makes the conversion of a 3D MOHID HDF5 file to netCDF-4 formart.

    Keyword arguments:
    - hdfin: name and path to the HDF5 file;
    - ncout: name and path to the ouput netCDF file;
    - covprms: name and path to the input JSON file;
    - gridout: switch to generate the grid file in netCDF format in the
    same directory of ncout.
    """
    
    # Check input file:
    if not path.isfile(convprms):
        print("[ERROR] m_data_hdftonc: input file not found")
        return 1
    
    with open(convprms, "r") as dat:
        prms = load(dat)
    prms: dict
    
    # Grid file:
    fgrid = path.join(path.dirname(ncout), "grid.nc")

    # I/O datasets:
    hdf = File(hdfin, "r")
    dset = xr.Dataset({})
    dsgrid = xr.Dataset({})

    # Add time dimension:
    grp = hdf["Time"]
    data = [grp[key][...].astype("i2") for key in grp.keys()]
    data = [datetime(*tuple(val)) for val in data]
    dset.update({"time": buildtime(data)})

    # Add depth dimension from the first Vertical output:
    grp = hdf["/Grid/VerticalZ"]
    data = np.ma.masked_less(grp[list(grp.keys())[0]][...], -98)

    # MOHID generally saves HDFs from the bottom up. Reverse layers
    # order, from surface to bottom:
    data = data[::-1]

    # Data is in the shape (layer, longitude, latitude).
    # Change to (layer, latitude, longitude):
    data = np.ma.transpose(data, (0, 2, 1))

    # Layers in MOHID are defined by thicknesses in the Geometry_1.dat
    # file. But in the HDF5, they are converted into boundaries, so that
    # the boundary of the cells on the surface, depending on tidal
    # conditions, hardly matches the hydrographic zero.
    
    # If Δz1 is the surface thickness, then Δz1 = z1-z0, in which z1 is
    # the first value defined in Geometry_1.dat, and z0 is the surface
    # boundary, which can be a negative value. That only indicates the
    # tide is above the hydrographic zero. Remove z0:
    data = data[1:]
    
    if not prms.pop("z3d", False):        
        # Make a 1D depth coordinate, from the deepest cell:
        vmax = np.where(data==data.max())
        
        # 'vmax' is a tuple with three arrays, one for each dimension, and 
        # each one contains the indices of the position that the maximum
        # value occurs. Get the first occurrence in lat and lon dims:
        data = data[:, vmax[1][0], vmax[2][0]]
    
    # Number of layers:
    darr = buildz(data)
    dset.update({darr.name: darr})
    nlayers = len(darr)

    # Update grid dataset with thickness, bathymetry and lsm:
    zsurf = prms.pop("z0", None)
    # zsurf is outside from the if, so it can always be removed from prms.
    if gridout: grid_part1(hdf, dsgrid, nlayers, zsurf)

    # Add horizontal grid:
    lat = hdf["/Grid/Latitude"][...]
    data = (lat[0] + (np.roll(lat[0], -1) - lat[0])/2)[:-1]  # cell centered
    dset.update({"latitude": buildlat(data)})
    if gridout: dsgrid.update({"latitude": dset["latitude"]})

    lon = np.transpose(hdf["/Grid/Longitude"][...])
    data = (lon[0] + (np.roll(lon[0], -1) - lon[0])/2)[:-1]  # cell centered
    dset.update({"longitude": buildlon(data, False)})
    if gridout: dsgrid.update({"longitude": dset["longitude"]})

    if gridout:
        grid_part2(dsgrid, np.transpose(lat), lon)
        dsgrid.to_netcdf(fgrid)
    
    del dsgrid, lat, lon
    
    # Add fields:
    opngrp = hdf["Grid/OpenPoints"]
    opnkeys = tuple(opngrp.keys())

    for fld in prms.keys():
        data = []
        attrs = prms.get(fld)
        grpid = attrs.pop("hdfgroup", "")

        # Check HDF group
        if not grpid:
            print(f"[WARNING] Missing HDF group for '{fld}' field")
            continue
        
        try:
            grp = hdf[grpid]
        except KeyError:
            print(f"[WARNING] '{grpid}' is not an HDF group")
            continue

        # Iterate outputs and upload to data:
        for pos, key in enumerate(grp.keys()):
            tmp = grp[key][...]
            opnpts = opngrp[opnkeys[pos]][...].astype("i2")
            # 1=water, 0=land

            if tmp.ndim == 2:
                # OpenPoints at the surface and arays with (lat, lon) shape:
                opnpts = np.transpose(opnpts[-1])
                tmp = np.transpose(tmp)
            else:
                # Arrays from surface to bottom and with (lat, lon) shape:
                opnpts = np.transpose(opnpts[::-1], (0, 2, 1))[:nlayers]
                tmp = np.transpose(tmp[::-1], (0, 2, 1))[:nlayers]

            # Mask land cells. In MOHID the Hydrodynamic fields are masked
            # with zeros, which can be mistaken with the propertie value.
            tmp = tmp*opnpts + (opnpts - 1)*9.9e15
            # tmp = land cells are zero + land cells are negative infinity
            data.append(tmp)
        
        data = np.ma.masked_less(data, -30000)  # creates the time dimension
        
        if data.ndim == 3:
            dims = ("time", "latitude", "longitude")
        else:
            dims = ("time", "depth", "latitude", "longitude")
        
        dset.update({fld: (dims, data, attrs, fldencd)})

    # Write netCDF:
    hdf.close()
    dset.to_netcdf(ncout)
    return 0
    

def grid_part1(
        hdf: File, dsout: xr.Dataset,
        nlayers: int, zsurf: Union[float, int]) -> None:
    """Coputes the first part of the grid file, by updating 'dsout'
    in-place with thickness, lsm and bathymetry fields.
    
    Keyword arguments:
    - hdf: opened instance of the HDF5 file;
    - dsout: xarray.Dataset to upload the fields;
    - nlayers: number of layers to extract;
    - zsurf: hydrographic zero value.
    """

    # In this program cell thickness is not a time dependent field.
    # For that check SSH, where the surface thickness varies with time.
    # Like so, the user can specify the hydrographic zero with 'zsurf'.
    # With great power comes great responsibility!
    # Otherwise the code sets it with the lowest value in VerticalZ. 
    grp = hdf["/Grid/VerticalZ"]
    
    # Check hydrographic zero:
    if not isinstance(zsurf, (float, int)):
        zsurf = 0

        # The next loop is not to load all VerticalZ to RAM:
        for key in grp.keys():
            data = np.ma.masked_less(grp[key][...], -98)
            zsurf = min(zsurf, float(data.min()))

    # Get first output and set hydrographic zero:
    data = np.ma.masked_less(grp[list(grp.keys())[0]][...], -98)
    data = np.transpose(data[::-1], (0, 2, 1))  # (layer, lat, lon)
    data[0] = data[0]*0 + zsurf
    data = (np.roll(data, -1, 0) - data)[:-1][:nlayers]

    # [:-1] removes the last Δz = zn - z1
    # Update dsout:
    attrs = {
        "long_name": "Cell thickness",
        "standard_name": "cell_thickness",
        "units": "m",
        "unit_long": "Meters",
        "dimensions": 3
    }
    dims = ("depth", "latitude", "longitude")
    dsout.update({"thickness": (dims, data, attrs, fldencd)})

    # Extract time independent land-sea mask:
    grp = hdf["/Grid/OpenPoints"]
    data = grp[tuple(grp.keys())[0]][...].astype("i2")
    data = np.transpose(data[::-1], (0,2,1))[:nlayers]

    attrs = {
        "long_name": "Land-sea mask",
        "standard_name": "land_sea_mask",
        "units": "-",
        "unit_long": "1-water, 0-land",
        "valid_max": 1,
        "valid_min": 0,
        "dimensions": 3
    }

    encd = fldencd.copy()
    encd["dtype"] = np.dtype("i2")
    dsout.update({"lsm": (dims, data, attrs, encd)})

    # Extract bathymetry:
    data = np.ma.masked_less(hdf["/Grid/Bathymetry"][...], -98)
    data = np.transpose(data)
    attrs = {
        "long_name": "Bathymetry",
        "standard_name": "bathymetry",
        "units": "m",
        "unit_long": "Meters",
        "dimensions": 2
    }
    dims = ("latitude", "longitude")
    dsout.update({"bathymetry": (dims, data, attrs, fldencd)})
            

def grid_part2(dsout: xr.Dataset, lat: np.ndarray, lon: np.ndarray) -> None:
    """Coputes the second part of the grid file, by updating 'dsout'
    in-place with the boundary horizontal grid values.
    
    Keyword arguments:
    - dsout: xarray.Dataset to upload the fields;
    - lat: 1D latitude array with the HDF5 values;
    - lon: 1D longitude array with the HDF5 values.
    """

    dims = ("lat_y", "lon_x")

    attrs = {
        "long_name": "Latitude boundaries",
        "standard_name": "latitude_boundaries",
        "units": "degrees_north", "unit_long": "Degrees north",
        "valid_min": -90.0, "valid_max": 90.0, "axis": "Y",
    }
    attrs["maximum"] = lat.max()
    attrs["minimum"] = lat.min()
    dsout.update({"boundary_y": (dims, lat, attrs, dimencd)})

    attrs = {
        "long_name": "Longitude boundaries",
        "standard_name": "longitude_boundaries",
        "units": "degrees_east", "unit_long": "Degrees east",
        "valid_min": -180.0, "valid_max": 180.0, "axis": "X",
    }
    attrs["maximum"] = lon.max()
    attrs["minimum"] = lon.min()
    dsout.update({"boundary_x": (dims, lon, attrs, dimencd)})
