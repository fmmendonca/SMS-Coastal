# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-11-04
#
# Updated : 2022-01-21 checked, all functions working
#
# Specifically built module to convert MOHID HDF5 output files to the
# netCDF format according to SMS-Coastal standards.
#

from datetime import datetime

import numpy as np
import numpy.ma as ma
import xarray as xr
from h5py import File

import support_xrdset as sxr


def mohidvars(varid):
    """varid = string with the name of MOHID variable/field

       Contains, in a dictionary, the set of MOHID variables that SMS-Coastal
       can convert. Returns a list of strings with the information of a
       single variable name in netCDF, its long name, and its standard name."""
    #
    # convertble MOHID variables:
    #
    hdfvrid = {"velocity U": ['uo', 'Eastward Velocity',
                              'eastward_sea_water_velocity'],
                "velocity V": ['vo', 'Northward Velocity', 
                               'northward_sea_water_velocity'],
                "velocity W": ['wo', 'Vertical Velocity',
                               'vertical_sea_water_velocity'],
                "velocity modulus": ['vmod', 'Velocity Modulus',
                                     'sea_water_velocity_modulus'],
                "temperature": ['thetao', 'Temperature',
                                'sea_water_potential_temperature'],
                "salinity": ['so', 'Salinity', 'sea_water_salinity'],
                "density": ['rho', 'Density', 'sea_water_density'],
                "water level": ["ssh", "Sea Surface Height",
                                "sea_surface_height_above_geoid"]}
    return hdfvrid.get(varid)


def mohidunts(unit):
    """unit = string with the name of MOHID unit
       
       Contains, in a dictionary, the set of MOHID units that SMS-Coastal can
       convert. Returns a list of strings with the information of a single
       unit name in netCDF, and its long name."""
    #
    # convertble MOHID units:
    #
    hdfunts = {"ºC": ["degrees_C", 'Degrees Celsius'],
                "?C": ["degrees_C", 'Degrees Celsius'],
                "m/s": ['m s-1', 'Meters per Second'],
                "psu": ['psu', 'Practical Salinity Unit'],
                "Kg/m3": ['kg m3-1', 'Kilograms per Cubic meter'],
                "m": ['m', 'Meters'],
                "W/m2": ['W m2-1',  'Watt per Meter Square'],
                "1/m": ['m-1', "Inverse Meters"]}
    return  hdfunts.get(unit)

    
def waterlevel(ohdf, keys, shpe):
    """ohdf = h5py instance of an opened HDF file
       keys = keys from the water level group in the HDF file
       shpe = iterable (list/tuple) with the shape of the other
       datasets in the HDF file
    
       This is a supporting function of hdf2xrdset. It was built to reduce the
       amount if statements used, since water level property is to be handled
       differently from other datasets in MOHID files."""
    #
    # waterlevel dataset shape and allocate array:
    #
    shpe = shpe[0], shpe[-2], shpe[-1]
    arr = ma.zeros(shpe).astype("f4")
    #
    # update array for each output in the HDF:
    #     
    for inst, key in enumerate(keys, 1):
        #
        # get land cells at the surface ([-1]):
        #
        opts = f"/Grid/OpenPoints/OpenPoints_{inst:05d}"
        opts = np.array(ohdf[opts]).astype('i1')[-1]
        #
        # get values and mask land cells:
        #
        dset = np.array(ohdf[key])            
        dset = dset * opts + ((opts - 1) * 9.98e15)
        dset = np.ma.masked_less(dset, -98)            
        dset = np.ma.transpose(dset, (1, 0))
        #
        # upload values to array:
        #             
        arr[inst - 1] = dset.astype("f4")    
    return arr


def hdf2xrdset(hdfin, hdfvars=None):
    """hdfin = string with the path and name of the MOHID HDF5 file
       hdfvars = iterable (list/tuple) of strings with the name of MOHID
       variables/fields to convert in Results group
    
       Function to extract the datasets from an HDF, and to create an
       xarray.Dataset object with those. The variables/fields for conversion
       of an HDF file can be specified in the arguments. It is suitable for
       both, 2D and 3D MOHID HDF5 files."""
    #
    # Allocate the output xarray.Dataset object:
    #
    xrdset = xr.Dataset({})
    #
    # Open HDF5 in read mode and import ouput dates:
    #
    hdf = File(hdfin, "r")
    keys = ['/Time/' + key for key in hdf.get('/Time')]
    dset = [np.array(hdf[key]).astype('i2') for key in keys]
    dset = [datetime(*tuple(val)) for val in dset]
    #
    # Update xrdset:
    #
    sxr.xrtime(dset, xrdset)
    #
    # Import depth dimension:
    #
    dset = hdf.get("/Grid/VerticalZ")

    if dset:
        # first output key:
        key = "/Grid/VerticalZ/" + list(dset.keys())[0]
        # get dataset and transpose to (lat, lon):
        dset = ma.masked_less(hdf.get(key), -98)[:-1].transpose()
        # [:-1] to remove surface edge (values must be cell centered)
        
        # locate cell with deepest value (lat, lon, layer):
        vmax = np.array(np.where(dset==dset.max())).transpose()[0]
        # get all layers for previous cell:
        dset = dset[vmax[0], vmax[1]].compressed()[::-1]
        # remove land cells qith compress and sort from shallowest to deepest

        #
        # Update xrdset:
        #
        sxr.xrdep(dset, xrdset)
    #
    # Import latitude dimension:
    #
    dset = np.array(hdf.get("/Grid/Latitude"))[0]
    # bring to center cell and remove last value:
    dset = (dset[:-1] + (dset[1] - dset[0])/2)
    #
    # Update xrdset:
    #
    xrdset = sxr.xrlat(dset, xrdset)
    #
    # Import longitude dimension:
    #
    dset = np.array(hdf.get("/Grid/Longitude")).transpose()[0]
    dset = (dset[:-1] + (dset[1] - dset[0])/2)
    #
    # Update xrdset:
    #
    xrdset = sxr.xrlon(dset, xrdset)
    #
    # xrdset datasets dimension and shape:
    #
    xrdims = dict(xrdset.dims)
    if not xrdims.get("depth"):
        dims = "time", "latitude", "longitude"
    else:
        dims = "time", "depth", "latitude", "longitude"
    shpe = [xrdims.get(dim) for dim in dims]
    #
    # variables to convert:
    #
    convars = hdfvars if hdfvars else [var for var in hdf.get("/Results")]

    # extract each variable and update xrdset:
    for var in convars:
        # variable attributes in netcdf:
        attrs = mohidvars(var)
        #
        # check if variable is convertble and if is in the HDF:
        #
        if (not attrs) or (var not in hdf.get("/Results")):
            print(f"WARNING: cannot convert « {var} ».")
            continue
        
        # get netCDF variable name:
        ncvar = attrs.pop(0)  # uo, vo, so...

        # variable group path in HDF and its keys:
        gpath = "/Results/" + var + "/"
        keys = [gpath + key for key in hdf.get(gpath)]
        #
        # decode MOHID unit string (iso8859_15" = ASCII latin-9, latin-0):
        #
        dset = hdf[keys[0]].attrs.get("Units")  # numpy.bytes
        dset = str(np.char.decode(dset, encoding="iso8859_15"))

        # add units to the variabel attributes in netCDF:
        attrs = attrs + mohidunts(dset)  # concatenate tuples
        #
        # create attributes dictionary
        #
        attrs_keys = "long_name", "standard_name", "units", "unit_long"
        attrs = dict(zip(attrs_keys, attrs))
        #
        # if variable/field is water level, use waterlevel function:
        #
        if var == "water level":
            dset = waterlevel(hdf, keys, shpe)
            var_dims = dims[0], dims[-2], dims[-1]
            xrdset = xrdset.assign({ncvar: (var_dims,  dset, attrs)})
            continue
        #
        # allocate masked array for the variable/field:
        #
        arr = ma.zeros(shpe).astype("f4")

        # populate dataset:
        for inst, key in enumerate(keys, 1):
            # open points for the same instant (1 sea, 0 land):
            opts = f"/Grid/OpenPoints/OpenPoints_{inst:05d}"
            opts = np.array(hdf[opts]).astype("int8")
            # import variable/field:
            dset = np.array(hdf[key])
            # make land cells iqual to -9.998e15:
            dset = dset * opts + ((opts - 1) * 9.998e15)
            # mask all land cells:
            dset = np.ma.masked_less(dset, -98)
            #
            # Dataset conversions:
            # 1. shape to (depth, latitude, longitute)
            # 2. remove above surface limit with [::-1]
            # all land layers ([:shpe[1]])??
            
            #
            # Note: even surface fields in MOHID HDF5 files have shape of (1, lon, lat)
            #

            dset = np.ma.transpose(dset[::-1][:shpe[1]], (0, 2, 1))
            #
            # remove dimension of single layer datasets:
            #
            dset = dset[0] if (len(shpe) < 4) else dset
            #
            # update variable/field masked array:
            #
            arr[inst - 1] = dset.astype("f4")
        #
        # upload arra to xrdset:
        #
        xrdset = xrdset.assign({ncvar: (dims, arr, attrs)})
    #
    # close HDF5 file and return the xarray object:
    #
    hdf.close()
    return xrdset
