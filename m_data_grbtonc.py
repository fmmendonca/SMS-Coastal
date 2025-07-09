# ###########################################################################
#
# File    : m_data_grbtonc.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Mar. 1st, 2023.
#
# Updated : May 17th, 2024.
#
# Descrp. : Makes the coversion of a GRIB file to netCDF format.
#
#           The program uses an input JSON file, which contains the
#           parameters for performing the conversion. In this file, the user
#           must define the boolean key "convlon", which tells the code to
#           convert longitude values from [0, 360] to [-180, 180]. Open the
#           file with pygrib mudule to check if this is necessary.
#           
#           Note: pygrib must also be used to identify the number of the GRIB
#           message corresponding to the field/variable of interest in the
#           conversion.
#
#           The fields are defined in numerical keys (1, 2, 3...) in string
#           type. Each key must contain "grb_message" with the GRIB message
#           number corresponding to the field and "name" with its name to be
#           used in the netCDF. Other keys will be added as attributes.
#           Additionally, the user can define two factors to modify the data
#           of a field: "multfactor" to multiply the array and "addfactor" to
#           add. Multiplication is performed first. E.g.:
#           
#           {
#               "convlon": false,
#           
#               "1": {
#                   "grb_message": 3,
#                   "name": "t2m",
#
#                   "multfactor": 1
#                   "addfactor": -273.15
#
#                   "cf_name": "air_temperature",
#                   "long_name": "2 metre temperature",
#                   "units": "degC",
#                   "unit_long": "Degrees Celsius",
#                   "step_type": "instant",
#                   "level_type": "heightAboveGround",
#                   "level": "2",
#               }
#           }
#
# ###########################################################################

from datetime import datetime
from json import load
from typing import Sequence, Union

import pygrib
import numpy as np
import xarray as xr

from m_supp_xarray import buildlat, buildlon, buildtime, encdset


def grbtonc(
        grbin: str, fout: str, grid: Sequence[Union[float,int]],
        prmsfile: str) -> None:
    """Makes the conversion of a GRIB file to netCDF format.
    Assumes tha data has the shape (lat, lon).
        
    Keyword arguments:
    - grbin: name and path of the input GRIB file;
    - fout: name and path of the output netCDF file;;
    - grid: area where to extract the data in the format
    [min lat, max lat, min lon, max lon] using WGS84;
    - prmsfile: JSON files with te parameters for the conversion.
    """

    # Read conversion parameters file:
    with open(prmsfile, "rb") as dat:
        prms = load(dat)
        prms: dict

    # Convert grid longitude values:
    convlon = prms.pop("convlon", False)

    if convlon:
        # From [-180°, 180°] to [0°, 360°]
        grid = grid[:2] + [val%360 for val in grid[2:]]

    # Get time from first message of 'instant' type:
    grb = pygrib.open(grbin)

    msg = grb.select(stepType="instant")[0]
    msgdtime = str(msg.validityDate) + f"{msg.validityTime:04d}"
    data = [datetime.strptime(msgdtime, "%Y%m%d%H%M")]

    # Build output dataset:
    dset = xr.Dataset({"time": buildtime(data)})

    # Get grid data ('data' will be just a dummy var):
    data, lat, lon = msg.data(
        lat1=grid[0], lat2=grid[1],
        lon1=grid[2], lon2=grid[3],
    )

    if abs(lat[0][1] - lat[0][0]) < 0.00000001:
        lat = np.transpose(lat)
    
    dset.update({"latitude": buildlat(lat[0])})

    if abs(lon[0][1] - lon[0][0]) < 0.00000001:
        lon = np.transpose(lon)
    
    dset.update({"longitude": buildlon(lon[0], convlon)})

    # Extract fields:
    dims = ("time", "latitude", "longitude")
    fldno = 1
    attrs = prms.pop(str(fldno), {})
    attrs: dict
    
    while attrs:
        varid = attrs.pop("name", f"field_{fldno}")
        msgno = attrs.pop("grb_message", 0)
        
        if msgno <= 0:
            # Loop control vars:
            fldno += 1
            attrs = prms.pop(str(fldno), {})
            continue
        
        # Get the data (lat and lon will be dummies):
        data, lat, lon = grb[msgno].data(
            lat1=grid[0], lat2=grid[1],
            lon1=grid[2], lon2=grid[3],
        )

        # Update data and add time dimension:
        data *= attrs.pop("multfactor", 1)
        data += attrs.pop("addfactor", 0)
        data = np.array([data]) if data.ndim == 2 else data

        # Update output dataset:
        dset.update({varid: (dims, data, attrs)})
        
        # Loop control vars:
        fldno += 1
        attrs = prms.pop(str(fldno), {})

    # Calculate relative humidity. Do it only if temperature is in
    # °C, pressure in Pa and specific humidity in kg/kg:
    attrs = prms.pop("rh", {})
    varid = attrs.pop("name", "rh")
    
    if attrs:
        data = dset[attrs.get("humidity")].to_numpy()
        mixratio = data/(1 - data)

        # Calculate saturation vapor pressure (Es [Pa]):
        # obs.: multiply by 1000 to have Es in Pa, and calculate,
        # Es for positive and negative temperature.
        data = dset[attrs.get("temperature")].to_numpy()
        espos = 1000*0.61078*np.exp(17.27*data/(data+237.3))
        esneg = 1000*0.61078*np.exp(21.875*data/(data+265.5))
        
        # Calculate saturation mixing ratio:
        press = dset[attrs.get("pressure")].to_numpy()
        data = np.where(
            data >= 0,
            0.622*espos/(press-espos),
            0.622*esneg/(press-esneg),
        )
        
        # Calculate relative humidity (avoid rounding errors):
        data = mixratio/data
        data[data >= 1] = 0.999

        # Upload RH to dset:
        dset.update({varid: (dims, data, attrs)})

    # Calculate the velocity modulus:
    attrs = prms.pop("vm", {})
    varid = attrs.pop("name", "vm")

    vxid = attrs.get("velocity_x")
    vyid = attrs.get("velocity_y")

    if vxid and vyid:
        data = np.square(dset[vxid].to_numpy())
        data+= np.square(dset[vyid].to_numpy())
        dset.update({"vm": (dims, np.sqrt(data), attrs)})

    
    # Close grib file and save netCDF
    grb.close()
    encdset(dset)
    dset.to_netcdf(fout)
