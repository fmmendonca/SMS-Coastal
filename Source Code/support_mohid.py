# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-12-03
#
# Updated : 2022-01-21 checked, all functions working
#


from os import getcwd, path, chdir
from  subprocess import run
from datetime import datetime
from shutil import rmtree

import numpy as np
from h5py import File


def checkfiles(files):
    """files = iterable (list/tuple) of strings with the full path of the
       files to check
       
       Function to check if file in an iterable exists."""

    status = 1

    for file in files:
        if not status:
            continue

        if not path.isfile(file):
            status = None
    
    return status


def readlog(mohidlog):
    """mohidlog = path and name of MOHID log file
    
       Function to read MOHID log files and determine if program has
       successfully ended."""
    
    dat = open(mohidlog, "r")
    line = dat.readline()
    status = None
    
    while line:
        if ("Program" and "successfully terminated") in line:
            line = None
            status = 1
            continue

        line = next(dat, None)
    
    dat.close()
    return status


def writenomfich(nomfich, inmodel):
    """nomfich = string with path to write nomfich file
       inmodel = string with the value for the keyword IN_MODEL of nomfich
       file
       
       Supporting function to write the nomfich file for different kinds
       of MOHID pre/post processing operations."""

    with open(nomfich + "\\nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT     : .\\\n")
        dat.write(f"IN_MODEL     : .\\{inmodel}\n")


def outmerger(hdfs, tridim, fout):
    """hdfs = iterable (list/tuple) with the path to the hydrodynamic and
       water properties HDF5 files pair, that is, of a single model level
       tridim = string switch to tell if is a 3D file (1) or surface file(0)
       fout = string with the path and name of the output file
       
       Function to merge forecast MOHID hydrodynamic and water properties
       output HDF5 files."""
    
    # check merger exe file:
    merger = getcwd() + "\\MOHID\\Merger\\Convert2Hdf5.exe"
    status = checkfiles([merger,])
    if not status:
        print("WARNING: ", end="")
        print("Merging operation FAILED, MOHID exe file not found")
        return
    
    # write nomfich:
    writenomfich(path.dirname(merger), "ConvertToHDF5Action.dat")

    # write ConvertToHDF5Action_original.dat:
    with open(path.dirname(merger) + "\\ConvertToHDF5Action.dat", "w") as dat:
        dat.write("<begin_file>\n")
        dat.write("ACTION         : GLUES HDF5 FILES\n") 
        dat.write("GLUE_IN_TIME   : 0\n")
        dat.write("3D_FILE        : " + tridim + "\n")
        dat.write("3D_OPEN        : 1\n")
        dat.write("OUTPUTFILENAME : " + fout + "\n\n")
        dat.write("<<begin_list>>\n")
        [dat.write(hdf + "\n") for hdf in hdfs]
        dat.write("<<end_list>>\n<end_file>\n")

    print("Merging MOHID hydrodynamic and water properties output files...")
    chdir(path.dirname(merger))
    cmd = path.basename(merger) + " > log_run.txt 2> log_err.txt"
    run(cmd, shell=True)

    # check log:
    status = readlog("log_run.txt")
    if not status:
        print("WARNING: Merging operation FAILED", end="\n\n")
    
    chdir("..\\..\\")
    print("Merging COMPLETED.", end="\n\n")
    return status


def extract_outputs(mergedhdf, outdir, prefix=""):
    """"mergedhdf = path and name of the MOHID HDF5 to extract
        outdir =  path to save new files
        prefix = string with the prefix for the extracted files
        
        Function to extract the outputs from a MOHID HDF5 file,
        one new HDF5 file for each output."""
        
    print("Extracting each HDF5 output...")
    # check MOHID files:
    extractor = getcwd() + "\\MOHID\\Extractor\\HDF5Extractor.exe"
    dlls = "\\szlibdll.dll", "\\zlib1.dll"
    dlls = [path.dirname(extractor) + dll for dll in dlls]
    status = checkfiles([extractor,] + dlls)
    
    if not status:
        print("WARNING: ", end="")
        print("MOHID files missing for extraction")
        return
    
    # write nomfich:
    writenomfich(path.dirname(extractor), "Extractor.dat")
        
    # get hdf variables to extract:
    hdf = File(mergedhdf, "r")
    hdfvars = list(hdf["Results"].keys())
    
    # get time array from hdf:
    dates = ["/Time/" + key for key in hdf.get("/Time")]
    dates = [np.array(hdf[key]).astype('i2') for key in dates]
    dates = [datetime(*tuple(val)) for val in dates]
    hdf.close()
        
    # extract ouputs:
    status = 1
    chdir(path.dirname(extractor))

    for inst in dates:
        if not status:
            continue

        # write Etractor.dat:
        with open("Extractor.dat", "w") as dat:
            dat.write("FILENAME       : " + mergedhdf + "\n")
            dat.write("OUTPUTFILENAME : " + outdir + f"\\{prefix}")
            dat.write(inst.strftime("%Y%m%d_%H%M.hdf5\n"))
            dat.write("START_TIME     : ")
            dat.write(inst.strftime("%Y %m %d %H %M 00\n"))
            dat.write("END_TIME       : ")
            dat.write(inst.strftime("%Y %m %d %H %M 01\n"))
            dat.write("SKIP_INSTANTS  : 0")

            for var in hdfvars:
                dat.write("\n\n<BeginParameter>\n")
                dat.write(f"PROPERTY      : {var}\n")
                dat.write(f"HDF_GROUP     : /Results/{var}\n")
                dat.write("<EndParameter>")
            
            dat.write("\n")
        
        # run MOHID extractor:
        cmd = path.basename(extractor) + " > log_run.txt 2> log_err.txt"
        run(cmd, shell=True)
        # check log:
        status = readlog("log_run.txt")

    chdir("..\\..\\")
    if not status:
        print("WARNING: Extracting operation FAILED.", end="\n\n")
        rmtree(outdir)
    else:
        print("Extraction COMPLETED.", end="\n\n")
    return status


def mergeintime(hdfs, hdfout):
    """hdfs = sorted iterable (list/tuple) of strings with the path and name
       of the files
       hdfout = string with the path and name of the output HDF5

       Function to merge a list of MOHID HDF5 files in time, that is, a
       sequence of outputs."""

    # check merger exe file:
    merger = getcwd() + "\\MOHID\\Merger\\Convert2Hdf5.exe"
    status = checkfiles([merger,])
    if not status:
        print("WARNING: ", end="")
        print("Merging operation FAILED, MOHID exe file not found")
        return

    # write nomfich:
    writenomfich(path.dirname(merger), "ConvertToHDF5Action.dat")

    # write ConvertToHDF5Action.dat:
    dattxt = "<begin_file>\nACTION         : GLUES HDF5 FILES\n"
    dattxt += "GLUE_IN_TIME   : 1\n3D_FILE        : 1\n3D_OPEN        : 1\n"
    dattxt += f"OUTPUTFILENAME : {hdfout}\n\n<<begin_list>>\n"
    dattxt += "\n".join(hdfs) + "\n<<end_list>>\n\n<end_file>\n"
    
    with open(path.dirname(merger) + "\\ConvertToHDF5Action.dat", "w") as dat:
        dat.write(dattxt)

    # run MOHID Convert2Hdf5:
    chdir(path.dirname(merger))
    cmd = path.basename(merger) + " > log_run.txt 2> log_err.txt"
    run(cmd, shell=True)

    # check log:
    status = readlog("log_run.txt")

    if not status:
        print("WARNING: Merging operation FAILED")
    chdir("..\\..\\")
    return status


def hdf2nc(hdf, ncout):
    """hdf = string with path and name of MOHID 3D HDF5 file
       ncout = string with path and name of netCDF output

       Function to convert MOHID 3D HDF5 files using its own tools."""
    #
    # check merger exe file:
    #
    conver = getcwd() + "\\MOHID\\NetCDF\\Convert2netcdf.exe"
    dlls = ['hdf.dll', 'hdf5.dll', 'hdf5_cpp.dll', 'hdf5_f90cstub.dll',
            'hdf5_fortran.dll', 'hdf5_hl.dll', 'hdf5_hl_cpp.dll',
            'hdf5_tools.dll', 'jpeg.dll', 'libcurl.dll', 'libifcoremd.dll',
            'libmmd.dll', 'mfhdf.dll', 'msvcp120.dll', 'msvcp140.dll',
            'msvcr120.dll', 'netcdf.dll', 'szip.dll', 'vcruntime140.dll',
            'xdr.dll', 'zlib.dll', 'zlib1.dll']
    dlls = [path.dirname(conver) + "\\" + dll for dll in dlls]
    status = checkfiles([conver,] + dlls)
    if not status:
        print("WARNING: ", end="")
        print("Conversion operation FAILED, MOHID file(s) not found")
        return
    #
    # write nomfich:
    #
    writenomfich(path.dirname(conver), "Convert2netcdf.dat")
    #
    # get field names:
    #
    with File(hdf, "r") as hdfin:
        vertical = list(hdfin["Grid/VerticalZ"].keys())[0].split("_")[0]
        fields = ["/Results/" + key for key in hdfin["Results"].keys()]
    #
    # write Convert2netcdf.dat:
    #
    dattxt = f"HDF_FILE            : {hdf}\nHDF_SIZE_GROUP       : /Grid\n"
    dattxt += "SIMPLE_GRID          : 1\nHDF_TIME_VAR         : Time\n"
    dattxt += "IMPOSE_MASK          : 1\n"
    dattxt += "HDF_SIZE_DATASET     : WaterPoints3D\n"
    dattxt += f"HDF_VERT_VAR         : VerticalZ/{vertical}\n"
    #
    # might have to add these keywords to init.dat:
    #
    dattxt += "HDF_READ_DEPTH       : 1\nHDF_READ_LATLON      : 1\n"
    dattxt += "HDF_READ_SIGMA       : 0\nDEPTH_OFFSET         : 2.28\n"
    dattxt += "CONVERT_EVERYTHING   : 1\n\n<begin_groups>\n"

    dattxt += "\n".join(fields) + "\n<end_groups>\n\n"
    dattxt += f"NETCDF_FILE          : {ncout}\n"
    dattxt += "NETCDF_TITLE         :  MOHID converted forecast data\n"
    dattxt += "NETCDF_CONVENTION    : CF-1.6\nNETCDF_VERSION       : 4.4.1\n"
    dattxt += "NETCDF_HISTORY       : 2018/11/12 14:45:24 Maretec Netcdf "
    dattxt += "creation\nNETCDF_SOURCE        : ConvertTonetcdf - Mohid "
    dattxt += "tools\nNETCDF_INSTITUTION   : Technical University of Lisbon "
    dattxt += "- Instituto Superior Tecnico (IST) - MARETEC\n"
    dattxt += "NETCDF_REFERENCES    : http://www.maretec.org/\n"
    dattxt += "NETCDF_DATE          : 2018\n"
    dattxt += "NETCDF_COORD_SYSTEM  : ucar.nc2.dataset.conv.CF1Convention\n"
    dattxt += "NETCDF_CONTACT       : joao.sobrinho@tecnico.ulisboa.pt\n"
    dattxt += "NETCDF_FIELD_TYPE    : mean\n"
    dattxt += "NETCDF_BULLETIN_DATE : 2018-11-12 00:00:00\n"
    dattxt += "NETCDF_COMMENT       : Maretec operational modelling product\n"

    with open(path.dirname(conver) + "\\Convert2netcdf.dat", "w") as dat:
        dat.write(dattxt)
    #
    # run MOHID Convert2netcdf:
    #
    chdir(path.dirname(conver))
    cmd = path.basename(conver) + " > log_run.txt 2> log_err.txt"
    run(cmd, shell=True)
    #
    # check log:
    #
    status = readlog("log_run.txt")

    if not status:
        print("WARNING: netCDF conversion operation FAILED")
    chdir("..\\..\\")
    return status
