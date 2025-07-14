# ###########################################################################
#
# File    : m_supp_mohid.py
#
# Author  : Fernando Mendonça (CIMA UAlg)
#
# Created : Jul. 12th, 2023.
#
# Updated : Apr. 11th, 2024.
#
# Descrp. : Functions to run MOHID operations:
#           - runsim: runs a simulation.
#           - extract: extracts all outputs from an HDF5 file, one new HDF5
#           for each output time.
#           - gluehdfs: merges a hydrodynamic HDF5 with a water property one
#           at the same time instant.
#           - glueintime: merges squential/consecutive HDFs.
#           - convert2hdf5: runs the Convert2Hdf5.exe tool.
#           - convert2nc: converts HDF to netCDF.
#
# ###########################################################################

from datetime import datetime
from glob import glob
from os import chdir, getcwd, path, unlink
from shutil import copyfile
from subprocess import run
from typing import Sequence

from h5py import File


def chklog(flog) -> int:
    """Check for the successful message in a MOHID log file."""

    status = 1
    dat = open(flog, "r")
    line = dat.readline()

    while line:
        if ("Program" and "successfully terminated") in line:
            line = None
            status = 0
            continue
        line = next(dat, None)

    dat.close()
    return status


def runsim(mohidir: str, wkdir: str) -> int:
    """Run a MOHID simulation.
    
    Keyword arguments:
    - mohidir: path to the directory containing the MOHID exe
    and its libraries;
    - wkdir: path to the directory in which MOHID will be launched,
    that is, the 'exe' folder of the model's first level.
    """
    
    # Get absolute path of MOHID directory:
    rtdir = getcwd()
    mohidir = path.abspath(mohidir)

    # Check MOHID files:
    mfiles = (
        "MOHIDWater.exe", "hd425m.dll", "hdf_fcstubdll.dll",
        "hdf_fortrandll.dll", "hdf5dll.dll", "hm425m.dll",
        "libiomp5md.dll", "mfhdf_fcstubdll.dll",
        "mfhdf_fortrandll.dll", "msvcp71.dll", "msvcr71.dll",
        "netcdf.dll", "szip.dll", "szlibdll.dll", "zlib1.dll",
    )

    mfiles = [path.join(mohidir, file) for file in mfiles]

    for file in mfiles:
        if path.isfile(file): continue
        print("[ERROR] m_supp_mohid: MOHID file missing:", file)
        return 1

    # MOHID log file:
    flog = path.join(wkdir, "mohid_stdout.dat")
    flog = path.abspath(flog)

    # Run MOHID:
    cmd = f"\"{mfiles[0]}\" > \"{flog}\""
    print(cmd)
    chdir(wkdir)
    run(cmd, shell=True)
    chdir(rtdir)
    status = chklog(flog)

    if status < 1: return 0
    print("[ERROR] m_supp_mohid: simulation stopped")
    return 1


def extract(mohidir: str, hdfin: str, outdir: str, prfx: str) -> int:
    """Extracts each output time from of a MOHID HDF5 file. Each outputed
    HDF5 files has the name as follows: prfix-YYYYMMDDTHHmm.hdf5, e.g. if
    'prfx' is 'HD', then one of the outputs could be HD-20240209T1200.hdf5.
    Relative paths are allowed in the inputs.
    
    Keyword arguments
    - mohidir: path to the directory with the MOHID tool and its libraries;
    - hdfin: name and path of the input HDF5 file;
    - outdir: output directory;
    - prfx: prefix of each outputed HDF5 file.
    """

    # Set paths:
    curdir = getcwd()
    mohidir = path.abspath(mohidir)
    hdfin = path.abspath(hdfin)
    outpath = path.join(path.abspath(outdir), prfx) + "-"

    # Check MOHID files:
    mfiles = ("HDF5Extractor.exe", "szlibdll.dll", "zlib1.dll")
    mfiles = [path.join(mohidir, file) for file in mfiles]

    for file in mfiles:
        if path.isfile(file): continue
        print("[ERROR] m_supp_mohid: MOHID file missing:", file)
        return 1
    
    # Get HDF5 dates and fields:
    hdf = File(hdfin, "r")
    grp = hdf["Time"]
    dates = [grp[key][...].astype("i2") for key in grp.keys()]
    dates = [datetime(*tuple(val)) for val in dates]
    flds = [key for key in hdf["Results"]]
    hdf.close()

    # Change to MOHID working directory:
    chdir(mohidir)

    with open("nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT : .\\\n")
        dat.write("IN_MODEL : ConvertToHDF5Action.dat\n")

    # Extrac outputs:
    print("Extracting", hdfin)

    for idate in dates:
        print("", outpath + idate.strftime("%Y%m%dT%H%M.hdf5"))

        # Write conversion file:
        dat = open("ConvertToHDF5Action.dat", "w")
        dat.write(f"""FILENAME       : {hdfin}
OUTPUTFILENAME : {outpath + idate.strftime("%Y%m%dT%H%M.hdf5")}
START_TIME     : {idate.strftime("%Y %m %d %H %M %S")}
END_TIME       : {idate.strftime("%Y %m %d %H %M %S")}
\n""")
        
        for fld in flds:
            dat.write(f"""<BeginParameter>
PROPERTY      : {fld}
HDF_GROUP     : /Results/{fld}
<EndParameter>\n\n""")
        
        dat.close()

        # Run MOHID:
        run("HDF5Extractor.exe > mohid_stdout.dat", shell=True)
        status = chklog("mohid_stdout.dat")
        if status < 1: continue

        print("[ERROR] m_supp_mohid: simulation stopped")
        return 1
  
    # change back to original working directory:
    chdir(curdir)
    return 0


def gluehdfs(
        mohidir: str, iptdir: str, hdprfx: str,
        wpprfx: str, prfxout: str, keep: bool) -> int:
    """Merges 3D MOHID HDF5 hydrodynamic files with the water
    properties ones, contained in the same directory. Merges files
    groups at same instants.
    
    Keyword arguments
    - mohidir: path to the directory with the MOHID tool and its libraries;
    - iptdir: input directory where the files are;
    - hdprfx: prefix to the hydrodynamic files;
    - wpprfx: prefix to the water properties files;
    - prfxout: output HDF5 files prefix;
    - keep: switch to keep original files.
    """

    # Set paths:
    curdir = getcwd()
    mohidir = path.abspath(mohidir)
    iptdir = path.abspath(iptdir)

    # Check MOHID files:
    mfile = path.join(mohidir, "Convert2Hdf5.exe")
    if not path.isfile(mfile):
        print("[ERROR] m_supp_mohid: MOHID file missing:", mfile)
        return 1
    
    # Glob files
    hdhdfs = sorted(glob(path.join(iptdir, hdprfx + "*")))
    wphdfs = sorted(glob(path.join(iptdir, wpprfx + "*")))

    if len(hdhdfs) != len(wphdfs):
        print("[ERROR] m_supp_mohid: unmatched amount of HDF5 files")
        return 1
    elif len(hdhdfs) < 1:
        print("[ERROR] m_supp_mohid: HDF5 files not found")
        return 1

    # Change to MOHID working directory:
    chdir(mohidir)

    with open("nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT : .\\\n")
        dat.write("IN_MODEL : ConvertToHDF5Action.dat\n")

    # Iterate and merge files:
    print("Merging HDF5 files...")

    for pos in range(len(hdhdfs)):
        print("", hdhdfs[pos], "&&", wphdfs[pos])
        
        # Get HDF5 dates:
        with File(hdhdfs[pos], "r") as hdf:
            grp = hdf["Time"]
            hddate = [grp[key][...].astype("i2") for key in grp.keys()]
            hddate = [datetime(*tuple(val)) for val in hddate][0]
        with File(wphdfs[pos], "r") as hdf:
            grp = hdf["Time"]
            wpdate = [grp[key][...].astype("i2") for key in grp.keys()]
            wpdate = [datetime(*tuple(val)) for val in wpdate][0]

        if hddate != wpdate:
            print("[ERROR] m_supp_mohid: unmatched HDF5 files")
            chdir(curdir)
            return 1
        
        fout = path.join(iptdir, prfxout)
        fout+= hddate.strftime("-%Y%m%dT%H%M.hdf5")

        # Write conversion file:
        dat = open("ConvertToHDF5Action.dat", "w")
        dat.write(f"""<begin_file>
ACTION         : GLUES HDF5 FILES
GLUE_IN_TIME   : 0
3D_FILE        : 1
3D_OPEN        : 1
OUTPUTFILENAME : {fout}

<<begin_list>>
{hdhdfs[pos]}
{wphdfs[pos]}
<<end_list>>
<end_file>\n""")  
        dat.close()

        # Run MOHID:
        run("Convert2Hdf5.exe > mohid_stdout.dat", shell=True)
        status = chklog("mohid_stdout.dat")

        if status < 1 and not keep:
            unlink(hdhdfs[pos])
            unlink(wphdfs[pos])
            continue
        elif status < 1 and keep:
            continue

        print("[ERROR] m_supp_mohid: HDF glue failed")
        chdir(curdir)
        return 1
  
    # change back to original working directory:
    chdir(curdir)
    return 0


def glueintime(mohidir: str, hdfs: Sequence, fout: str) -> int:
    """Merges a sequence of 3D MOHID HDF5 files in time,
    so they must be sequential/consecutive.
    
    Keyword arguments
    - mohidir: path to the directory with the MOHID tool and its libraries;
    - hdfs: list with paths and names of the HDF files;
    - fout: name and path of the output file.
    """

    # Set paths:
    curdir = getcwd()
    mohidir = path.abspath(mohidir)
    hdfs = [path.abspath(hdf) for hdf in hdfs]
    fout = path.abspath(fout)

    # Check MOHID files:
    mfile = path.join(mohidir, "Convert2Hdf5.exe")
    if not path.isfile(mfile):
        print("[ERROR] m_supp_mohid: MOHID file missing:", mfile)
        return 1
    
    # Change to MOHID working directory:
    chdir(mohidir)

    with open("nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT : .\\\n")
        dat.write("IN_MODEL : ConvertToHDF5Action.dat\n")

    with open("ConvertToHDF5Action.dat", "w") as dat:
        dat.write(f"""<begin_file>
ACTION                    : GLUES HDF5 FILES
GLUE_IN_TIME              : 1
3D_FILE                   : 1
3D_OPEN                   : 1
TIME_GROUP                : Time
BASE_GROUP                : Results
OUTPUTFILENAME            : {fout}

<<begin_list>>\n""")
        for hdf in hdfs: dat.write(hdf + "\n")
        dat.write("<<end_list>>\n<end_file>\n")
    
    # Merge files:
    run("Convert2Hdf5.exe > mohid_stdout.dat", shell=True)
    status = chklog("mohid_stdout.dat")
    chdir(curdir)

    if status > 0:
        print("[ERROR] m_supp_mohid: HDFs glue failed")
        return 1
    return 0


def convert2hdf5(mohidir: str, mdat: str, outdir: str) -> int:
    """Runs the program Convert2Hdf5.exe from MOHID.
    Relative paths are allowed.
    
    Keyword arguments
    - mohidir: path to the directory with the MOHID tool and its libraries;
    - mdat: name and path to the ConvertToHDF5Action.dat file;
    - outdir: output file directory, where the log will also be written.
    """

    # Set paths:
    curdir = getcwd()
    mohidir = path.abspath(mohidir)
    mdat = path.abspath(mdat)
    
    # Check MOHID files:
    if not path.isdir(mohidir):
        print("[ERROR] m_supp_mohid: MOHID directory not found")
        return 1
    elif not path.isfile(mdat):
        print("[ERROR] m_supp_mohid: MOHID conversion .dat file not found")
        return 1
    
    mlog = path.join(path.abspath(outdir), "mohidlog.dat")
    copyfile(mdat, path.join(mohidir, "ConvertToHDF5Action.dat"))

    mfiles = (
        "Convert2Hdf5.exe", "hdf5.dll", "hdf5dll.dll", "hdf5_f90cstub.dll",
        "hdf5_fortran.dll", "hdf5_hl.dll", "hdf5_tools.dll", "libcurl.dll",
        "libiomp5md.dll", "msvcp140.dll", "msvcr120.dll", "netcdf.dll",
        "szip.dll", "szlibdll.dll", "vcruntime140.dll", "zlib.dll",
        "zlib1.dll",
    )
    mfiles = [path.join(mohidir, file) for file in mfiles]

    for file in mfiles:
        if path.isfile(file): continue
        print("[ERROR] m_supp_mohid: MOHID file missing:", file)
        return 1

    # Change to MOHID working directory and run program:
    print("Running MOHID Convert2Hdf5...")
    chdir(mohidir)

    with open("nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT : .\\\n")
        dat.write("IN_MODEL : ConvertToHDF5Action.dat\n")

    cmd = "Convert2Hdf5.exe > " + mlog
    print(cmd)
    run(cmd, shell=True)
    chdir(curdir)

    return chklog(mlog)


def convert2nc(mohidir: str, mdat: str) -> int:
    """Runs the conversion from HDF5 to netCDF.
    
    Keyword arguments
    - mohidir: path to the directory with the MOHID tool and its libraries;
    - mdat: name and path to the Convert2netcdf.dat file.
    """

    # Set paths:
    curdir = getcwd()
    mohidir = path.abspath(mohidir)
    mdat = path.abspath(mdat)

    # Check MOHID files:
    if not path.isdir(mohidir):
        print("[ERROR] m_supp_mohid: MOHID directory not found")
        return 1
    
    copyfile(mdat, path.join(mohidir, "Convert2netcdf.dat"))

    mfiles = (
        "Convert2netcdf.exe", "hdf.dll", "hdf5.dll", "hdf5_cpp.dll",
        "hdf5_f90cstub.dll", "hdf5_fortran.dll", "hdf5_hl.dll",
        "hdf5_hl_cpp.dll", "hdf5_tools.dll", "jpeg.dll", "libcurl.dll",
        "libifcoremd.dll", "libmmd.dll", "mfhdf.dll", "msvcp120.dll",
        "msvcp140.dll", "msvcr120.dll", "netcdf.dll", "szip.dll", 
        "vcruntime140.dll", "xdr.dll", "zlib.dll", "zlib1.dll",
    )
    mfiles = [path.join(mohidir, file) for file in mfiles]

    for file in mfiles:
        if path.isfile(file): continue
        print("[ERROR] m_supp_mohid: MOHID file missing:", file)
        return 1

    # Change to MOHID working directory and run program:
    chdir(mohidir)

    with open("nomfich.dat", "w") as dat:
        dat.write("ROOT_SRT : .\\\n")
        dat.write("IN_MODEL : Convert2netcdf.dat\n")

    mlog = "mohid_stdout.dat"
    cmd = "Convert2netcdf.exe > " + mlog
    print(cmd)
    run(cmd, shell=True)
    status = chklog(mlog)
    chdir(curdir)

    return status
