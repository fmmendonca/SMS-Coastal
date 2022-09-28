# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-09-14
#
# Updated : 2022-02-05


from os import path, makedirs
from datetime import datetime, timedelta
from time import sleep

from support_mailing import mailreport
from post_special import convertbasicforc


def forc_manager(forc_inpts, manager):
    """forc_inpts = the dictionary with the inputs read from init.dat
       manager = forc_operations.ForcOp object
       
       Coordinates and manages the processes defined for a single 
       source of forcing data, namely: download, special operations
       and interpolation and/or conversion to time series.
        
       Variables in inpts: 'opdate', 'fin', 'MAILTO', 'CMEMS',
       'SPECIAL', 'src', 'hdfintp', 'tsconv', 'fstart', 'tsloc'."""
    #
    # update manager object attributes with source properties:
    #
    manager.updateattrs(forc_inpts.get("src"), forc_inpts.get("fin"))
    manager.prodinfo()
    mesg = path.basename(manager.smsc) + " " + manager.src + " Process "
    print("\nFORCING SOURCE\n" + manager.src, end="\n\n")
    #
    # check folders and log:
    #
    dirs = "\\Download", "\\Conversion", "\\Interpolation", "\\Data"
    dirs = [manager.root + folder for folder in dirs]
    for folder in dirs:
        if not path.isdir(folder):
            makedirs(folder)

    log = manager.root + "\\run.log"
    if not path.isfile(log):
        with open(log, "w") as entry:
            entry.write("Date;Time;Status\n")
    #
    # log entry:
    #
    opdate = forc_inpts.get('opdate')
    with open(log, "a") as entry:
        entry.write(str(opdate) + ";")
    #
    # run backup:
    #
    manager.src_backup()
    #
    # wait for data availability:
    # todays's date at 00:00:
    #
    fstart = datetime.fromordinal(manager.today.toordinal())
    fstart = fstart + timedelta(hours=int(forc_inpts.get("fstart")))
    # in seconds:
    fstart = int((fstart - datetime.today()).total_seconds() + 1)
    
    if manager.fin >= manager.today and fstart > 0:   
        print("Waiting for", manager.src, "data availability...")
        
        # loop through each second in fstart:
        while fstart > 0:
            print(fstart, "second(s) remaining", end="\r")
            sleep(1)
            fstart -= 1
            print(" "*80, end="\r")            
        print("\n")
    #
    # download data:
    #
    mail = forc_inpts.get("MAILTO", "")
    status = manager.download()
    if not status:
        with open(log, "a") as entry:
            entry.write(str(datetime.today()) + ";ERR01\n")
        body = str(opdate) + "\nDownload failed."
        mailreport(mail, mesg + "FAILED", body, ())
        return
    #
    # run special operations:
    #
    if "BASIC_THREDD" in forc_inpts.get("SPECIAL", ""):
        convertbasicforc(manager.root)
    #
    #
    #
    # intp = int(inpts.get('hdfintp'))  # interpolation switch
    tsconv = int(forc_inpts.get('tsconv'))  # time series switch
    #
    # save download:
    #
    # if hdfinpt + tsconv < 1:
    #     manager.save_download()
    #     with open(log, "a") as entry:
    #         entry.write(str(datetime.today()) + ";1\n")
    #     print("B send email", mail)
    #     return 
    
    #
    # extract time series:
    #
    if tsconv > 0:
        manager.tsconv(forc_inpts.get("tsloc"), opdate)
    #
    # end operations for one source:
    #
    with open(log, "a") as entry:
        entry.write(str(datetime.today()) + ";1\n")
    body = str(opdate) + "\nOperation completed."
    mailreport(mail, mesg + "COMPLETED", body, ())
    