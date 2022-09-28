# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-10-08
#
# Updated : 2021-10-08


def readkey(smsc, key, std=None):
    """smsc = SMS-Coastal folder
       key = keyword to be read in init.dat
       std = standard values if keyword is not found"""
    
    data = std
    dat = open(smsc + "\\init.dat", "r")
    line = dat.readline()
    
    while line:
        cond_c = line[0] == "!"
        line = line.strip()
        cond_a = not line 
        cond_b = ":" not in line
        cond_d = key not in line
        
        # skip lines:
        if cond_a or cond_b or cond_c or cond_d:
            line = next(dat, None)
            continue
        
        data = line.split(':', maxsplit=1)[1].strip()
        line = None
    
    dat.close()
    return data
