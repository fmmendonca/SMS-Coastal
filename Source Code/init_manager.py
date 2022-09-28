# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-10-04
#
# Updated : 2021-12-20


from init_read import InitReader


def init_manager():
    """Prompts init.dat file reading and coordinates the update of the
       inputs for each operation type defined by OPTYPE keyword."""
    
    print("Reading inputs...")
    reader = InitReader()
    optype = reader.inpts.get("optype")
    
    # for all optypes (1, 2, 3, 4):
    reader.forc()
    reader.levels()
 
    # update inpts according to optype:
    if optype == "1":
        reader.siminpts()
        reader.rstday()
        reader.opfct()
        reader.oprst()

    elif optype == "3":
        reader.siminpts()
        reader.opfct()
        
    elif optype == "4":
        reader.siminpts()
        reader.oprst()
    
    return reader.inpts
