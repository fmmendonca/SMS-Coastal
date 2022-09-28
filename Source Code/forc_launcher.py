# Author  : Fernando Mendonça (fmmendonca@ualg.pt)
#
# Created : 2021-09-14
#
# Updated : 2022-02-05


from forc_operations import ForcOp
from forc_manager import forc_manager


def forc_launcher(inpts):
    """inpts = a copy of the dictionary with the treated inputs
       read from init.dat

       SMS-Coastal forcing layer launcher. Get the variables read by 
       init_read.py and update the keys to the forcing layer. Sort the
       sources listed by the download start time and runs each one in 
       series.    
       Parallel execution, more than one thread, besides being confusing,
       was not being used. It was also not well programmed as it presented
       errors related to the current working directory."""
    #
    # check if user defined forcing sources:
    # forc is alwayas a pandas.DataFrame, even if it is empty
    #
    forc = inpts.pop('forc')
    if forc.empty:
        return
    
    print("FORCING MANAGER\n" + '<<>>'*19)
    #
    # sort sources by start time and change index to 'src' column:
    #
    forc.sort_values("fstart", inplace=True)
    forc.set_index("src", inplace=True)
    #
    # create manager object:
    #
    manager = ForcOp(inpts)
    #
    # iterate sources:    
    #
    for src in forc.index.tolist():
        inpts.update(forc.loc[src].to_dict()); inpts.update({"src": src})
        forc_manager(inpts, manager)
        print('<<>>'*19)
