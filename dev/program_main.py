from os import path

from ruamel.yaml import YAML

from dev.m_boundary_cmems import cmems


def main() -> None:
    # #######################################################################
    # Check initialization file:
    ylfile = "initsmsc.yml"

    if not path.isfile(ylfile):
        print("[ERROR]", __name__ + ":")
        print("\tYAML initialization file not found.")
        raise SystemExit

    # Read initialization file:
    yl = YAML(typ="safe")

    try:
        with open("initsmsc.yml", "r") as dat:
            inpts = yl.load(dat)
        inpts: dict
    except Exception as err:
        print("[ERROR]", __name__ + ":")
        print("\tInvalid YAML inintialization file.")
        print(err)
        raise SystemExit
    
    # #######################################################################
    # Iterate operations defined in the initialization file:
    opn = 1
    prms = inpts.pop(f"OPERATION_{opn}", {})

    while prms:
        print("*"*78)
        
        # Check operation:
        if not prms.pop("ACTIVE", False):
            print(f"OPERATION_{opn}: [DISABLED]")
            opn += 1
            prms = inpts.pop(f"OPERATION_{opn}", {})
            continue

        # Check type:
        optype = prms.pop("TYPE", "").lower()
        print(f"OPERATION_{opn}:", optype)

        match optype:
            case "simulation":
                print("Running a simulation")
            case "cmems":
                cmems(prms)
            case "skiron":
                print("Doing something with Skiron")
            case _:
                print(f"[ERROR]", __name__ + ":")
                print("\tOperation not defined in SMS-Coastal library.")
                raise SystemExit

        # Loop control variables:
        opn += 1
        prms = inpts.pop(f"OPERATION_{opn}", {})
    

if __name__ == "__main__":
    main()
