# SMS-Coastal

The **Simulation Management System for Coastal Operational Hydrodynamic Models** (SMS-Coastal) is a Python-based program designed to run and manage operational forecast simulations using the [MOHID System](https://github.com/Mohid-Water-Modelling-System/Mohid). It is composed of modular components that handle the full simulation workflow, including:

- Downloading and processing boundary conditions (e.g., from [CMEMS](https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description))
- Managing simulation execution
- Post-processing model outputs

This `README.md` provides a step-by-step guide on how to use SMS-Coastal. It explains how to configure the initialization file and describes the functionality of each module and setup. Users are expected to have:

- Basic knowledge of Python
- Experience working with conda environments
- Familiarity with the file and directory structure of MOHID-based applications

When referencing SMS-Coastal, please use the following citation:

> Mendonça, F., Martins, F., & Janeiro, J. (2023). SMS-Coastal: A new Python tool to manage MOHID-based coastal operational models. *Journal of Marine Science and Engineering, 11(8)*, 1606.
https://doi.org/10.3390/jmse11081606

# Environment Setup

SMS-Coastal was developed and tested on Windows systems. However, once all required dependencies are installed, it should also run on Linux systems.

The application is designed to run within a `conda` environment. For Conda installation refer to the [Anaconda/Miniconda installation guide](https://www.anaconda.com/docs/getting-started/miniconda/install#quickstart-install-instructions).

SMS-Coastal requires **Python 3.12 or higher** in a Conda environment containing the following packages:

- `ruamel.yaml`
- `scipy`
- `numpy`
- `pandas`
- `h5py`
- `netCDF4`
- `xarray`
- `pygrib`
- `requests`
- `paramiko`
- `copernicusmarine`

This repository includes an `envmake.yml` file to automatically create a Conda environment named `smsc` with all required dependencies. After downloading the `.yml` file or cloning SMS-Coastal repository, open the **Anaconda Prompt** or **Anaconda PowerShell Prompt** in the directory where the file is located and run the following commands:

```powershell
# Update conda to the latest version:
(base) PS C:\Users\modeler\Downloads> conda update conda
*** command outputs ***

# Create the environment from the YML file:
(base) PS C:\Users\modeler\Downloads> conda env create --file envmake.yml
*** command outputs ***

# Activate the environment:
(base) PS C:\Users\modeler\Downloads> conda activate smsc
(smsc) PS C:\Users\modeler\Downloads>
```

From this point onward, the directory path in the prompt is omitted for simplicity, unless it is necessary to display it:
```powershell
(smsc) PS C:\Users\modeler>
```
Becomes:
```powershell
(smsc) >
```

# General Use

After downloading or cloning the SMS-Coastal repository, activate the corresponding Conda environment and execute the main program:

```powershell
(smsc) > python .\program_main.py
```

In the last line, SMS-Coastal was launched in the current working directory. However, to organize simulation cases, the application can also be called from a different folder: 

```powershell
# Create a folder to store model data:
(smsc) > mkdir my_model
# Enter the folder:
(smsc) > cd my_model
# Call SMS-Coastal to run inside 'my_model':
(smsc) > python ..\program_main.py
```

At startup, SMS-Coastal checks for the presence of the initialization file (`initsmsc.yml`) in the current working directory (e.g., `my_model`). This file contains all user-specified parameters required to select and run operations. It follows the [YAML](https://yaml.org/) format and is structured as follows:

```yaml
OPERATION_1:
  ENABLED: !!bool TRUE
  TYPE: !!str "operation 1 name"
  ...

OPERATION_2:
  ENABLED: !!bool TRUE
  TYPE: !!str "operation 2 name"
  ...

...

OPERATION_n:
  ENABLED: !!bool TRUE
  TYPE: !!str "operation n name"
  ...
```

SMS-Coastal processes operations sequentially, from `OPERATION_1` through `OPERATION_n`. Each operation is defined by a corresponding `OPERATION_i` keyword, where `i` denotes its numeric index. The `ENABLED` parameter controls whether a given operation is executed, allowing operations to be temporarily disabled without removing their configuration. Each operation is associated with a unique name defined by the `TYPE` parameter. The available operation types supported by SMS-Coastal are described in the following sections.

## Operation Dates
Each operation in SMS-Coastal requires a defined time period for execution, which is specified using the parameters `OPDATE` and `DTDAYS` in the initialization file.

The base temporal unit in SMS-Coastal is one day, which therefore defines the minimum duration of an operation. The start date is defined by the `OPDATE` parameter, while the duration (in days) is specified by `DTDAYS`:
```yaml
OPERATION_1:
  ENABLED: !!bool TRUE
  TYPE: !!str "sample operation"
  OPDATE: !!str "2026-04-17"
  DTDAYS: !!int 3
  ...
```

In practice, SMS-Coastal internally represents the operation period as a range derived from `OPDATE` and `DTDAYS`. For example, the configuration above is interpreted as:
```python
dtdays = [0, 3]
dates  = ["2026-04-17", "2026-04-20"]
```

`DTDAYS` can also assume a negative value, indicating a backward time range in which the end date is defined by `OPDATE`:
```yaml
OPERATION_1:
  ENABLED: TRUE
  TYPE: "sample operation"
  OPDATE: "2026-04-17"
  DTDAYS: -2
  ...
```

In this case, the configuration is interpreted as:
```python
dtdays = [-2, 0]
dates  = ["2026-04-15", "2026-04-17"]
```

>**Note**:
>
>Since SMS-Coastal is designed for operational use, the `OPDATE` parameter can assume the value "today". In this case, the current system date is used at runtime, allowing operations to be executed relative to the day of execution.
