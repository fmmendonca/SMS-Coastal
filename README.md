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
