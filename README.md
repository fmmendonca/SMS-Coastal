# SMS-Coastal

The Simulation Management System for Coastal Operational Models (SMS-Coastal) is a Python-based program built to run and control operational forecast simulations of MOHID-based ([MOHID oficial GitHub](https://github.com/Mohid-Water-Modelling-System)) applications. The program was formally presented in 2023, in the Journal of Marine Science and Engineering by MDPI: https://doi.org/10.3390/jmse11081606.

## Repository content

- `dev`: development version of SMS-Coastal;

- `source`: current version of SMS-Coastal;

- `envmake.yml`: YAML file containing all the modules needed to run SMS-Coastal in an anaconda/miniconda environment.

This repository does not include MOHID executables and libraries, which are required to run SMS-Coastal. They can be obtained in MOHID official channels.

## How to use SMS-Coastal

SMS-Coastal needs Python (version >= 3.10) and the modules listed in the `envmake.yml` file. A working environment for the program can be easily created with `conda`. To install Anaconda or Miniconda visit [Installing conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) from Conda website.

After downloading or cloning this repository, open the installed `conda` terminal in the destination folder and run the following:

```
conda update conda
conda env create --file envmake.yml
```

The last will create an environment called `smsc`, with all the necessary dependencies, in the `conda` installation. After that, to run SMS-Coastal, change the working directory to the `./source` folder and run:

```
conda activate smsc
python ./program_main.py
```

### Schedule SMS-Coastal

On computers running the operating system Windows, the easiest way to schedule a task is by creating a `.bat` file and link it in Task Scheduler. SMS-Coastal can be scheduled to run using the file `.\source\run_smsc.bat`. To run this file correctly, Conda’s scripts and binaries must be registered in the system’s `Path` variable. For a standard Miniconda installation, the required paths are:

```
C:\Users\<username>\AppData\Local\miniconda3\condabin
C:\Users\<username>\AppData\Local\miniconda3\Scripts
```
Find the installation paths in your computer and ensure they are added to the system's `Path` variable, otherwise the Windows terminal won't recognize Conda commands when `.bat` file is called.
