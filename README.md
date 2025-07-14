# SMS-Coastal

The Simulation Management System for Coastal Operational Models (SMS-Coastal) is a Python-based program built to run and control operational forecast simulations of MOHID-based ([MOHID oficial GitHub](https://github.com/Mohid-Water-Modelling-System)) applications. The program was formally presented in 2023, in the Journal of Marine Science and Engineering by MDPI: https://doi.org/10.3390/jmse11081606.

## Repository content

- `dev`: development version of SMS-Coastal;

- `source`: current version of SMS-Coastal;

- `envmake.yml`: YAML file containing all the modules needed to run SMS-Coastal in an anaconda/miniconda environment.

## How to use SMS-Coastal

SMS-Coastal needs Python (version >= 3.10) and the modules listed in the `envmake.yml` file. A working environment for the program can be easily created with `conda`. To install Anaconda or Miniconda visit [Installing conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) from Conda website.

After downloading or cloning this repository, open the installed `conda` shell on the destination folder and run the following:

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

Add anaconda to Path in system environment variables.  
For a standard installation on Windows the paths should be:  
C:\Users\username\AppData\Local\miniconda3\condabin  
C:\Users\username\AppData\Local\miniconda3\Scripts  
This will enable cmd to call anacaonda exes.  

Create a .bat file to call sms-coastal and its respective conda environment.  
Write the following lines in the file:  
call activate hidrotec && python .\program_main.py  
timeout /t 86400  

