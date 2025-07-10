# SMS-Coastal

The Simulation Management System for Coastal Operational Models (SMS-Coastal) is a Python-based program built to run and control operational forecast simulations of MOHID-based ([MOHID oficial GitHub](https://github.com/Mohid-Water-Modelling-System)) applications. The first version of the program was presented in 2023, in the Journal of Marine Science and Engineering by MDPI: https://doi.org/10.3390/jmse11081606.

SMS-Coastal is being used to manage the forecasts of operational models at the Centre for Marine and Environmental Research of the University of Algarve (CIMA UAlg) in Faro, Portugal:
- Algarve Operational Modeling and Monitoring System ([SOMA](https://soma.ualg.pt/))
- Basin Sea Interactions with Communities ([BASIC](http://bahiacartagena.omega.eafit.edu.co/))

## Repository content

## How to use SMS-Coastal

Add anaconda to Path in system environment variables.  
For a standard installation the paths should be:  
C:\Users\username\AppData\Local\miniconda3\condabin  
C:\Users\username\AppData\Local\miniconda3\Scripts  
This will enable cmd to call anacaonda exes.  

Create a .bat file to call sms-coastal and its respective conda environment.  
Write the following lines in the file:  
call activate hidrotec && python .\program_main.py  
timeout /t 86400  

Open miniconda or anaconda PowerShell, change to the directory where the file smsc_environment.yml is and run:  
>>> conda update conda  
>>> conda env create --file smsc_environment.yml  
