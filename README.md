# SMS-Coastal

[CONSTRUCTION SITE]

The Simulation Management System for Coastal Operational Models (SMS-Coastal) is a Python-based program built to run and control operational forecast simulations of MOHID-based ([MOHID oficial GitHub](https://github.com/Mohid-Water-Modelling-System)) applications.

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
