# SMS-Coastal

under construction

This is the repository of the current version of the Simulation Management System for Coastal Operational Models (SMS-Coastal), developed at the Centre for Marine and Environmental Research (CIMA) of the University of Algarve (UAlg) in Portugal.

## Overview

SMS-Coastal is written in Python programming language and is designed to automate all the processes involved in the operational forecasting of coastal models built based on to the MOHID Modelling System ([MOHID Official repository](https://github.com/Mohid-Water-Modelling-System/Mohid)).


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
