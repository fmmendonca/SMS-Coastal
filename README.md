# SMS-Coastal

The **Simulation Management System for Coastal Operational Hydrodynamic Models** (SMS-Coastal) is a Python-based program designed to run and manage operational forecast simulations using the [MOHID System](https://github.com/Mohid-Water-Modelling-System/Mohid). It is composed of modular components that handle the full simulation workflow, including:

- Downloading and processing boundary conditions (e.g., from [CMEMS](https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description))
- Managing simulation execution
- Post-processing model outputs

This ```README.md``` provides a step-by-step guide on how to use SMS-Coastal. It explains how to configure the initialization file and describes the functionality of each module and setup. Users are expected to have:

- Basic knowledge of Python
- Experience working with conda environments
- Familiarity with the file and directory structure of MOHID-based applications

When referencing SMS-Coastal, please use the following citation:

> Mendonça, F., Martins, F., & Janeiro, J. (2023). SMS-Coastal: A new Python tool to manage MOHID-based coastal operational models. *Journal of Marine Science and Engineering, 11(8)*, 1606.
https://doi.org/10.3390/jmse11081606
