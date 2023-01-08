# DWL – Enabeling eMobility
Project of the module "Data Warehouse and Data Lake Systems" within the program MSc. Applied Information and Data Science at the Lucerne University of Applied Sciences and Arts (HSLU).
 
 The members of this project work are as follows:
 
 * Levin Reichmuth
 * Raphael Portmann
 * Felix Bigler
 
 # Project Description
 
In this semster project, we would like to take a closer look at the current infrastructure and utilization of charging stations in Switzerland. In doing so, we will determine the existing potential for the expansion of further charging stations by canton and regions of Switzerland for a potential charging station provider.
 
 # Project Goals
 
 Follwoing key questions shall be answered in this work:
 
 * What is the current occupancy rate of the charging stations by canton/region?
 * How high is the estimated average share of renewable energies in comparison to the estimated energy consumption at charging stations?
 *	How good is the distribution of charging stations compared to the number of electric cars by canton/region?

 
 # Repository Structure
 
 The Repository is structured according to the ressources used to set up the data lake and data warehouse architecture and to answer the project goals. The following folders are part of the repository:
 
 1. Lambda functions
 2. Jupyter notebooks
 3. SQL scripts
 4. References (if available in PDF format)

For detailed information in regard to the files contained in the specific folder, please consult the documentation below.

## Lambda functions

This folder contains all the lambda functions which have been used to develop the data piplines inside the AWS cloud. A detailed description  can be found in the final project report.

## Jupyter notebooks

This folder contains all the jupyter notebooks which where used to explore the data, ingest data into the warehouse for certain data sources as well as setting up die schema of the RDS inside the warehouse.

## SQL scripts

This folder contains the SQL statements for answering the business questions. With this statements view tables are generated inside the RDS and can be accessed directly with tableau.

## References

## Final project report

Contains the final report as pdf.
