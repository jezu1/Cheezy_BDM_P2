# Cheezy_BDM_P2

## Table of Contents
1. [General Info](#general-info)
2. [Set up and requirements](#set-up-and-requirements)
3. [Formatted Zone](#formatted-zone)
4. [Exploitation Zone](#exploitation-zone)

### General Info
***
This git repository is part of the university project P2 Formatted and Exploitation Zones implemented in the course of Big Data Management at UPC barcelona. The python scripts move data needed for our own startup idea Cheezy to a formatted Zone implemented as a datawarehouse. Afterwards part of the data is moved to the exploitation zone implemented as a property graph in Neo4J.

## Set up and requirements
***
A virtual machine on OpenNebula with the following specifications is used for this project: 
50 gb GB and OS - Ubuntu 18.04.3 LTS

Other technologies used within the project:
* Hadoop File System: Version 3.3.2 
* Python: Version 3.9.7
* Spark: Version 3.3.0
* Delta Lake: Version 2.1.0 
* Neo4J Community: Version 4.4.20

## Formatted Zone
***
In the following is briefly described which function which of the python scripts have when creating the data warehouse (our formatted zone):

* Warhouse_create_google_trip.py: Merge and clean the data that was retrieved from google and tripdadvisor in the previous part of the project. Afterwards create the initial datawarehouse and move the cleaned and merged data to the data warehouse. **This script needs to be run first during the creation of the date warehouse.**
* Warehouse_create_google_details.py: Move the data retrieved from the Google places details API calls to the data warehouse: This data includes the google reviews and the opening hours of the restaurants.
* Warehouse_update_google_trip.py: Merge and clean the data retrieved from google and tripadviors and determine which information is new and needs to be added or updated  in the data warehouse and which information needs to be removed from the data warehouse. **This script needs to be run first during the update of the data warehouse.**
* Warehouse_update_google_details.py: Update the data warehouse with the data that is retrieved from calling the google places details API. This data includes google reviews and the opening hours of a restaurant.
* Warehouse_external_images.py: Move the filepaths of the external google and tripadvisor images to the data warehouse (script used for both the initial creation and the update of the data warehouse).

## Exploitation Zone
***
Put information for dashboard and graph here
