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

* **Warhouse_create_google_trip.py**: Merge and clean the data that was retrieved from google and tripdadvisor in the previous part of the project. Afterwards create the initial datawarehouse and move the cleaned and merged data to the data warehouse. **This script needs to be run first during the creation of the date warehouse.**
* **Warehouse_create_google_details.py**: Move the data retrieved from the Google places details API calls to the data warehouse: This data includes the google reviews and the opening hours of the restaurants.
* **Warehouse_update_google_trip.py**: Merge and clean the data retrieved from google and tripadviors and determine which information is new and needs to be added or updated  in the data warehouse and which information needs to be removed from the data warehouse. **This script needs to be run first during the update of the data warehouse.**
* **Warehouse_update_google_details.py**: Update the data warehouse with the data that is retrieved from calling the google places details API. This data includes google reviews and the opening hours of a restaurant.
* **Warehouse_external_images.py**: Move the filepaths of the external google and tripadvisor images to the data warehouse (script used for both the initial creation and the update of the data warehouse).
* **Warehouse_deletion.py**: Remove restaurants that are marked as closed from the data warehouse.
* **dw_Cheezy**: This code is run after transferring all cheezy data tables into the persistent landing zone. This then produces cleaned and normalized tables and saves to the data warehouse.

## Exploitation Zone
***

* **datamart_dashboard**: This code is run after the data warehouse tables for Cheezy and restaurants are created. This then aggregates data into monthly or daily statistics per restaurant and saves into the datamarts folder for the dashboard to use.
* 
* **dashboard.py**: This is a streamlit application that connects to the Delta Lake datamart using via Spark. To run this, streamlit 1.12 must be installed. To run, navigate to the folder path where this file is located and enter the following in the command line: "streamlit run dashboard.py" This should then show the url in which you can access the dashboard from your browser.

#### 4.1 Recommender
The file **recommender.cql** includes all the cypher queries to create the recommender for our Graph in Neo4J. To be able to execute all the queries the Neo4J Graph Data Science Library muss be installed.



