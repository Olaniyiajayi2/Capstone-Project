## Data Engineering Capstone Project

### Project Summary
The aim and objective of this project is to create an ETL pipeline for the Covid 19 World dataset, to create a database of analytics for the covid 19 pandemic outbreaks. Possible use of this database is to find patterns in different countries on the outbreak of the pandemic.
Examples of questions that this analytics database can answer are:
    * What are the trends on the number of confirmed, death, and recovered cases in each country.
    * Does the trend shows that there is possibility of 2nd wave of the pandemic in each country based on
      rising number of confirmed cases.
    * What is the recovery rate per country. The recovery rate can help organizations study countries
      with low recovery rate and learn about how they are dealing with the pandemic. And many other questions that could be asked by the general public, organizations, world leaders and countries.

#### This project is in two part
    * This project is in two parts
        # part 1
            * part 1 entails creating of a pipeline to create dimension tables. From the main covid 19 data, it consist of features that has been taking on the covid19 pandemic from different country. The goal of this part is to create a pipeline to extract data exclusive to 4 countries; china, US, India, and Russia.
            *code explanation
                * conf.cfg :- contains configurations and AWS credentials
                * etl_utility_and_check.py :- though this code was unused in this project, it contains helper codes should the case arise.
                * etl :- this is the code that contains the ETL pipeline
                * run_pieline.ipynb :- this notebook is used to run the pipeline

          # part 2
            * part 2 entails moving the covid 19 dataset from AWS S# to AWS Redshift for analysis, and creating each dimension table in redshift
            *code explanation
                * conf.cfg :- contains configurations and AWS credentials
                * create_tables.py :- this code create the fact and dimension tables
                * sql_queries.py :- this code is where scripts for droppi g table and creating tables are written.
                * etl :- this is the code that contains the ETL pipeline
                * programmatic_access: this notebook contains code for creating the aws infrastructure via code.
                * run_pieline.ipynb :- this notebook is used to run the pipeline


### Tecnologies used in this project
* AWS Redshift
* AWS S3
* Apache Spark
* PostgreSQL
* configparser
* Python 3

## Data and Code
 The Data for this project was gotten from kaggle https://www.kaggle.com/sudalairajkumar/novel-corona-virus-2019-dataset?select=time_series_covid_19_confirmed_US.csv.
 All the data were loaded to AWS S3 before starting the project.

 * ### Code
     * The code in this project are in two segments. one was



### Data Model
* Conceptual data model
![Database_schema](covid_table.jpeg)


### Data Quality Check
    * Integrity check:- Integrity check checks if each dimension dataframe contains only one type if unique value
    * Count check:- Checks if there are values in the dataframes.
    
    
### Data Dictionary
    * Fact Table - This contains information from the covid 19 main dataset. 
    Columns:-
        * SNo: Serial number, representing the index in the dataframe
        * ObservationDate: Date of the observation in MM/DD/YYYY
        * Province/State - Province or state of the observation (Could be empty when missing)
        * Country/Region - Country of observation (contains all 226 countries where the observation was taken)
        * Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
        * Confirmed - Cumulative number of confirmed cases till that date
        * Deaths - Cumulative number of of deaths till that date
        * Recovered - Cumulative number of recovered cases till that date
        
        * Dimension table 1 - This contains information for observations taken only in the US. 
    Columns:-
        * SNo: Serial number, representing the index in the dataframe
        * ObservationDate: Date of the observation in MM/DD/YYYY
        * Province/State - Province or state of the observation (Could be empty when missing)
        * Country/Region - Country of observation (contains only observations made in US)
        * Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
        * Confirmed - Cumulative number of confirmed cases till that date
        * Deaths - Cumulative number of of deaths till that date
        * Recovered - Cumulative number of recovered cases till that date
        
        * Dimension table 2 - This contains information for observations taken only in the China. 
    Columns:-
        * SNo: Serial number, representing the index in the dataframe
        * ObservationDate: Date of the observation in MM/DD/YYYY
        * Province/State - Province or state of the observation (Could be empty when missing)
        * Country/Region - Country of observation (contains only observations made in China)
        * Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
        * Confirmed - Cumulative number of confirmed cases till that date
        * Deaths - Cumulative number of of deaths till that date
        * Recovered - Cumulative number of recovered cases till that date
        
        * Dimension table 3 - This contains information for observations taken only in the India. 
    Columns:-
        * SNo: Serial number, representing the index in the dataframe
        * ObservationDate: Date of the observation in MM/DD/YYYY
        * Province/State - Province or state of the observation (Could be empty when missing)
        * Country/Region - Country of observation (contains only observations made in India)
        * Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
        * Confirmed - Cumulative number of confirmed cases till that date
        * Deaths - Cumulative number of of deaths till that date
        * Recovered - Cumulative number of recovered cases till that date
        
        
        * Dimension table 2 - This contains information for observations taken only in the Russia.  
    Columns:-
        * SNo: Serial number, representing the index in the dataframe
        * ObservationDate: Date of the observation in MM/DD/YYYY
        * Province/State - Province or state of the observation (Could be empty when missing)
        * Country/Region - Country of observation (contains only observations made in Russia)
        * Last Update - Time in UTC at which the row is updated for the given province or country. (Not standardised and so please clean before using it)
        * Confirmed - Cumulative number of confirmed cases till that date
        * Deaths - Cumulative number of of deaths till that date
        * Recovered - Cumulative number of recovered cases till that date
    

#### Running the ETL
The ETL pipeline is defined in the etl.py scripts, and running the run__.ipynb executes the pipeline
> refer to the jupyter notebook
