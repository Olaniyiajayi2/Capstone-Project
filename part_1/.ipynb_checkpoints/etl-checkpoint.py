import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import col, hour, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id

from etl_utility_and_check import *


config = configparser.ConfigParser()
config.read('conf.cfg')


os.environ['AWS_ACCESS_KEY'] = config['AWS']['AWS_ACCESS_KEY']
os.environ['AWS_ACCESS_SECRET'] = config['AWS']['AWS_ACCESS_SECRET']

def create_session():
    """
    This function creates a spark session to process all the data.
    Returns:
        spark :- Apache Spark Session.
    """    
    spark = SparkSession.builder \
            .appName("Capstone Project") \
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .enableHiveSupport() \
            .getOrCreate()
    return spark



def covid_data_processing(spark, inputData, outputData, identity):
    """
    This function loads the covid19 csv data from the inputData path, process the data
        to extract out the 4 tables; Us covid19 data, china covid19 data, Russia covid19 data, and India         covid19 data, and store the data into a parquest files.
        Parameters:
        ------------------
        spark: An Apache Spark Session.
        
        inputData: path to the input csv data
        
        outputData: path to the output data
        
        identity: parquest file extention
    """
    
    start_time = datetime.now()
    
    covidData = inputData
    print("Processing Covid19 data...!!!")
    
    print("Reading in files {}".format(covidData))
    
    df_covid = spark.read.csv(covidData, header = True, inferSchema = True)
    stop_time = datetime.now()
    
    total_time = stop_time - start_time
    print("Total time taken to lead data is {} ...".format(total_time))
    print("Covid19 data schema:")
    df_covid.printSchema()
    print("[INFO]... renaming columns")
    df_covid = df_covid.withColumnRenamed('Province/State', 'state') \
    .withColumnRenamed('Country/Region', 'country') \
    .withColumnRenamed('Last Update', 'lastUpdate')
    print("Columns renamed")
    df_covid.printSchema()
    print("-----------[INFO]--------------")
    print('lenght of dataframe {}'.format(df_covid.count()))
    print("Store covid data in a paraquet: ")
    df_covid.createOrReplaceTempView("covid_table")
    
    #------- Extracting Columns for country's table ----------#
    # Extracting Us covid 19 table
    
    print("Extracting Us covid 19 table...")
    us_covid = spark.sql("""
                        SELECT 
                        ObservationDate, 
                        state, 
                        country, 
                        lastUpdate, 
                        Confirmed, 
                        Deaths, 
                        Recovered
                        FROM covid_table
                        WHERE country = 'US'                   
                        """)
    print("us table schema:")
    us_covid.printSchema()
    
    print('lenght of dataframe {}'.format(us_covid.count()))
    print("example of us_covid table")
    us_covid.show(n = 5, truncate = False)
    
    print("Performing quality check")
    data_quality(spark, us_covid)
    integrity_check(spark, us_covid, "US")
    
    # write us_covid table to a parquet file
    us_covid_table_path = outputData + 'us_table' + "_" + identity
    print("Writing table parquest file to {}".format(us_covid_table_path))
    us_covid.write.mode("overwrite").parquet(us_covid_table_path)
    #end_time1 = datetime.now
    #done = end_time1-start_time
    #print("Finished processing and writing table in {}".format(done))
    print("Done")
    
   
    # Extracting China covid 19 table
    
    print("Extracting china covid 19 table...")
    china_covid = spark.sql("""
                        SELECT 
                        ObservationDate, 
                        state, 
                        country, 
                        lastUpdate, 
                        Confirmed, 
                        Deaths, 
                        Recovered
                        FROM covid_table
                        WHERE country = "Mainland China"                        
                        """)
    print("china table schema:")
    china_covid.printSchema()
    
    print('lenght of dataframe {}'.format(us_covid.count()))
    print("example of china_covid table")
    china_covid.show(n = 5, truncate = False)
    
    print("Performing quality check")
    data_quality(spark, china_covid)
    integrity_check(spark, china_covid, "Mainland China")
    
    # write us_covid table to a parquet file
    china_covid_table_path = outputData + 'china_table' + "_" + identity
    print("Writing table parquest file to {}".format(china_covid_table_path))
    china_covid.write.mode("overwrite").parquet(china_covid_table_path)
    #end_time2 = datetime.now
    #print("Finished processing and writing table in {}".format(end_time2-start_time))
    
    
    # Extracting india covid 19 table
    
    print("Extracting india covid 19 table...")
    india_covid = spark.sql("""
                        SELECT 
                        ObservationDate, 
                        state, 
                        country, 
                        lastUpdate, 
                        Confirmed, 
                        Deaths, 
                        Recovered
                        FROM covid_table
                        WHERE country = "India"                        
                        """)
    print("india table schema:")
    india_covid.printSchema()
    
    print('lenght of dataframe {}'.format(us_covid.count()))
    print("example of india_covid table")
    india_covid.show(n = 5, truncate = False)
    
    print("Performing quality check")
    data_quality(spark, india_covid)
    integrity_check(spark, india_covid, 'India')
    
    # write us_covid table to a parquet file
    india_covid_table_path = outputData + 'india_table' + "_" + identity
    print("Writing table parquest file to {}".format(india_covid_table_path))
    india_covid.write.mode("overwrite").parquet(india_covid_table_path)
    #end_time3 = datetime.now
    #print("Finished processing and writing table in {}".format(end_time3-start_time))
    
    
    # Extracting russia covid 19 table
    
    print("Extracting russia covid 19 table...")
    russia_covid = spark.sql("""
                        SELECT 
                        ObservationDate, 
                        state, 
                        country, 
                        lastUpdate, 
                        Confirmed, 
                        Deaths, 
                        Recovered
                        FROM covid_table
                        WHERE country = "Russia"                        
                        """)
    print("russia table schema:")
    russia_covid.printSchema()
    
    print('lenght of dataframe {}'.format(us_covid.count()))
    print("example of russia_covid table")
    russia_covid.show(n = 5, truncate = False)
    
    print("Performing quality check")
    data_quality(spark, russia_covid)
    integrity_check(spark, russia_covid, "Russia")
    
    # write us_covid table to a parquet file
    russia_covid_table_path = outputData + 'russia_table' + "_" + identity
    print("Writing table parquest file to {}".format(russia_covid_table_path))
    russia_covid.write.mode("overwrite").parquet(russia_covid_table_path)
    #end_time = datetime.now
    #print("Finished processing and writing table in {}".format(end_time-start_time))
    
    
    return us_covid, china_covid, india_covid, russia_covid


# Performing data quality check
def data_quality(spark, table):
    """
    This function performs quality check on table
    Parameters:
    ---------------
    spark: An Apache spark session
    
    table: table to confirm it's quality
    """
    #table_df = table.toPandas()
    table_df = table
    
    if table_df.count() == 0:
        print("[INFO] Data quality check failed for {} table with Zero records".format(table))
    else:
        print("[INFO] Data quality check passed for {} with {} record".format(table, table.count()))
        

    return None

# Performs integrity check, a form of quality check
def integrity_check(spark, dimension_data, value_to_check):
    '''
    This function performs integrity check on each dimension table. 
    It confirms if the dimension data belongs to only one country.
    Parameters:
    ----------------
    spark: An Apache spark session
    
    dimension_data: The dimension data to perform integrity check on
    
    value_to_check: The value to check for in the table
    '''
    data = dimension_data.toPandas()
    l = len(data["country"].unique())
    if l > 1:
        print("[INFO] Integrity check for {} dataframe failed".format(value_to_check))
    else:
        print("[INFO] Integrity check Passed!!!")

def main():
    
    
    start = datetime.now()
    identity =  datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')

    print("ETL pipeline started at {}".format(start))
    
    spark = create_session()
    #inputData = ""
    #outputData = ""
    
    inputData = config['S3']['INPUT_DATA']
    outputData = config['S3']['OUTPUT_DATA']
    
    us_covid, china_covid, india_covid, russia_covid = covid_data_processing(spark, \
                                                                             inputData, \
                                                                             outputData, \
                                                                             identity)
    print("ETL process finished running")
    print("DONE")
    
    
    
if __name__ == "__main__":
    main()