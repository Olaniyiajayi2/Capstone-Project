import pandas as pd
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import logging



def covid19_table_dim(spark, output):
    return spark.read.parquet(output + "covid")


def covid19_fact_table(spark, data, output):

    df = covid19_table_dim(spark, output)

    get_date = udf(lambda x: (dt.datetime(1900, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    df = df.withColumnRenamed("Country/Region", "country") \
        .withColumnRenamed("Province/State", "province") \
        .withColumnRenamed("Last Update", "lastUpdate")


    df.createOrReplaceTempView("covid19_view")


    df = spark.sql(
        """
        SELECT *
        FROM covid19_view
        """
    )

    df = df.withColumn("lastUpdate", get_date(df.lastUpdate))

    df.write.parquet(output + "covid19_fact_tab", mode = "overwrite")


    return df



def data_quality(df, table_name):
    total_count = df.count()

    if total_count == 0:
        print("Data quality test for {} Failed!!! with zero records".format(table_name))
    else:
        print("Data quality test for {} Passed!!! with {} records".format(table_name, total_count))

    return 0


def clean_data(df):
    df = df.dropna(how = 'all')
    return df
