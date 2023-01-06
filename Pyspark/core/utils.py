import findspark
findspark.init()
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import re
from functools import reduce

from pyspark.sql import SparkSession

def sparkSession():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def converyDF(spark):
    fileschema = StructType([
        StructField("ProductName",StringType(),True),
        StructField("IssueDate",StringType(),True),
        StructField("Price",IntegerType(),True),
        StructField("Brand",StringType(),True),
        StructField("user_Country",StringType(),True),
        StructField("Productnumber",IntegerType(),True)
            ])
    filepath = spark.read.option("header",True).schema(fileschema).csv('../../res/timestamp.csv')
    return filepath

#b)	Convert timestamp to date type
def dateConvert(df):
    converyDate = df.withColumn("equal_time",F.from_unixtime(F.col("IssueDate") / 1000))\
    .withColumn("date",F.date_format(col("equal_time"),"MM-dd-yyyy"))
    return converyDate
# Remove the starting extra space in Brand column for LG and Voltas fields
def remove_space(df):
    remove_Space_Brand = df.withColumn("NewBrand", F.trim(col("Brand"))).show()
    return remove_Space_Brand

# Replace null values with empty values in Country column
def removeNull(df,Country):
    remove_Null_value =df.withColumn("Country", F.when(col("user_Country")=="null","").otherwise(col("user_Country")))
    return remove_Null_value


def trans_Schema(spark):
 transschema = StructType([
     StructField("SourceId",IntegerType(),True),
     StructField("TransactionNumber",IntegerType(),True),
     StructField("country",StringType(),True),
     StructField("ModelNumber",IntegerType(),True),
     StructField("StartTime",StringType(),True),
     StructField("ProductNumber",IntegerType(),True)
])
 trans_DF =spark.read.option("header",True).schema(transschema).csv('../../res/transaction_details.csv')
 return trans_DF

#Change the camel case columns to snake case
def convertsnake_case(df):
    column_name_list = df.columns
    df_column_name = reduce(lambda df1, i: df1.withColumnRenamed(i, re.sub(r'(?<!^)(?=[A-Z])', '_', i).lower()),
                    column_name_list, df)
    return df_column_name

# Add another column as start_time_ms and convert the values of StartTime to milliseconds
def converyMilliSec(df,timestamp):
    convert_milli_sec = df.withColumn(timestamp, F.to_timestamp(col("StartTime")))\
        .withColumn("millisecond", F.unix_timestamp(col(timestamp)))
    return convert_milli_sec

# Combine both the tables based on the Product Number
def joinDF(df,df1):
    join_DF_ProNum = df.join(df1,df["Productnumber"] == df1["ProductNumber"],"left")
    return join_DF_ProNum

# get the country as EN
def filterBYEN(df,country):
    filter_Country_En = df.filter(col(country) =="EN")
    return filter_Country_En