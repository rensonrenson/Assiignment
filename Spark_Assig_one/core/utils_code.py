import findspark
findspark.init()
from pyspark.sql.types import StructField, IntegerType,StructType,StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def SparkSe():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def userDF(spark):
    user_Schema = StructType([
        StructField("user_id", IntegerType(),True),
        StructField("emailid",StringType(),True),
        StructField("nativelanguage",StringType(),True),
        StructField("location",StringType(),True)
    ])
    user_DF = spark.read.option("header",True).schema(user_Schema).csv('../../res/user.csv')
    return user_DF

def transactionDF(spark):
    transactionSchema = StructType([
        StructField("transaction_id",IntegerType(),True),
        StructField("product_id",IntegerType(),True),
        StructField("userid",IntegerType(),True),
        StructField("price",IntegerType(),True),
        StructField("product_description",StringType(),True)
    ])
    transaction_DF = spark.read.option("header",True).schema(transactionSchema).csv('../../res/transaction.csv')
    return transaction_DF

def joinDF(user_DF,transaction_DF):
    joinDF = user_DF.join(transaction_DF,user_DF.user_id == transaction_DF.userid,"inner")
    return joinDF

#def count_location(join_DF,location,product_description):
    count_ul = join_DF.groupBy(product_description,location).agg(count(location).alias("new_count"))
    return count_ul
def product_Bought(join_df,userid):
    product_bought = join_df.groupBy(userid).agg(count("product_description").alias("new_count"))
    return product_bought
def total_spending(join_df,user,product):
    total_spending_price =join_df.groupBy("userid","product_description").agg(sum("price").alias("Total_Amout"))
    return total_spending_price