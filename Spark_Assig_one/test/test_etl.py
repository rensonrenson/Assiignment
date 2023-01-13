import findspark
findspark.init()
from pyspark.sql.types import StructType, StructField, StringType, LongType
import unittest
from pyspark.sql import SparkSession
from Spark_Assig_one.core.utils_code import *


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_something(self_join):
        user_Schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True)
        ])
        user_Data = [(101, "abc.123 @ gmail.com", "hindi", "mumbai"),
                     (102, "abc.123 @ gmail.com", "hindi", "usa"),
                     (103,"madan.44@gmail.com","marathi","nagpur"),
                     (104, "local.88 @ outlook.com", "tamil", "chennai")
                  ]
        user_df = self_join.spark.createDataFrame(data=user_Data, schema=user_Schema)

        transactionSchema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])
        transaction_Data = [(3300101,1000001,101,700,"mouse"),
                            (3300102,1000002,102,900,"laptop"),
                            (3300103, 1000003, 103, 34000, "tv"),
                            (3300104, 1000004, 101, 35000, "fridge")

                            ]
        transaction_df = self_join.spark.createDataFrame(data=transaction_Data, schema=transactionSchema)


        expected_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("userid", IntegerType(), True),
            StructField("price", IntegerType(), True),
            StructField("product_description", StringType(), True)

        ])

        expected_data = [(101, "abc.123 @ gmail.com", "hindi", "mumbai",3300104,1000004,101,35000,"fridge"),
                         (101, "abc.123 @ gmail.com", "hindi", "mumbai",3300101, 1000001, 101, 700, "mouse"),
                         (102, "abc.123 @ gmail.com", "hindi", "usa",3300102,1000002,102,900,"laptop"),
                         (103,"madan.44@gmail.com","marathi","nagpur",3300103,1000003,103,34000,"tv")]


#Test Case for Join Two table
        expected_df = self_join.spark.createDataFrame(data=expected_data, schema=expected_schema)
        transformed_df = joinDF(user_df,transaction_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self_join.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self_join.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))
        # Case Two
        # Test Case for Count of unique locations where each product is sold.
        expected_schema1 = StructType([

            StructField("product_description", StringType(), True),
            StructField("location", StringType(), True),
            StructField("new_count", LongType(), True)
        ])
        expected_data1 = [("mouse","mumbai",1),
                          ("laptop","usa",1),
                          ("tv","nagpur",1),
                          ("fridge","mumbai",1)]
        expected_df1 = self_join.spark.createDataFrame(data=expected_data1, schema=expected_schema1)
        transformed_df1 = count_location(transformed_df,"location","product_description")
        transformed_df1.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self_join.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self_join.assertEqual(sorted(expected_df1.collect()), sorted(transformed_df1.collect()))

        # Case Three
        # Find out products bought by each user

        expected_schema2 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("new_count", LongType(), True)
        ])
        expected_data2 = [(101, 2), (102,1),(103,1) ]
        expected_df2 = self_join.spark.createDataFrame(data=expected_data2, schema=expected_schema2)
        transformed_df2 = product_Bought(transformed_df, "userid")
        transformed_df2.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self_join.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self_join.assertEqual(sorted(expected_df2.collect()), sorted(transformed_df2.collect()))

        # Case Four
        # Total spending done by each user on each product


        expected_schema3 = StructType([

            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True),
            StructField("Total_Amount", LongType(), True)
        ])
        expected_data3 = [(101,"fridge", 35000), (101,"mouse",700), (102,"laptop",900),(103,"tv",34000)]
        expected_df3 = self_join.spark.createDataFrame(data=expected_data3, schema=expected_schema3)
        transformed_df3 = total_spending(transformed_df, "userid","product_description")
        transformed_df3.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self_join.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self_join.assertEqual(sorted(expected_df3.collect()), sorted(transformed_df3.collect()))
        #

if __name__ == '__main__':
    unittest.main()
