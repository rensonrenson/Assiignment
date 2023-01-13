import findspark
findspark.init()
import unittest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from Spark_Assig_two.core.utils_code import *


class SparkETLTestCase(unittest.TestCase):

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

    def test_etl(self):
        input_Schema = StructType([
            StructField("Logging", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("ghtorrent", StringType(), True),
            StructField("api_client", StringType(), True),
            StructField("url", StringType(), True)
        ])

        input_data = [("DEBUG", "2017-03-23","ghtorrent-40","ghtorrent.rb:","Repo EFForg/https-everywhere exists"),
                      ("DEBUG", "2017", "ghtorrent-49","ghtorrent.rb:","Repo Shikanime/print exists"),
                      ("INFO", "2017", "ghtorrent-42","api_client.rb:","Successful request. URL: https://api.github.com/repos/CanonicalLtd/maas-docs/issues/365/events?per_page=100, Remaining: 4943, Total: 88 ms "),
                      ("WARN", "2017-03-23", "ghtorrent-13","api_client.rb:","Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 3031 "),
                      ("DEBUG", "2017-03-23T09", "ghtorrent-40","ghtorrent.rb:","Transaction committed (11 ms)")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)

        # Unit Test Case How many lines does the RDD contain
        expected_schema = StructType([
            StructField('WARN', LongType(), True)
                   ])
        expected_data = [(1,)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Apply transforamtion on the input data frame
        transformed_df = find_warn(input_df,"Logging")
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit Test Case Count the number of WARNing messages
        expected_schema = StructType([
            StructField('total_line', LongType(), True)
        ])
        expected_data = [(5,)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Apply transforamtion on the input data frame
        transformed_df = total_line(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit Test Case How many repositories where processed in tota
        expected_schema = StructType([
            StructField('Total_api_Client', LongType(), True)
        ])
        expected_data = [(2,)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)


        # Apply transforamtion on the input data frame
        transformed_df = api_Client(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit Test Case Which client did most HTTP requests
        expected_schema = StructType([
            StructField('ghtorrent', StringType(), True),
            StructField('Most_Http', LongType(), True)
        ])

        expected_data = [("ghtorrent-13",1),("ghtorrent-40",2),("ghtorrent-42",1),("ghtorrent-49",1)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)


        # Apply transforamtion on the input data frame
        transformed_df = most_Http(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))


        # Unit Test case for Which client did most FAILED HTTP requests
        expected_schema = StructType([
            StructField('failed_Request_count', LongType(), True)
        ])
        expected_data = [(1,)]
        expected_df = self.spark.createDataFrame(data=expected_data,schema=expected_schema)

        transformed_df = faild_Request(input_df)
        transformed_df.show();

        field_list = lambda fields:(fields.name,fields.dataType,fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit Test Case for What is the most active repository

        expected_schema = StructType([
            StructField("ghtorrent", StringType(), True),
             StructField("most_Active_Repository", LongType(), True)
        ])
        expected_data = [("ghtorrent-13",1),("ghtorrent-40",2),("ghtorrent-42",1),("ghtorrent-49",1)]
        expected_df = self.spark.createDataFrame(data=expected_data,schema=expected_schema)

        transformed_df = most_Active_Repository(input_df)
        transformed_df.show();

        field_list = lambda fields:(fields.name,fields.dataType,fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

if __name__ == '__main__':
    unittest.main()