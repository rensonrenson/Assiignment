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
            StructField("ghtorrent_id", StringType(), True)
        ])

        input_data = [("DEBUG", "2017-03-23", "ghtorrent "),
                      ("DEBUG", "2017", "ghtorrent "),
                      ("INFO", "2017", "ghtorrent "),
                      ("WARN", "2017-03-23", "ghtorrent-13"),
                      ("DEBUG", "2017-03-23T09", "ghtorrent")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)

        expected_schema = StructType([
            StructField('warn_count', LongType(), True),
            StructField('total_line', IntegerType(), True)
        ])

        expected_data = [5]

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
if __name__ == '__main__':
    unittest.main()