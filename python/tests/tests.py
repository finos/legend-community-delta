import unittest

from legend.delta import *
from pyspark.sql import SparkSession
import os
from pathlib import Path


class LegendTest(unittest.TestCase):

    def setUp(self):

        # retrieve legend model for test
        path = Path(os.getcwd()).parent.absolute()
        self.legend_path = os.path.join(path, 'src', 'test', 'resources')

        # retrieve all jar files required for test
        path = Path(os.getcwd())
        dep_path = os.path.join(path, 'build', 'dependencies')
        dep_file = [os.path.join(dep_path, f) for f in os.listdir(dep_path)]
        spark_conf = ':'.join(dep_file)
        self.spark_conf = spark_conf

        # inject scala classes
        self.spark = SparkSession.builder.appName("geoscan") \
            .config("spark.driver.extraClassPath", spark_conf) \
            .master("local") \
            .getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_schema(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        schema = legend.get_schema('databricks::mapping::employee_delta')
        self.assertTrue(len(schema.fields) == 8)
        print(schema)

    def test_expectations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        expectations = legend.get_expectations('databricks::mapping::employee_delta')
        self.assertTrue(len(expectations) == 8)
        print(expectations)

    def test_transformations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        transformations = legend.get_transformations('databricks::mapping::employee_delta')
        self.assertTrue(len(transformations) == 8)
        print(transformations)

    def test_derivations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        derivations = legend.get_derivations('databricks::mapping::employee_delta')
        self.assertTrue(len(derivations) == 2)
        print(derivations)

    def test_table(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        table = legend.get_table('databricks::mapping::employee_delta')
        self.assertTrue(table == 'legend.employee')


## MAIN
if __name__ == '__main__':
    unittest.main()