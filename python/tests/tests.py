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
        self.spark = SparkSession.builder.appName("legend") \
            .config("spark.driver.extraClassPath", spark_conf) \
            .master("local") \
            .getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_schema(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        schema = legend.get_schema('databricks::mapping::employee_delta')
        fields = set([f.name for f in schema.fields])
        expected = set([
            'firstName',
            'lastName',
            'birthDate',
            'gender',
            'id',
            'sme',
            'joinedDate',
            'highFives'
        ])
        self.assertEqual(fields, expected)
        print(schema)

    def test_expectations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        expectations = legend.get_expectations('databricks::mapping::employee_delta')
        expected = {
            '[birthDate] is mandatory': 'birth_date IS NOT NULL',
            '[sme] not allowed value': "(sme IS NULL OR sme IN ('Scala', 'Python', 'Java', 'R', 'SQL'))",
            '[id] is mandatory': 'id IS NOT NULL',
            '[joinedDate] is mandatory': 'joined_date IS NOT NULL',
            '[firstName] is mandatory': 'first_name IS NOT NULL',
            '[high five] should be positive': '(high_fives IS NOT NULL AND high_fives > 0)',
            '[lastName] is mandatory': 'last_name IS NOT NULL',
            '[hiringAge] should be > 18': 'year(joined_date) - year(birth_date) > 18'
        }
        self.assertEqual(expectations, expected)
        print(expectations)

    def test_transformations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        transformations = legend.get_transformations('databricks::mapping::employee_delta')
        expected = {
            'highFives': 'high_fives',
            'joinedDate': 'joined_date',
            'lastName': 'last_name',
            'firstName': 'first_name',
            'birthDate': 'birth_date',
            'id': 'id',
            'sme': 'sme',
            'gender': 'gender'
        }
        self.assertEqual(transformations, expected)
        print(transformations)

    def test_derivations(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        derivations = legend.get_derivations('databricks::mapping::employee_delta')
        expected = {
            'hiringAge': 'year(joined_date) - year(birth_date) AS `hiringAge`',
            'age': 'year(current_date) - year(birth_date) AS `age`',
            'initials': "concat(substring(first_name, 0, 1), substring(last_name, 0, 1)) AS `initials`"
        }
        self.assertEqual(derivations, expected)
        print(derivations)

    def test_table(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        table = legend.get_table('databricks::mapping::employee_delta')
        self.assertTrue(table == 'legend.employee')


if __name__ == '__main__':
    unittest.main()