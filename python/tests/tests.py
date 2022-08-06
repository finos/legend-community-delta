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

    def test_names(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        entities = legend.get_entities()
        for entity in entities:
            print(entity)
        self.assertTrue('databricks::mapping::employee_delta' in entities)

    def test_generate_sql(self):
        legend = LegendFileLoader().loadResources(self.legend_path)
        sql = legend.generate_sql('databricks::mapping::employee_delta')
        expected = """select `root`.high_fives as `highFives`, `root`.joined_date as `joinedDate`, 
        `root`.last_name as `lastName`, `root`.first_name as `firstName`, `root`.birth_date as `birthDate`, 
        `root`.id as `id`, `root`.sme as `sme`, `root`.gender as `gender`, 
        year(`root`.joined_date) - year(`root`.birth_date) as `hiringAge`, 
        year(current_date) - year(`root`.birth_date) as `age`, concat(substring(`root`.first_name, 0, 1), 
        substring(`root`.last_name, 0, 1)) as `initials` from legend.employee as `root` 
        WHERE birth_date IS NOT NULL AND (sme IS NULL OR sme IN ('Scala', 'Python', 'Java', 'R', 'SQL')) 
        AND id IS NOT NULL AND joined_date IS NOT NULL AND first_name IS NOT NULL AND (high_fives IS NOT NULL 
        AND high_fives > 0) AND last_name IS NOT NULL AND year(joined_date) - year(birth_date) > 18"""
        expected = ' '.join([x.strip() for x in expected.split('\n')])
        print(sql)
        self.assertEqual(sql, expected)

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
        print(schema)
        self.assertEqual(fields, expected)

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