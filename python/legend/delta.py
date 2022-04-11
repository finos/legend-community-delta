#
# Copyright 2022 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json

class LegendFileLoader():

    def __init__(self):
        self.spark = SparkSession.getActiveSession()

    def loadResources(self, path):
        legend = self.spark.sparkContext._jvm.org.finos.legend.spark.LegendFileLoader.loadResources(path)
        return Legend(legend)


class LegendClasspathLoader():

    def __init__(self):
        self.spark = SparkSession.getActiveSession()

    def loadResources(self):
        legend = self.spark.sparkContext._jvm.org.finos.legend.spark.LegendClasspathLoader.loadResources()
        return Legend(legend)


class Legend():

    def __init__(self, legend):
        self.spark = SparkSession.getActiveSession()
        self.legend = legend

    def get_schema(self, entity_name):
        schema_str = self.legend.getSchema(entity_name).json()
        return StructType.fromJson(json.loads(schema_str))

    def get_expectations(self, entity_name):
        expectations_str = self.legend.getExpectationsJson(entity_name)
        return json.loads(expectations_str)

    def get_transformations(self, mapping_name):
        transformations_str = self.legend.getTransformationsJson(mapping_name)
        return json.loads(transformations_str)

    def get_table(self, mapping_name):
        table = self.legend.getTable(mapping_name)
        return table

    def create_table(self, mapping_name, path, col_name):
        table = self.legend.createTable(mapping_name, path, col_name)
        return table

    def validate_table(self, mapping_name, col_name):
        self.legend.validateTable(mapping_name, col_name)
