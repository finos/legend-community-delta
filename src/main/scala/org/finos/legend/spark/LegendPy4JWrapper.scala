/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 Databricks - see NOTICE.md file
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.legend.spark

import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/**
 * Since we're offering the same experience for python users, we have to ensure compatibility with pyspark.
 * Py4J does not deal with Scala collections nicely, we return JSON objects instead that we can easily deserialize on
 * python side. Another approach would be to create 1 unique interface and return Java collections instead, but would
 * affect Scala user experience. Hence this wrapper object
 * @param legend: the underlying legend class with scala business logic
 */
class LegendPy4JWrapper(legend: Legend) {

  def getSchema(entityName: String): String = {
    legend.getSchema(entityName).json
  }

  def getEntityNames: String = {
    Json(DefaultFormats).write(legend.getEntityNames)
  }

  def getTransformations(mappingName: String): String = {
    Json(DefaultFormats).write(legend.getTransformations(mappingName))
  }

  def getExpectations(entityName: String): String = {
    Json(DefaultFormats).write(legend.getExpectations(entityName).filter(_._2.isSuccess).map(i => (i._1, i._2.get)))
  }

  def getDerivations(mappingName: String): String = {
    Json(DefaultFormats).write(legend.getDerivations(mappingName))
  }

  def query(entityName: String): DataFrame = {
    legend.query(entityName)
  }

  def generateSql(entityName: String): String = {
    legend.generateSql(entityName)
  }

  def getTable(mappingName: String): String = {
    legend.getTable(mappingName)
  }

  def createTable(mappingName: String, path: String): String = {
    legend.createTable(mappingName, Some(path))
  }

  def createTable(mappingName: String): String = {
    legend.createTable(mappingName, None)
  }



}
