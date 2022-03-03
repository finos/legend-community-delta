/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2021 Databricks - see NOTICE.md file
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

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

class LegendTest extends AnyFlatSpec {

  Logger.getLogger("Alloy Execution Server").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  "A legend data type" should "be converted into spark data type" in {
    val returnTypes = Seq("String", "Boolean", "Binary", "Integer", "Number", "Float", "Decimal", "Date", "StrictDate", "DateTime")
    val dataType = Seq(StringType, BooleanType, BinaryType, IntegerType, LongType, FloatType, DoubleType, DateType, DateType, TimestampType)
    returnTypes.zip(dataType).foreach({ case (returnType, dataType) =>
      val converted = LegendUtils.convertDataTypeFromString(returnType)
      assert(converted == dataType, s"[$returnType] should be converted as [$dataType], got [$converted]")
    })
  }

  it should "fail for invalid types" in {
    assertThrows[IllegalArgumentException] {
      LegendUtils.convertDataTypeFromString("foobar")
    }
  }

  "A legend model" should "be loaded from classpath" in {
    assert(LegendClasspathLoader.loadResources("model").getEntityNames.nonEmpty)
  }

  it should "be loaded from external directory" in {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource("model").getFile)
    assert(LegendFileLoader.loadResources(file.toString).getEntityNames.nonEmpty)
  }

  it should "contain specifications for entity classes" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::entity::person"))
    val fields = legend.getEntitySchema("databricks::entity::person").fields.map(_.name)
    assert(fields.toSet == Set("firstName", "lastName", "birthDate", "gender"))
  }

  it should "support supertype entities" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::entity::employee"))
    val personFields = legend.getEntitySchema("databricks::entity::person").fields.map(_.name)
    val employeeFields = legend.getEntitySchema("databricks::entity::employee").fields.map(_.name)
    assert(employeeFields.diff(personFields).toSet == Set("highFives", "sme", "id", "joinedDate"))
  }

  "Expectations" should "be generated from a legend class" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val expectations = legend.getMappingExpectations(
      "databricks::mapping::employee_delta"
    )
    assert(expectations.nonEmpty)
  }

  it should "be generated in SQL" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val transform = legend.getMappingExpectations("databricks::mapping::employee_delta")

    assert(
      transform.values.toSet == Set(
        "first_name IS NOT NULL",
        "last_name IS NOT NULL",
        "birth_date IS NOT NULL",
        "id IS NOT NULL",
        "(sme IS NULL OR sme IN ('Scala', 'Python', 'C', 'Java', 'R', 'SQL'))",
        "joined_date IS NOT NULL",
        "(high_fives IS NOT NULL AND high_fives > 0)",
        "year(joined_date) - year(birth_date) > 20"
      )
    )
  }

  "A relational mapping" should "capture transformations" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val transform = legend.getMappingTransformations("databricks::mapping::employee_delta")
    assert(transform.keys.toSet == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
    assert(transform.values.toSet == Set("high_fives", "joined_date", "last_name", "first_name", "birth_date", "id", "sme", "gender"))
  }

  it should "create a spark schema" in {
    SparkSession.getActiveSession match {
      case Some(_) =>
      case _ => SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    }
    val legend = LegendClasspathLoader.loadResources("model")
    val schema = legend.getMappingSchema("databricks::mapping::employee_delta")
    assert(schema.fields.map(_.name).toSet == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
  }

  it should "generate a table DDL" in {
    SparkSession.getActiveSession match {
      case Some(_) =>
      case _ => SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    }
    val legend = LegendClasspathLoader.loadResources("model")
    val table = legend.getMappingTable("databricks::mapping::employee_delta", ddl=true)
    println(table)
  }

}
