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

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class LegendTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

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
    assert(legend.getEntityNames.contains("databricks::person"))
    val fields = legend.getEntitySchema("databricks::person").fields.map(_.name)
    assert(fields.toSet == Set("first_name", "last_name", "birth_date"))
  }

  it should "support supertype entities" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))
    val personFields = legend.getEntitySchema("databricks::person").fields.map(_.name)
    val employeeFields = legend.getEntitySchema("databricks::employee").fields.map(_.name)
    assert(employeeFields.diff(personFields).toSet == Set("high_fives", "sme", "id", "joined_date"))
  }

  "Expectations" should "be generated from a legend class" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val expectations = legend.getExpectations(
      "databricks::employee",
      "databricks::lakehouse::emp2delta"
    )
    assert(expectations.nonEmpty)
  }

  it should "be generated in SQL" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val transform = legend.buildStrategy(
      "databricks::employee",
      "databricks::lakehouse::emp2delta"
    )

    assert(
      transform.expectations.map(_.sql) == Seq(
        "firstname IS NOT NULL",
        "lastname IS NOT NULL",
        "birthdate IS NOT NULL",
        "id IS NOT NULL",
        "sme IS NOT NULL",
        "joineddate IS NOT NULL",
        "highfives IS NOT NULL",
        "highfives < 300",
        "year(joineddate) - year(birthdate) > 20"
      )
    )

    assert(
      transform.expectations.map(_.lambda) == Seq(
        "$this.first_name->isNotEmpty()",
        "$this.last_name->isNotEmpty()",
        "$this.birth_date->isNotEmpty()",
        "$this.id->isNotEmpty()",
        "$this.sme->isNotEmpty()",
        "$this.joined_date->isNotEmpty()",
        "$this.high_fives->isNotEmpty()",
        "$this.high_fives < 300",
        "$this.joined_date->dateDiff($this.birth_date,DurationUnit.YEARS) > 20"
      )
    )
  }

  "A relational mapping" should "capture transformations" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val transform = legend.buildStrategy(
      "databricks::employee",
      "databricks::lakehouse::emp2delta"
    )
    val withColumns = transform.transformations
    assert(withColumns.map(_.from).toSet == Set("id", "first_name", "last_name", "birth_date", "sme", "joined_date", "high_fives"))
    assert(withColumns.map(_.to).toSet == Set("id", "firstname", "lastname", "birthdate", "sme", "joineddate", "highfives"))
  }
}
