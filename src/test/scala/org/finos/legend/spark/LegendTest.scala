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

  "A legend resources directory" should "be loaded from classpath" in {
    assert(LegendClasspathLoader.loadResources("model").getEntityNames.nonEmpty)
  }

  it should "also be loaded from external directory" in {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource("model").getFile)
    assert(LegendFileLoader.loadResources(file.toString).getEntityNames.nonEmpty)
  }

  it should "contain specification for [databricks::person]" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::person"))
    val fields = legend.getEntitySchema("databricks::person").fields.map(_.name)
    assert(fields.toSet == Set("first_name", "last_name", "birth_date"))
  }

  "[databricks::employee]" should "be a supertype of entity [databricks::person]" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))
    val personFields = legend.getEntitySchema("databricks::person").fields.map(_.name)
    val employeeFields = legend.getEntitySchema("databricks::employee").fields.map(_.name)
    assert(employeeFields.diff(personFields).toSet == Set("high_fives", "sme", "id", "joined_date"))
  }

  "A json specification for [databricks::employee]" should "be loaded as a Spark Schema" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val schema = legend.getEntitySchema("databricks::employee")
    assert(schema.fields.nonEmpty, "StructType should be loaded")
    schema.fields.foreach(f => logger.info(s"[${f.toDDL}]"))
  }

  "Expectations" should "be created from a [databricks::employee] class" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val expectations = legend.getEntityExpectations("databricks::employee")
    expectations.foreach(println)
    assert(expectations.nonEmpty)
  }

  "A schema" should "defined as backwards compatible" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val schema1 = legend.getEntitySchema("databricks::employee")
    val schema0 = StructType(schema1.drop(1))
    val drift = legend.detectEntitySchemaDrift("databricks::employee", schema0)
    assert(drift.isCompatible)
    drift.alterStatements.foreach(p => logger.info(s"[ALTER TABLE bobby_table1 $p]"))
  }

  it should "allow for new metadata" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val schema1 = legend.getEntitySchema("databricks::employee")
    val schema0 = StructType(schema1.fields.map(_.copy(metadata = new MetadataBuilder().putString("comment", "this is a new comment").build())))
    val drift = legend.detectEntitySchemaDrift("databricks::employee", schema0)
    assert(drift.isCompatible)
    drift.alterStatements.foreach(p => logger.info(s"[ALTER TABLE bobby_table1 $p]"))
  }

  it should "be incompatible when column was dropped" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val schema1 = legend.getEntitySchema("databricks::employee")
    val schema0 = StructType(schema1 :+ StructField("test", StringType, nullable = true))
    val drift = legend.detectEntitySchemaDrift("databricks::employee", schema0)
    assert(!drift.isCompatible)
  }

  it should "be incompatible when column type changed" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val schema1 = legend.getEntitySchema("databricks::employee")
    val schema0 = StructType(schema1.drop(1) :+ StructField(schema1.head.name, BinaryType, nullable = true))
    val drift = legend.detectEntitySchemaDrift("databricks::employee", schema0)
    assert(!drift.isCompatible)
  }

  "Legend Return type" should "be converted in DataType" in {
    val returnTypes = Seq("String", "Boolean", "Binary", "Integer", "Number", "Float", "Decimal", "Date", "StrictDate", "DateTime")
    val dataType = Seq(StringType, BooleanType, BinaryType, IntegerType, LongType, FloatType, DoubleType, DateType, DateType, TimestampType)
    returnTypes.zip(dataType).foreach({ case (returnType, dataType) =>
      val converted = convertDataTypeFromString(returnType)
      assert(converted == dataType, s"[$returnType] should be converted as [$dataType], got [$converted]")
    })
    assertThrows[IllegalArgumentException] {
      convertDataTypeFromString("Foo Bar")
    }
  }
}
