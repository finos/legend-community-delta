/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2021 FINOS Legend2Delta contributors - see NOTICE.md file
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.finos.legend.spark.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class LegendTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A legend resources directory" should "be loaded from classpath" in {
    assert(LegendClasspathLoader.loadResources("fire-model").getEntityNames.nonEmpty)
  }

  it should "also be loaded from external directory" in {
    val classLoader = getClass.getClassLoader
    val file = new File(classLoader.getResource("fire-model").getFile)
    assert(LegendFileLoader.loadResources(file.toString).getEntityNames.nonEmpty)
  }

  it should "contain specification for [fire::collateral]" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    assert(legend.getEntityNames.contains("fire::collateral"))
  }

  "A json specification for [fire::collateral]" should "be loaded as a Spark Schema" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema = legend.getEntitySchema("fire::reporter")
    assert(schema.fields.nonEmpty, "StructType should be loaded")
    schema.fields.foreach(f => logger.info(s"[${f.toDDL}]"))
  }

  "All fire entities" should "be retrieved" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val actual = legend.getEntityNames.filter(_.split("::").length == 2)
    val expected = Set(
      "fire::derivative_cash_flow",
      "fire::account",
      "fire::issuer",
      "fire::agreement",
      "fire::security",
      "fire::loan",
      "fire::loan_transaction",
      "fire::guarantor",
      "fire::reporter",
      "fire::collateral",
      "fire::adjustment",
      "fire::exchange_rate",
      "fire::customer",
      "fire::derivative",
      "fire::curve"
    )
    actual.foreach(println)
    assert(actual == expected)
  }

  it should "be properly schematized" in {
    // no exception should be raised
    val legend = LegendClasspathLoader.loadResources("fire-model")
    legend.getEntityNames.filter(_.split("::").length == 2).foreach(e => {
      val ddl = legend.getEntitySchema(e).toDDL
      logger.info(ddl)
    })
  }

  "A json specification" should "not work against non class file" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    assertThrows[IllegalArgumentException] {
      legend.getEntitySchema("fire::common::country_code")
    }
  }

  "Expectations" should "be created from a [fire::collateral] class" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    assert(legend.getEntityExpectations("fire::collateral", validate = false).nonEmpty)
  }

  it should "be syntactically valid" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val expectations = legend.getEntityExpectations("fire::collateral", validate = false)
    assert(!expectations.exists(_.expression.isFailure))
    expectations.foreach(e => logger.info(s"[${e.expression.get}]"))
  }

  it should "be converted as Lambda representations" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val expectations = legend.getEntityExpectations("fire::collateral", validate = false)
    expectations.filter(_.lambda.isDefined).foreach(e => logger.info(s"[${e.lambda.get}]"))
  }

  it should "contain syntactically and contextually valid expectations" in {
    val spark = SparkSession.builder().master("local").appName("legend").getOrCreate()
    spark.registerLegendUDFs()
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val expectations = legend.getEntityExpectations("fire::collateral")
    assert(!expectations.exists(_.expression.isFailure))
    expectations.foreach(e => logger.info(s"[${e.expression.get}]"))
  }

  "All fire entities" should "have correct expectations" in {
    // no exception should be raised
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val expectations = legend.getEntityNames.filter(_.split("::").length == 2).flatMap(e => {
      legend.getEntityExpectations(e, validate = false)
    })
    expectations.foreach(e => if (e.expression.isSuccess) logger.info(e.toString) else logger.error(e.toString))
    assert(!expectations.exists(_.expression.isFailure), "all FIRE expectations should have been mapped")
  }

  it should "be validated against schema" in {
    val spark = SparkSession.builder().master("local[1]").appName("legend").getOrCreate()
    spark.registerLegendUDFs()
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val expectations = legend.getEntityNames.filter(_.split("::").length == 2).flatMap(e => {
      legend.getEntityExpectations(e)
    })
    expectations.foreach(e => if (e.expression.isSuccess) logger.info(e.toString) else logger.error(e.toString))
    assert(!expectations.exists(_.expression.isFailure), "all FIRE expectations should have been mapped")
  }

  "A schema" should "defined as backwards compatible" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema1 = legend.getEntitySchema("fire::collateral")
    val schema0 = StructType(schema1.drop(1))
    val drift = legend.detectEntitySchemaDrift("fire::collateral", schema0)
    assert(drift.isCompatible)
    drift.alterStatements.foreach(p => logger.info(s"[ALTER TABLE bobby_table1 $p]"))
  }

  it should "allow for new metadata" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema1 = legend.getEntitySchema("fire::collateral")
    val schema0 = StructType(schema1.fields.map(_.copy(metadata = new MetadataBuilder().putString("comment", "foobar").build())))
    val drift = legend.detectEntitySchemaDrift("fire::collateral", schema0)
    assert(drift.isCompatible)
    drift.alterStatements.foreach(p => logger.info(s"[ALTER TABLE bobby_table1 $p]"))
  }

  it should "be incompatible when column was dropped" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema1 = legend.getEntitySchema("fire::collateral")
    val schema0 = StructType(schema1 :+ StructField("test", StringType, nullable = true))
    val drift = legend.detectEntitySchemaDrift("fire::collateral", schema0)
    assert(!drift.isCompatible)
  }

  it should "be incompatible when column type changed" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema1 = legend.getEntitySchema("fire::collateral")
    val schema0 = StructType(schema1.drop(1) :+ StructField(schema1.head.name, BinaryType, nullable = true))
    val drift = legend.detectEntitySchemaDrift("fire::collateral", schema0)
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
