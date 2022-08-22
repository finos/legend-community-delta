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

package org.finos.legend.spark.pure

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.log4j.{Level, Logger}

class LegendCodegenTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession.builder().appName("TEST").master("local[1]").getOrCreate()
  Logger.getLogger("Alloy Execution Server").setLevel(Level.OFF)
  Logger.getLogger("org.apache").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  implicit class SchemaImpl(schema: StructType) {
    def toDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  "A simple spark schema" should "be converted as PURE entity" in {
    val schema = StructType(List(
      StructField("name", StringType, nullable = false)
    ))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} name: String[1];
                     |}""".stripMargin
    assert(pure == expected)
  }

  it should "fail if namespace is invalid" in {
    assertThrows[IllegalArgumentException] {
      val schema = StructType(List(
        StructField("name", StringType, nullable = false)
      ))
      LegendCodegen.codeGen("foo", "foo.bar", schema)
    }
  }

  it should "return metadata if any" in {
    val schema = StructType(List(
      StructField("name", StringType, nullable = false,
        new MetadataBuilder().putString("comment", "entity name").build())
    ))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'entity name'} name: String[1];
                     |}""".stripMargin
    assert(pure == expected)
  }

  it should "handle cardinality" in {
    val schema = StructType(List(
      StructField("name", StringType, nullable = true)
    ))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} name: String[0..1];
                     |}""".stripMargin
    assert(pure == expected)
  }

  it should "handle all primitive types" in {
    val schema = StructType(List(
      StructField("string", StringType, nullable = false),
      StructField("boolean", BooleanType, nullable = false),
      StructField("binary", BinaryType, nullable = false),
      StructField("integer", IntegerType, nullable = false),
      StructField("long", LongType, nullable = false),
      StructField("float", FloatType, nullable = false),
      StructField("double", DoubleType, nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} string: String[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} boolean: Boolean[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} binary: Binary[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} integer: Integer[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} long: Number[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} float: Float[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} double: Decimal[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} date: Date[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} timestamp: DateTime[1];
                     |}""".stripMargin
    assert(pure == expected)
  }

  it should "handle array type" in {
    val schema = StructType(List(StructField("name", ArrayType(StringType), nullable = false)))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} name: String[1..*];
                     |}""".stripMargin
    assert(pure == expected)
  }

  it should "handle array cardinality" in {
    val schema = StructType(List(StructField("name", ArrayType(IntegerType), nullable = true)))
    val pure = LegendCodegen.codeGen("foo::bar::entity", "foo.bar", schema)
    val expected = """###Pure
                     |Class foo::bar::entity
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} name: Integer[0..*];
                     |}""".stripMargin
    assert(pure == expected)
  }

  "A nested spark schema" should "be converted as PURE entity" in {
    val child = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
    ))
    val parent = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("children", ArrayType(child), nullable = false)
    ))
    assertThrows[IllegalArgumentException] {
      LegendCodegen.codeGen("foo::bar::entity", "foo.bar", parent)
    }
  }

}
