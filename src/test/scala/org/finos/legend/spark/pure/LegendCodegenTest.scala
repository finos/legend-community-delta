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

class LegendCodegenTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession.builder().appName("TEST").master("local[1]").getOrCreate()

  implicit class SchemaImpl(schema: StructType) {
    def toDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  "A spark schema" should "be converted as PURE" in {

    val schema = StructType(List(
      StructField("string", StringType, nullable = true),
      StructField("boolean", ArrayType(BooleanType), nullable = false),
      StructField("binary", ArrayType(BinaryType), nullable = true),
      StructField("byte", ByteType, nullable = true),
      StructField("short", ShortType, nullable = true),
      StructField("integer", IntegerType, nullable = false, metadata =
        new MetadataBuilder().putString("comment", "this is a comment").build()),
      StructField("long", LongType, nullable = false),
      StructField("float", FloatType, nullable = false),
      StructField("double", DoubleType, nullable = false),
      StructField("decimal", DoubleType, nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("timestamp", TimestampType, nullable = false)
    ))
    val observed = LegendCodegen.codeGen(schema, "foo.bar")
    val expected = """###Pure
                     |Profile legend::delta::generated::Profile
                     |{
                     |  stereotypes: [Generated];
                     |}
                     |
                     |Class <<legend::delta::generated::Profile.Generated>> legend::delta::generated::class::bar
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} string: String[0..1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} boolean: Boolean[1..*];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} binary: Binary[0..*];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} byte: Integer[0..1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} short: Integer[0..1];
                     |  {meta::pure::profiles::doc.doc = 'this is a comment'} integer: Integer[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} long: Number[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} float: Float[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} double: Decimal[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} decimal: Decimal[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} date: Date[1];
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} timestamp: DateTime[1];
                     |}
                     |
                     |###Relational
                     |Database legend::delta::generated::store::Schema
                     |(
                     |  Schema foo
                     |  (
                     |    Table bar
                     |    (
                     |      string VARCHAR(2147483647),
                     |      boolean BIT,
                     |      binary BINARY(2147483647),
                     |      byte TINYINT,
                     |      short SMALLINT,
                     |      integer INTEGER,
                     |      long BIGINT,
                     |      float DOUBLE,
                     |      double DOUBLE,
                     |      decimal DOUBLE,
                     |      date DATE,
                     |      timestamp TIMESTAMP
                     |    )
                     |  )
                     |)
                     |
                     |###Mapping
                     |Mapping legend::delta::generated::mapping::bar
                     |(
                     |  *legend::delta::generated::class::bar: Relational
                     |  {
                     |    ~primaryKey
                     |    (
                     |      [legend::delta::generated::store::Schema]foo.bar.string
                     |    )
                     |    ~mainTable [legend::delta::generated::store::Schema]foo.bar
                     |    string: [legend::delta::generated::store::Schema]foo.bar.string,
                     |    boolean: [legend::delta::generated::store::Schema]foo.bar.boolean,
                     |    binary: [legend::delta::generated::store::Schema]foo.bar.binary,
                     |    byte: [legend::delta::generated::store::Schema]foo.bar.byte,
                     |    short: [legend::delta::generated::store::Schema]foo.bar.short,
                     |    integer: [legend::delta::generated::store::Schema]foo.bar.integer,
                     |    long: [legend::delta::generated::store::Schema]foo.bar.long,
                     |    float: [legend::delta::generated::store::Schema]foo.bar.float,
                     |    double: [legend::delta::generated::store::Schema]foo.bar.double,
                     |    decimal: [legend::delta::generated::store::Schema]foo.bar.decimal,
                     |    date: [legend::delta::generated::store::Schema]foo.bar.date,
                     |    timestamp: [legend::delta::generated::store::Schema]foo.bar.timestamp
                     |  }
                     |)""".stripMargin
    print(observed)
    assert(observed == expected)
  }

}
