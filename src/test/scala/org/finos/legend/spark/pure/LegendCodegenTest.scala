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

    val schema1 = StructType(List(
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

    val schema2 = StructType(List(
      StructField("string", StringType, nullable = true)
    ))

    val pureTable1 = LegendCodegen.codeGen(schema1, "table_bar1")
    val pureTable2 = LegendCodegen.codeGen(schema2, "table_bar2")

    val observed = PureDatabase("database_foo", Array(pureTable1, pureTable2)).toPure
    val expected = """###Pure
                     |Profile legend::delta::generated::Profile
                     |{
                     |  stereotypes: [Generated];
                     |}
                     |
                     |Class <<legend::delta::generated::Profile.Generated>> legend::delta::generated::class::table_bar1
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
                     |Class <<legend::delta::generated::Profile.Generated>> legend::delta::generated::class::table_bar2
                     |{
                     |  {meta::pure::profiles::doc.doc = 'auto-generated property'} string: String[0..1];
                     |}
                     |
                     |###Relational
                     |Database legend::delta::generated::store::Schema
                     |(
                     |  Schema database_foo
                     |  (
                     |    Table table_bar1
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
                     |    Table table_bar2
                     |    (
                     |      string VARCHAR(2147483647)
                     |    )
                     |  )
                     |)
                     |
                     |###Mapping
                     |Mapping legend::delta::generated::mapping::table_bar1
                     |(
                     |  *legend::delta::generated::class::table_bar1: Relational
                     |  {
                     |    ~primaryKey
                     |    (
                     |      [legend::delta::generated::store::Schema]database_foo.table_bar1.string
                     |    )
                     |    ~mainTable [legend::delta::generated::store::Schema]database_foo.table_bar1
                     |    string: [legend::delta::generated::store::Schema]database_foo.table_bar1.string,
                     |    boolean: [legend::delta::generated::store::Schema]database_foo.table_bar1.boolean,
                     |    binary: [legend::delta::generated::store::Schema]database_foo.table_bar1.binary,
                     |    byte: [legend::delta::generated::store::Schema]database_foo.table_bar1.byte,
                     |    short: [legend::delta::generated::store::Schema]database_foo.table_bar1.short,
                     |    integer: [legend::delta::generated::store::Schema]database_foo.table_bar1.integer,
                     |    long: [legend::delta::generated::store::Schema]database_foo.table_bar1.long,
                     |    float: [legend::delta::generated::store::Schema]database_foo.table_bar1.float,
                     |    double: [legend::delta::generated::store::Schema]database_foo.table_bar1.double,
                     |    decimal: [legend::delta::generated::store::Schema]database_foo.table_bar1.decimal,
                     |    date: [legend::delta::generated::store::Schema]database_foo.table_bar1.date,
                     |    timestamp: [legend::delta::generated::store::Schema]database_foo.table_bar1.timestamp
                     |  }
                     |)
                     |
                     |Mapping legend::delta::generated::mapping::table_bar2
                     |(
                     |  *legend::delta::generated::class::table_bar2: Relational
                     |  {
                     |    ~primaryKey
                     |    (
                     |      [legend::delta::generated::store::Schema]database_foo.table_bar2.string
                     |    )
                     |    ~mainTable [legend::delta::generated::store::Schema]database_foo.table_bar2
                     |    string: [legend::delta::generated::store::Schema]database_foo.table_bar2.string
                     |  }
                     |)""".stripMargin
    print(observed)
    assert(observed == expected)
  }

}
