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

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class LegendCodegenTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession.builder().appName("TEST").master("local[1]").getOrCreate()

  implicit class SchemaImpl(schema: StructType) {
    def toDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  "A spark schema" should "be converted as PURE" in {

    val testSchema1 = StructType(List(
      StructField("string_field", StringType, nullable = true),
      StructField("byte_field", ByteType, nullable = true),
      StructField("boolean_field", BooleanType, nullable = true),
      StructField("short_field", ShortType, nullable = true),
      StructField("integer_field", IntegerType, nullable = true),
      StructField("long_field", LongType, nullable = true),
      StructField("float_field", FloatType, nullable = true),
      StructField("double_field", DoubleType, nullable = true),
      StructField("date_field", DateType, nullable = true),
      StructField("timestamp_field", TimestampType, nullable = true),
      StructField("binary_field", BinaryType, nullable = true)
    ))

    val testSchema2 = StructType(List(
      StructField("integer_field", IntegerType, nullable = true),
      StructField("string_field", ArrayType(StringType), nullable = true)
    ))

    val testSchema3 = StructType(List(
      StructField("integer_field", IntegerType, nullable = true),
      StructField("struct_field", testSchema2, nullable = true)
    ))

    val observed = LegendCodegen.parseDatabase(
      "com::databricks",
      "legend_integration",
      Map(
        "legend_primitive" -> testSchema1.toDF,
        "legend_arrays" -> testSchema2.toDF,
        "legend_nested" -> testSchema3.toDF,
      )
    )
    print(observed)
  }

}
