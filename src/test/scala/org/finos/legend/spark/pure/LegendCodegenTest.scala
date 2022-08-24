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
import org.scalatest.flatspec.AnyFlatSpec

class LegendCodegenTest extends AnyFlatSpec {

  "A spark schema" should "be converted as PURE" in {

    val testSchema1 = StructType(List(
      StructField("string", StringType, nullable = true),
      StructField("byte", ByteType, nullable = true),
      StructField("boolean", BooleanType, nullable = true),
      StructField("short", ShortType, nullable = true),
      StructField("integer", IntegerType, nullable = true),
      StructField("long", LongType, nullable = true),
      StructField("float", FloatType, nullable = true),
      StructField("double", DoubleType, nullable = true),
      StructField("date", DateType, nullable = true),
      StructField("timestamp", TimestampType, nullable = true),
      StructField("binary", BinaryType, nullable = true)
    ))

    val testSchema2 = StructType(List(
      StructField("integer", IntegerType, nullable = true),
      StructField("string", ArrayType(StringType), nullable = true)
    ))

    val testSchema3 = StructType(List(
      StructField("integer", IntegerType, nullable = true),
      StructField("grandChild", testSchema2, nullable = true)
    ))

    val testSchema4 = StructType(List(
      StructField("integer", IntegerType, nullable = true),
      StructField("child", testSchema3, nullable = true)
    ))

    val observed = LegendCodegen.parseSchemas(
      "legend_integration",
      Map(
        "legend_primitive" -> testSchema1,
        "legend_arrays" -> testSchema2,
        "legend_nested" -> testSchema2,
        "legend_nested2" -> testSchema4,
      )
    )
    print(observed)
  }

}
