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

  "A namespace" should "be valid" in {
    assert("antoine::amend".isValidNamespace)
    assert(!" antoine::amend".isValidNamespace)
    assert(!"antoine.amend".isValidNamespace)
    assert(!"antoine::amend::".isValidNamespace)
    assert("antoine::amend::legend".isValidNamespace)
  }

  "Capitalizing field" should "work" in {
    assert("hello_world".camelCaseEntity == "HelloWorld")
    assert("helloWorld".camelCaseEntity == "HelloWorld")
    assert("helloworld".camelCaseEntity == "Helloworld")
    assert("hello_world".camelCaseField == "helloWorld")
    assert("helloWorld".camelCaseField == "helloWorld")
    assert("helloworld".camelCaseField == "helloworld")
  }

  "A spark schema" should "be converted as PURE" in {

    val child = StructType(List(
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

    val parent = StructType(List(
      StructField("integer_field", IntegerType, nullable = true),
      StructField("struct_field", child, nullable = true)
    ))

    val observed = LegendCodegen.generatePureFromSchema(
      "org::finos::legend::delta",
      "database",
      "table",
      parent
    )
    print(observed)
  }

}
