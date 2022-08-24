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

  "A nested spark schema" should "be converted as PURE" in {

    val child = StructType(List(
      StructField("nickname", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true)
    ))

    val parent = StructType(List(
      StructField("first_name", StringType, nullable = true),
      StructField("last_name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("children", ArrayType(child), nullable = true)
    ))

    val observed = LegendCodegen.generatePureFromSchema(
      "org::finos::legend::delta",
      "family",
      "parent",
      parent
    )

    print(observed)
  }

  "Generated PURE model" should "be correctly interpreted" in {

    val schema = StructType(List(
      StructField("name", StringType, nullable = true),
      StructField("id", ByteType, nullable = true),
      StructField("has_star", BooleanType, nullable = true),
      StructField("forks", ShortType, nullable = true),
      StructField("likes", IntegerType, nullable = true),
      StructField("lines_code", LongType, nullable = true),
      StructField("avg_review", FloatType, nullable = true),
      StructField("avg_lines", DoubleType, nullable = true),
      StructField("created_on", DateType, nullable = true),
      StructField("last_commit", TimestampType, nullable = true),
      StructField("thumbnail", BinaryType, nullable = true)
    ))

    val observed = LegendCodegen.generatePureFromSchema(
      "org::finos::legend::delta",
      "github",
      "project",
      schema
    )

    print(observed)
  }

}
