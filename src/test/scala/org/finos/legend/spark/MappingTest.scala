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

class MappingTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A legend resources directory" should "be loaded from classpath" in {
    val legend = LegendFileLoader.loadResources("src/test/resources/model")

    val transform = legend.transform(
      "databricks::employee",
      "databricks::lakehouse::emp2delta",
      "databricks::lakehouse::runtime"
    )

    assert(transform.dstTable == "legend.employee")

    transform.srcExpectations.foreach({ case (src, dst) =>
      println(src + " -> " + dst)
    })

    transform.transformations.foreach({ case (src, dst) =>
      println(src + " -> " + dst)
    })

    transform.constraints.foreach({ case (src, dst) =>
      println(src + " -> " + dst)
    })
  }

}
