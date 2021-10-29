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

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class LegendMappingTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A spark execution  strategy" should "be generated" in {
    val legend = LegendClasspathLoader.loadResources("model")

    val transform = legend.transform(
      "databricks::employee",
      "databricks::lakehouse::emp2delta",
      "databricks::lakehouse::runtime"
    )

    assert(transform.table == "legend.employee")

    val expectations = transform.filters.map(_.sql).toSet
    assert(expectations == Set("`id` IS NOT NULL", "`sme` IS NOT NULL", "`joined_date` IS NOT NULL", "`high_fives` IS NOT NULL"))

    val withColumns = transform.withColumnsRenamed.map(_.from).toSet
    assert(withColumns == Set("id", "first_name", "last_name", "birth_date", "sme", "joined_date", "high_fives"))

    val constraints = transform.constraints.map(_.sql).toSet
    assert(constraints == Set("high_fives > 0", "year(joined_date) - year(birth_date) > 20"))

  }

}
