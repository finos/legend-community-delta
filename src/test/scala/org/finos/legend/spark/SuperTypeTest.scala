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

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class SuperTypeTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A legend referenced super type" should "be schematized" in {
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema = legend.getEntitySchema("fire::customer")
    schema.fields.foreach(f => logger.info(s"[${f.toDDL}]"))
    assert(schema.fields.nonEmpty, "StructType should be loaded")
    assert(schema.fields.map(_.name).contains("id"), "Field ID must have been inherited from super type")
  }
}
