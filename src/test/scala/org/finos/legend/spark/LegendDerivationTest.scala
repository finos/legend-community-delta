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

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.finos.legend.spark.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Failure

class LegendDerivationTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "Derived properties" should "be loaded from [fire::collateral] specs" in {
    val spark = SparkSession.builder().master("local").appName("legend").getOrCreate()
    spark.registerLegendUDFs()
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val derivations = legend.getEntityDerivations("fire::collateral")
    assert(derivations.size == 2, "Two derivations should be loaded")
    assert(!derivations.exists(_.expression.isFailure)    , "Two derivation should be correct")
    val ddls = derivations.map(_.toDDL)
    ddls.filter(_.isFailure).head match {
      case Failure(exception) => assert(exception.getMessage == "A generated column cannot use a user-defined function")
    }
    assert(ddls.count(_.isSuccess) == 1, "One derivation is DDL supported")
  }

}
