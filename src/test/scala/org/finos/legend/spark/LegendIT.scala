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

import java.net.URL
import java.nio.file.{Path, Paths}
import java.util.Objects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.finos.legend.spark.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class LegendIT extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "Raw collateral files" should "be processed fully from a legend specs" in {

    val spark = SparkSession.builder().master("local").appName("legend").getOrCreate()
    import spark.implicits._

    spark.registerLegendUDFs()

    val dataModel = "fire::collateral"
    val legend = LegendClasspathLoader.loadResources("fire-model")
    val schema = legend.getEntitySchema(dataModel)
    val expectations = legend.getEntityExpectations(dataModel)
    val derivations = legend.getEntityDerivations(dataModel)

    val input_df = spark
      .read
      .format("json")
      .schema(schema)
      .load(getCollateralDirectory)

    input_df.show(truncate = false)

    val output_df = input_df.legendExpectations(expectations)
    output_df
      .withColumn("legend", explode(col("legend")))
      .select("id", "encumbrance_amount", "start_date", "end_date", "currency_code", "legend")
      .show(truncate = false)

    val derivationDDL = derivations.flatMap(p => p.toDDL.toOption).toDF("derivation")
    derivationDDL.show(2, truncate = false)

  }

  def getCollateralDirectory: String = {
    val url: URL = Objects.requireNonNull(this.getClass.getClassLoader.getResource("sample-data"))
    val directory: Path = Paths.get(url.toURI)
    directory.toString
  }

}
