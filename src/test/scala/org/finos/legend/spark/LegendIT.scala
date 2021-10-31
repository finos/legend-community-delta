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

import java.net.URL
import java.nio.file.{Files, Path, Paths}
import java.util.Objects

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class LegendIT extends AnyFlatSpec {

  val dataPath: String = {
    val url: URL = Objects.requireNonNull(this.getClass.getClassLoader.getResource("data"))
    val directory: Path = Paths.get(url.toURI)
    if (!Files.isDirectory(directory)) throw new IllegalAccessException("Not a directory: " + directory)
    directory.toString
  }

  val spark: SparkSession = SparkSession.builder().appName("TEST").master("local[1]").getOrCreate()
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "Raw files" should "be processed fully from a legend specs" in {

    val legend = LegendClasspathLoader.loadResources("model")

    val legendStrategy = legend.buildStrategy(
      "databricks::employee",
      "databricks::lakehouse::emp2delta",
      "databricks::lakehouse::runtime"
    )

    val inputDF = spark.read.format("csv").schema(legendStrategy.schema).load(dataPath)
    inputDF.show()

    val mappedDF = inputDF.legendTransform(legendStrategy.transformations)
    mappedDF.show()

    val cleanedDF = mappedDF.legendValidate(legendStrategy.expectations, "legend")
    cleanedDF.withColumn("legend", explode(col("legend"))).show()
    val test = cleanedDF.withColumn("legend", explode(col("legend"))).groupBy("legend").count()
    test.show()
    assert(test.count() == 2)
    assert(legendStrategy.targetTable == "legend.employee")


  }

}
