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
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec

class LegendSparkTest extends AnyFlatSpec {

  val dataFile: String = {
    val url: URL = Objects.requireNonNull(this.getClass.getClassLoader.getResource("data"))
    val directory: Path = Paths.get(url.toURI)
    if (!Files.isDirectory(directory)) throw new IllegalAccessException("Not a directory: " + directory)
    s"$directory/employee.json"
  }

  val spark: SparkSession = SparkSession.builder().appName("TEST").master("local[1]").getOrCreate()
  Logger.getLogger("Alloy Execution Server").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  "Raw files" should "be processed fully from a legend specs" in {

    val legend = LegendClasspathLoader.loadResources()

    // 1. READ: Apply schema of the PURE entity
    val schema = legend.getSchema("databricks::entity::employee")
    val bronze = spark.read.format("json").schema(schema).load(dataFile)
    bronze.show(truncate = false)

    // 2. TRANSFORM: Convert source into target field
    val transformations = legend.getTransformations("databricks::mapping::employee_delta")
    val silver = bronze.legendTransform(transformations)
    silver.show(truncate = false)

    // 3. VALIDATE: Apply all necessary constraints
    val expectations = legend.getExpectations("databricks::mapping::employee_delta").mapValues(_.get)
    val gold = silver.legendValidate(expectations, "legend").withColumn("legend", explode(col("legend")))
    gold.show(truncate = false)

    // 4. DERIVATION: Apply all derived properties
    val derivations = legend.getDerivations("databricks::mapping::employee_delta").mapValues(_.get)
    val view = derivations.foldLeft(silver)((d, w) => d.withColumn(w._1, expr(w._2)))
    view.show(truncate = false)

    // 5. ASSERT
    val dqm = gold.groupBy("legend").count()
    dqm.show(truncate = false)
    assert(dqm.count() == 3)
    val failed = dqm.rdd.map(r => r.getAs[String]("legend")).collect().map(_.split(" ").head.trim).toSet
    assert(failed == Set("[id]", "[sme]", "[hiringAge]"))
  }

}
