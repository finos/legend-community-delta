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

import scala.io.Source

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
    val schema = legend.getSchema("databricks::mapping::employee_delta")
    val transformations = legend.getTransformations("databricks::mapping::employee_delta")
    val expectations = legend.getExpectations("databricks::mapping::employee_delta")
    val derivations = legend.getDerivations("databricks::mapping::employee_delta")
    val table = legend.getTable("databricks::mapping::employee_delta")

    assert(table == "legend.employee")
    assert(!expectations.exists(_._2.isFailure))
    assert(!derivations.exists(_._2.isFailure))

    val inputDF = spark.read.format("json").schema(schema).load(dataFile)

    inputDF.show(truncate = false)

    val mappedDF = inputDF.legendTransform(transformations)

    mappedDF.show(truncate = false)

    val validExpectations = expectations.filter(_._2.isSuccess).mapValues(_.get)
    val cleanedDF = mappedDF
      .legendValidate(validExpectations, "legend")
      .withColumn("legend", explode(col("legend")))

    cleanedDF.show(truncate = false)

    val validDerivations = derivations.filter(_._2.isSuccess).mapValues(_.get)
    val df1 = validDerivations.foldLeft(mappedDF)((d, w) => d.withColumn(w._1, expr(w._2)))
    val df2 = transformations.foldLeft(df1)((d, w) => d.withColumnRenamed(w._2, w._1))

    df2.show(truncate = false)

    val test = cleanedDF.groupBy("legend").count()

    test.show(truncate = false)

    assert(test.count() == 3)

    val failed = test.rdd.map(r => r.getAs[String]("legend"))
      .collect().map(_.split(" ").head.trim).toSet

    assert(failed == Set("[id]", "[sme]", "[hiringAge]"))

  }

}
