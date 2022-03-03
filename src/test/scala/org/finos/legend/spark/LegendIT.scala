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

class LegendIT extends AnyFlatSpec {

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

    val legend = LegendClasspathLoader.loadResources("model")
    val mapping = legend.getMapping("databricks::mapping::employee_delta")
    val schema = legend.getMappingSchema(mapping)
    val transformations = legend.getMappingTransformations(mapping)
    val expectations = legend.getMappingExpectations(mapping)
    val table = legend.getMappingTable(mapping, true)

    expectations.foreach(s => {
      println(s._1 + "\t" + s._2)
    })

    val inputDF = spark.read.format("json").schema(schema).load(dataFile)
    inputDF.show(truncate = false)

    val mappedDF = inputDF.legendTransform(transformations)
    mappedDF.show(truncate = false)

    val cleanedDF = mappedDF.legendValidate(expectations, "legend").withColumn("legend", explode(col("legend")))
    cleanedDF.show(truncate = false)

    val test = cleanedDF.groupBy("legend").count()
    test.show(truncate = false)

    assert(test.count() == 3)
    val failed = test.rdd.map(r => r.getAs[String](("legend"))).collect().map(_.split(" ").head.trim).toSet
    assert(failed == Set("[id]", "[sme]", "[age]"))

    val expected = Source.fromInputStream(this.getClass.getResourceAsStream("/data/output_table.sql")).getLines().mkString("\n")
    assert(expected == table)

  }

}
