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

package org.finos.legend

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


package object spark {

  implicit class DataframeImpl(df: DataFrame) {

    def legendTransform(transformations: Map[String, String]): DataFrame = {
      transformations.foldLeft(df)((d, w) => d.withColumnRenamed(w._1, w._2))
    }

    def legendValidate(expectations: Seq[LegendExpectation], colName: String = "legend"): DataFrame = {

      val filter_constraints = udf((r: Row) => {
        val names = r.getAs[Seq[String]](0)
        val exprs = r.getAs[Seq[Boolean]](1)
        names.zip(exprs).filter(!_._2).map(_._1)
      })

      df
        .withColumn(
          colName,
          filter_constraints(
            struct(
              array(expectations.map(_.name).map(lit): _*),
              array(expectations.map(_.sql).map(expr): _*),
            )
          )
        )
    }
  }

  case class LegendExpectation(
                                name: String,
                                lambda: String,
                                sql: String = "1=1"
                              )

  case class LegendMapping(
                            schema: StructType,
                            mapping: Map[String, String],
                            expectations: Seq[LegendExpectation],
                            table: String
                                     ) {

    def targetSchema: StructType = {
      SparkSession.getActiveSession match {
        case Some(spark) =>
          val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
          mapping.foldLeft(df)((d, t) => d.withColumnRenamed(t._1, t._2)).schema
        case _ => throw new IllegalArgumentException("There should be an active spark session")
      }
    }

    def targetDDL(location: String = ""): String = {
      val ddl = s"""
        |CREATE DATABASE IF NOT EXISTS ${table.split("\\.").head};
        |CREATE TABLE $table
        |USING DELTA
        |(
        |\t${targetSchema.fields.map(_.toDDL).mkString(",\n\t")}
        |)""".stripMargin

      if (location != null && location.nonEmpty) s"$ddl\nLOCATION '${location}';" else s"$ddl;"
    }

  }

}
