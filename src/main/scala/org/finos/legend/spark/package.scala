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
import org.apache.spark.sql.{DataFrame, Row}

package object spark {

  implicit class DataframeImpl(df: DataFrame) {

    def legendTransform(transformations: Seq[LegendRelationalTransformation]): DataFrame = {
      transformations
        .foldLeft(df)((d, w) => d.withColumnRenamed(w.from, w.to))
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

  case class LegendRelationalTransformation(
                                             from: String,
                                             to: String
                                           )

  case class LegendRelationalStrategy(
                                       schema: StructType,
                                       transformations: Seq[LegendRelationalTransformation],
                                       expectations: Seq[LegendExpectation],
                                       targetTable: String
                                     )

}
