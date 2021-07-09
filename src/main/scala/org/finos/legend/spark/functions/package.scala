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

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTime, Days, Months, Weeks, Years}

package object functions {

  implicit class SparkImpl(spark: SparkSession) {
    /**
     * Some functions that are not supported as 1:1 between PURE and Spark have been re-coded as custom UDFs
     * The UDFs must be registered on an active spark context
     */
    def registerLegendUDFs(): Unit = legendUDFs.map(s => spark.udf.register(s._1, s._2))
  }

  val STARTS_WITH: UserDefinedFunction = udf((s: String, prefix: String) => s.startsWith(prefix))

  val ENDS_WITH: UserDefinedFunction = udf((s: String, suffix: String) => s.endsWith(suffix))

  val DATE_DIFF: UserDefinedFunction = udf((from: Date, to: Date, duration: String) => {
    val fromJoda = new DateTime(from.getTime)
    val toJoda = new DateTime(to.getTime)
    duration match {
      case "DAYS"   => Days.daysBetween(fromJoda, toJoda).getDays
      case "WEEKS"  => Weeks.weeksBetween(fromJoda, toJoda).getWeeks
      case "MONTHS" => Months.monthsBetween(fromJoda, toJoda).getMonths
      case "YEAR"   => Years.yearsBetween(fromJoda, toJoda).getYears
      case _ => throw new IllegalArgumentException(s"Duration [${duration}] is not supported")
    }
  })

  val FIRST_DAY_OF_MONTH: UserDefinedFunction = udf((d: Date) => {
    val millis = new DateTime(d.getTime)
      .withDayOfMonth(1)
      .withTimeAtStartOfDay()
      .getMillis
    new Date(millis)
  })

  val FIRST_DAY_OF_QUARTER: UserDefinedFunction = udf((d: Date) => {
    val date = new DateTime(d)
    val millis = date.withMonthOfYear(((date.getMonthOfYear / 3) + 1) * 3 - 2)
      .withDayOfMonth(1)
      .withTimeAtStartOfDay()
      .getMillis
    new Date(millis)
  })

  val FIRST_DAY_OF_WEEK: UserDefinedFunction = udf((d: Date) => {
    val date = new DateTime(d)
    val millis = date.withDayOfWeek(1).withTimeAtStartOfDay().getMillis
    new Date(millis)
  })

  val FIRST_DAY_OF_YEAR: UserDefinedFunction = udf((d: Date) => {
    val date = new DateTime(d)
    val millis = date.withDayOfYear(1).withTimeAtStartOfDay().getMillis
    new Date(millis)
  })

  val TODAY: UserDefinedFunction = udf(() => new Date(new DateTime().withTimeAtStartOfDay().getMillis))

  val legendUDFs: Map[String, UserDefinedFunction] = Map(
    "STARTS_WITH" -> STARTS_WITH,
    "ENDS_WITH" -> ENDS_WITH,
    "DATE_DIFF" -> DATE_DIFF,
    "FIRST_DAY_OF_MONTH" -> FIRST_DAY_OF_MONTH,
    "FIRST_DAY_OF_QUARTER" -> FIRST_DAY_OF_QUARTER,
    "FIRST_DAY_OF_WEEK" -> FIRST_DAY_OF_WEEK,
    "FIRST_DAY_OF_YEAR" -> FIRST_DAY_OF_YEAR,
    "TODAY" -> TODAY
  )


}
