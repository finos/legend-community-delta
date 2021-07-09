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

package org.finos.legend.spark.functions

/**
 * Best effort mapping between PURE language and SQL expressions
 * We won't have a 1:1 mapping with all functions available in the PURE stack. Some could be build from primitive SQL functions
 * Some others could (potentially) be defined and registered as USER_DEFINED_FUNCTIONS
 */
object LegendPureFunctions {

  // Function mapping between Legend and Spark SQL.
  // For these type of function, we expect a left and a right parameter
  val LEGEND_FUNCTIONS_LR = Map(
    "OR" -> "OR",
    "AND" -> "AND",
    "in" -> "IN",
    "contains" -> "IN",
    "equal" -> "=",
    "lessThanEqual" -> "<=",
    "lessThan" -> "<",
    "greaterThanEqual" -> ">=",
    "greaterThan" -> ">",
    "divide" -> "/",
    "times" -> "*",
    "plus" -> "+",
    "minus" -> "-",
    "rem" -> "%"
  )

  // https://legend.finos.org/docs/language/released-functions
  // https://spark.apache.org/docs/3.1.1/api/sql/index.html
  // Function mapping between Legend and Spark SQL. Whilst some could be ported as is, some other have been coded as UDF
  // For these type of function, we unfold parameters
  val LEGEND_FUNCTIONS_FOLD = Map(
    "toOne" -> "", // useless function, but this is used in Legend to convert optional object into its value
    "abs" -> "ABS",
    "average" -> "AVG",
    "count" -> "SIZE",
    "dateDiff" -> "DATE_DIFF", //UDF
    "dayOfMonth" -> "DAYOFMONTH",
    "dayOfWeekNumber" -> "DAYOFWWEEK",
    "endsWith" -> "ENDS_WITH",
    "firstDayOfMonth" -> "FIRST_DAY_OF_MONTH", //UDF
    "firstDayOfQuarter" -> "FIRST_DAY_OF_QUARTER", //UDF
    "firstDayOfWeek" -> "FIRST_DAY_OF_WEEK", //UDF
    "firstDayOfYear" -> "FIRST_DAY_OF_YEAR", //UDF
    "hour" -> "HOUR",
    "length" -> "LENGTH",
    "minute" -> "MINUTE",
    "monthNumber" -> "MONTH",
    "now" -> "NOW",
    "parseDate" -> "TO_DATE",
    "quarterNumber" -> "QUARTER",
    "round" -> "ROUND",
    "second" -> "SECOND",
    "size" -> "SIZE",
    "split" -> "SPLIT",
    "startsWith" -> "STARTS_WITH", // UDF
    "substring" -> "SUBSTRING",
    "toLower" -> "LOWER",
    "toUpper" -> "UPPER",
    "today" -> "TODAY", //UDF
    "trim" -> "TRIM",
    "weekOfYear" -> "WEEKOFYEAR",
    "year" -> "YEAR"
  )

}
