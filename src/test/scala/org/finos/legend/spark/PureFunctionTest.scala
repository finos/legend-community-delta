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

import org.apache.spark.sql.functions.expr
import org.junit.Assert
import org.slf4j.{Logger, LoggerFactory}

class PureFunctionTest extends FunctionTest {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A constraint" should "return the fields it applies to" in {
    var spec = "|$this.nicknames->contains('TONY')".toValueSpec
    spec.getFieldItAppliesTo().foreach(println)
    assert(spec.getFieldItAppliesTo().toSet == Set("`nicknames`"))

    spec = "|$this.days->greaterThan($this.hours)".toValueSpec
    spec.getFieldItAppliesTo().foreach(println)
    assert(spec.getFieldItAppliesTo().toSet == Set("`days`", "`hours`"))
  }

  "left right functions" should "be converted as Spark SQL" in {

    var sql = "|$this.nicknames->contains('TONY')".pure2sql
    Assert.assertEquals("(`nicknames`) IN ('TONY')", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.languages->in('SCALA', 'JAVA', 'PYTHON')".pure2sql
    Assert.assertEquals("(`languages`) IN ('SCALA', 'JAVA', 'PYTHON')", sql)
    expr(sql)
    logger.info(sql)
  }

  "useless functions" should "be ignored" in {
    val sql = "|$this.first_name->toOne()->length()".pure2sql
    Assert.assertEquals("LENGTH((`first_name`))", sql)
    expr(sql)
    logger.info(sql)
  }

  "arithmetic functions" should "be converted as Spark SQL" in {

    var sql = "|$this.legend_complexity->times(5)".pure2sql
    Assert.assertEquals("(`legend_complexity`) * (5)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->divide(2)".pure2sql
    Assert.assertEquals("(`legend_complexity`) / (2)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->plus(15)".pure2sql
    Assert.assertEquals("(`legend_complexity`) + (15)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->minus(1)".pure2sql
    Assert.assertEquals("(`legend_complexity`) - (1)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->rem(3)".pure2sql
    Assert.assertEquals("(`legend_complexity`) % (3)", sql)
    expr(sql)
    logger.info(sql)
  }

  "comparative functions" should "be converted as Spark SQL" in {

    var sql = "|$this.legend_complexity->greaterThan(9)".pure2sql
    Assert.assertEquals("(`legend_complexity`) > (9)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->greaterThanEqual(10)".pure2sql
    Assert.assertEquals("(`legend_complexity`) >= (10)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->lessThan(80)".pure2sql
    Assert.assertEquals("(`legend_complexity`) < (80)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->lessThanEqual(80)".pure2sql
    Assert.assertEquals("(`legend_complexity`) <= (80)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_complexity->equal(5)".pure2sql
    Assert.assertEquals("(`legend_complexity`) = (5)", sql)
    expr(sql)
    logger.info(sql)
  }

  "Folded functions" should "be converted as Spark SQL" in {

    var sql = "|$this.legend->abs()".pure2sql
    Assert.assertEquals("ABS(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->count()".pure2sql
    Assert.assertEquals("SIZE(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->dayOfMonth()".pure2sql
    Assert.assertEquals("DAYOFMONTH(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->dayOfWeekNumber()".pure2sql
    Assert.assertEquals("DAYOFWWEEK(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->hour()".pure2sql
    Assert.assertEquals("HOUR(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->minute()".pure2sql
    Assert.assertEquals("MINUTE(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->monthNumber()".pure2sql
    Assert.assertEquals("MONTH(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|now()".pure2sql
    Assert.assertEquals("NOW()", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->quarterNumber()".pure2sql
    Assert.assertEquals("QUARTER(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->parseDate('yyyy-MM-dd')".pure2sql
    Assert.assertEquals("TO_DATE(`legend`, 'yyyy-MM-dd')", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->second()".pure2sql
    Assert.assertEquals("SECOND(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->round()".pure2sql
    Assert.assertEquals("ROUND(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->size()".pure2sql
    Assert.assertEquals("SIZE(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->split(',')".pure2sql
    Assert.assertEquals("SPLIT(`legend`, ',')", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->substring(0, 2)".pure2sql
    Assert.assertEquals("SUBSTRING(`legend`, 0, 2)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->toLower()".pure2sql
    Assert.assertEquals("LOWER(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->toUpper()".pure2sql
    Assert.assertEquals("UPPER(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->trim()".pure2sql
    Assert.assertEquals("TRIM(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->weekOfYear()".pure2sql
    Assert.assertEquals("WEEKOFYEAR(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->year()".pure2sql
    Assert.assertEquals("YEAR(`legend`)", sql)
    expr(sql)
    logger.info(sql)
  }

  "User defined functions" should "be converted as Spark SQL" in {

    var sql = "|$this.legend->startsWith('ANTOINE')".pure2sql
    Assert.assertEquals("STARTS_WITH(`legend`, 'ANTOINE')", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->endsWith('AMEND')".pure2sql
    Assert.assertEquals("ENDS_WITH(`legend`, 'AMEND')", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend_start->dateDiff($this.legend_end)".pure2sql
    Assert.assertEquals("DATE_DIFF(`legend_start`, `legend_end`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->firstDayOfMonth()".pure2sql
    Assert.assertEquals("FIRST_DAY_OF_MONTH(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->firstDayOfQuarter()".pure2sql
    Assert.assertEquals("FIRST_DAY_OF_QUARTER(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->firstDayOfWeek()".pure2sql
    Assert.assertEquals("FIRST_DAY_OF_WEEK(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|$this.legend->firstDayOfYear()".pure2sql
    Assert.assertEquals("FIRST_DAY_OF_YEAR(`legend`)", sql)
    expr(sql)
    logger.info(sql)

    sql = "|today()".pure2sql
    Assert.assertEquals("TODAY()", sql)
    expr(sql)
    logger.info(sql)
  }

  "AND / OR functions" should "be converted as Spark SQL" in {

    var sql = "|AND($this.legend->monthNumber()->equal(1), $this.legend->hour()->lessThan(3))".pure2sql
    Assert.assertEquals("((MONTH(`legend`)) = (1)) AND ((HOUR(`legend`)) < (3))", sql)
    expr(sql)
    logger.info(sql)

    sql = "|OR($this.legend->monthNumber()->equal(1), $this.legend->hour()->lessThan(3))".pure2sql
    Assert.assertEquals("((MONTH(`legend`)) = (1)) OR ((HOUR(`legend`)) < (3))", sql)
    expr(sql)
    logger.info(sql)

    sql = "|OR(AND($this.legend->monthNumber()->equal(1), $this.legend->hour()->lessThan(3)), $this.legend->minute()->greaterThan(10))".pure2sql
    Assert.assertEquals("(((MONTH(`legend`)) = (1)) AND ((HOUR(`legend`)) < (3))) OR ((MINUTE(`legend`)) > (10))", sql)
    expr(sql)
    logger.info(sql)

  }

}
