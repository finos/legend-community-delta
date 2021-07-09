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
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, expr}
import org.finos.legend.spark.functions._
import org.junit.Assert
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

class PureUDFFunctionTest extends FunctionTest {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "user define functions" should "be validated as Spark SQL" in {

    val spark: SparkSession = SparkSession.builder().master("local[1]").appName("legend").getOrCreate()
    import spark.implicits._
    spark.registerLegendUDFs()

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val records = Seq(
      ("Antoine", "Amend ", new Date(sdf.parse("1983-06-09").getTime)),
      ("Bob", "Smith", new Date(sdf.parse("1982-01-09").getTime)),
      ("Amanda", "Dupond", new Date(sdf.parse("1953-01-12").getTime))
    )
      .toDF("first_name", "last_name", "birth_date")

    val sql1 = "|$this.first_name->startsWith('A')".pure2sql
    val sql2 = "|$this.last_name->toUpper()->trim()->endsWith('D')".pure2sql
    val sql3 = "|$this.birth_date->dateDiff(today(), 'YEAR')->lessThan(40)".pure2sql // We have a good 2 years before UNIT TEST blows

    logger.info(sql1)
    logger.info(sql2)
    logger.info(sql3)

    val filtered = records.filter(expr(sql1)).filter(expr(sql2)).filter(expr(sql3))
    Assert.assertEquals(filtered.count(), 1L)

    records
      .withColumn("days_since_birth", expr("|$this.birth_date->dateDiff(today(), 'DAYS')".pure2sql))
      .withColumn("weeks_since_birth", expr("|$this.birth_date->dateDiff(today(), 'WEEKS')".pure2sql))
      .withColumn("months_since_birth", expr("|$this.birth_date->dateDiff(today(), 'MONTHS')".pure2sql))
      .withColumn("years_since_birth", expr("|$this.birth_date->dateDiff(today(), 'YEAR')".pure2sql))
      .show()

    val expectations = Seq(
      Expectation(Seq.empty[String], "[first_name] should start with A", None, Try(sql1)),
      Expectation(Seq.empty[String], "[last_name] should end with D", None, Try(sql2)),
      Expectation(Seq.empty[String], "[customer] should not be more than 40yo", None, Try(sql3)),
    )

    val expectedRecords = records
      .legendExpectations(expectations, colName = "legend")
      .withColumn("legend", explode(col("legend")))

    expectedRecords.show(10, truncate = false)
    Assert.assertEquals(expectedRecords.count(), 3L)

  }

}
