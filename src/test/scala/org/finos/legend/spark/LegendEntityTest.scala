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

import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths

class LegendEntityTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "A legend data type" should "be converted into spark data type" in {
    val returnTypes = Map(
      "String" -> StringType,
      "Boolean" -> BooleanType,
      "Binary" -> BinaryType,
      "Integer" -> IntegerType,
      "Number" -> LongType,
      "Float" -> FloatType,
      "Decimal" -> DoubleType,
      "Date" -> DateType,
      "StrictDate" -> DateType,
      "DateTime" -> TimestampType
    )
    returnTypes.foreach({ case (returnType, dataType) =>
      val converted = LegendUtils.convertDataTypeFromString(returnType)
      assert(converted == dataType, s"[$returnType] should be converted as [$dataType], got [$converted]")
    })
  }

  it should "fail for invalid types" in {
    assertThrows[IllegalArgumentException] {
      LegendUtils.convertDataTypeFromString("foobar")
    }
  }

  "A legend model" should "be loaded from classpath" in {
    assert(LegendClasspathLoader.loadResources().getEntityNames.nonEmpty)
  }

  it should "be loaded from external directory" in {
    val path = Paths.get("src","test", "resources")
    val absolutePath = path.toFile.getAbsolutePath
    assert(LegendFileLoader.loadResources(absolutePath).getEntityNames.nonEmpty)
  }

  "A legend entity" should "be loaded from pure model" in {
    val legend = LegendClasspathLoader.loadResources()
    assert(legend.getEntityNames.contains("databricks::entity::person"))
  }

  "A pure entity" should "be loaded from pure model" in {
    val legend = LegendClasspathLoader.loadResources()
    legend.getSchema("databricks::mapping::employee_delta")
  }

  it should "be converted as a spark schema" in {
    val legend = LegendClasspathLoader.loadResources()
    assert(legend.getEntityNames.contains("databricks::entity::person"))
    val fields = legend.getSchema("databricks::entity::person").fields.map(_.name)
    assert(fields.toSet == Set("firstName", "lastName", "birthDate", "gender"))
  }

  it should "support supertype entities" in {
    val legend = LegendClasspathLoader.loadResources()
    assert(legend.getEntityNames.contains("databricks::entity::employee"))
    val personFields = legend.getSchema("databricks::entity::person").fields.map(_.name)
    val employeeFields = legend.getSchema("databricks::entity::employee").fields.map(_.name)
    assert(employeeFields.diff(personFields).toSet == Set("highFives", "sme", "id", "joinedDate"))
  }

  it should "include entity expectations" in {
    val legend = LegendClasspathLoader.loadResources()
    val expectations = legend.getExpectations("databricks::entity::employee", compile = false)
    assert(!expectations.exists(_._2.isFailure))
    assert(
      expectations.values.map(_.get).toSet == Set(
        "$this.highFives > 0",
        "$this.firstName->isNotEmpty()",
        "$this.sme->isEmpty() || $this.sme->in(['Scala', 'Python', 'Java', 'R', 'SQL'])",
        "$this.id->isNotEmpty()",
        "$this.birthDate->isNotEmpty()",
        "$this.lastName->isNotEmpty()",
        "$this.joinedDate->isNotEmpty()",
        "$this.hiringAge > 18"
      )
    )
  }

  "Entity expectations" should "return SQL expressions" in {
    val legend = LegendClasspathLoader.loadResources()
    val expectations = legend.getExpectations("databricks::entity::employee")
    assert(!expectations.exists(_._2.isFailure))
    assert(
      expectations.values.map(_.get).toSet == Set(
        "birthDate IS NOT NULL",
        "sme IS NULL OR sme IN ('Scala', 'Python', 'Java', 'R', 'SQL')",
        "id IS NOT NULL",
        "joinedDate IS NOT NULL",
        "firstName IS NOT NULL",
        "lastName IS NOT NULL"
      )
    )
  }

  "A legend mapping" should "be loaded from pure model" in {
    val legend = LegendClasspathLoader.loadResources()
    assert(legend.getEntityNames.contains("databricks::mapping::employee_delta"))
  }

  it should "include a source schema" in {
    val legend = LegendClasspathLoader.loadResources()
    val fields = legend.getSchema("databricks::mapping::employee_delta").fields.map(_.name).toSet
    assert(fields == Set("high_fives", "joined_date", "last_name", "first_name", "birth_date", "id", "sme", "gender"))
  }

  it should "include entity expectations" in {
    val legend = LegendClasspathLoader.loadResources()
    val expectations = legend.getExpectations("databricks::mapping::employee_delta", compile = false)
    assert(!expectations.exists(_._2.isFailure))
    assert(
      expectations.values.map(_.get).toSet == Set(
        "$this.highFives > 0",
        "$this.firstName->isNotEmpty()",
        "$this.sme->isEmpty() || $this.sme->in(['Scala', 'Python', 'Java', 'R', 'SQL'])",
        "$this.id->isNotEmpty()",
        "$this.birthDate->isNotEmpty()",
        "$this.lastName->isNotEmpty()",
        "$this.joinedDate->isNotEmpty()",
        "$this.hiringAge > 18"
      )
    )
  }

  "Mapping expectations" should "return SQL expressions" in {
    val legend = LegendClasspathLoader.loadResources()
    val expectations = legend.getExpectations("databricks::mapping::employee_delta")
    assert(!expectations.exists(_._2.isFailure))
    assert(
      expectations.values.map(_.get).toSet == Set(
        "year(joined_date) - year(birth_date) > 18",
        "id IS NOT NULL",
        "last_name IS NOT NULL",
        "first_name IS NOT NULL",
        "(sme IS NULL OR sme IN ('Scala', 'Python', 'Java', 'R', 'SQL'))",
        "birth_date IS NOT NULL",
        "joined_date IS NOT NULL",
        "(high_fives IS NOT NULL AND high_fives > 0)"
      )
    )
  }

  "A legend mapping" should "capture transformations" in {
      val legend = LegendClasspathLoader.loadResources()
      val transform = legend.getTransformations("databricks::mapping::employee_delta")
      assert(transform.keys.toSet == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
      assert(transform.values.toSet == Set("high_fives", "joined_date", "last_name", "first_name", "birth_date", "id", "sme", "gender"))
  }

  it should "yield derivations" in {
    val legend = LegendClasspathLoader.loadResources()
    val derivations = legend.getDerivations("databricks::mapping::employee_delta", compile = false)
    assert(derivations.keySet == Set("hiringAge", "age", "initials"))
    assert(!derivations.exists(_._2.isFailure))
    assert(derivations.mapValues(_.get).values.toSet == Set(
      "$this.birthDate->dateDiff($this.joinedDate,DurationUnit.YEARS)",
      "$this.birthDate->dateDiff(today(),DurationUnit.YEARS)",
      "$this.firstName->substring(0,1) + $this.lastName->substring(0,1)"
    ))
  }

  "derivations" should "be compiled as SQL" in {
    val legend = LegendClasspathLoader.loadResources()
    val derivations = legend.getDerivations("databricks::mapping::employee_delta")
    assert(!derivations.exists(_._2.isFailure))
    assert(derivations.mapValues(_.get).values.toSet == Set(
      "year(joined_date) - year(birth_date)",
      "year(current_date) - year(birth_date)",
      "concat(substring(first_name, 0, 1), substring(last_name, 0, 1))")
    )
  }

  "A legend mapping" should "be compiled as SQL query" in {
    val legend = LegendClasspathLoader.loadResources()
    val observed = legend.generateSql("databricks::mapping::employee_delta")
    assert(observed == """select
        |`root`.high_fives as `highFives`,
        |`root`.joined_date as `joinedDate`,
        |`root`.last_name as `lastName`,
        |`root`.first_name as `firstName`,
        |`root`.birth_date as `birthDate`,
        |`root`.id as `id`,
        |`root`.sme as `sme`,
        |`root`.gender as `gender`,
        |year(`root`.joined_date) - year(`root`.birth_date) as `hiringAge`,
        |year(current_date) - year(`root`.birth_date) as `age`,
        |concat(substring(`root`.first_name, 0, 1), substring(`root`.last_name, 0, 1)) as `initials`
        |from legend.employee as `root`
        |where (`root`.high_fives is not null and `root`.high_fives > 0)
        |and `root`.first_name is not null
        |and `root`.joined_date is not null
        |and `root`.id is not null
        |and (`root`.sme is null or `root`.sme in ('Scala', 'Python', 'Java', 'R', 'SQL'))
        |and `root`.birth_date is not null
        |and `root`.last_name is not null"""
        .stripMargin.split("\n")
        .mkString(" ")
        .replaceAll("\\s+", " "))
  }

  "A legend service" should "be compiled as SQL query" in {
    val legend = LegendClasspathLoader.loadResources()
    val observed = legend.generateSql("databricks::service::skills")
    assert(observed == """select
        |`root`.gender as `Gender`,
        |avg(1.0 * `root`.high_fives) as `HighFives`,
        |count(`root`.id) as `Employees`
        |from legend.employee as `root`
        |where not `root`.gender is null
        |group by `Gender`
        |order by `HighFives` desc
        |limit 10"""
        .stripMargin.split("\n")
        .mkString(" ")
        .replaceAll("\\s+", " "))
  }
}
