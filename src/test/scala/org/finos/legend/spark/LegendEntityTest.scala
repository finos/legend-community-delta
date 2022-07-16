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

import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.{PlainSelect, Select}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.finos.legend.engine.language.pure.compiler.Compiler
import org.finos.legend.engine.language.pure.compiler.toPureGraph.{CompileContext, HelperValueSpecificationBuilder, PureModel}
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser
import org.finos.legend.engine.plan.generation.PlanGenerator
import org.finos.legend.engine.plan.platform.PlanPlatform
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.Function
import org.finos.legend.spark.LegendUtils.buildLambda
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{BufferedReader, InputStreamReader, StringReader}
import java.nio.file.Paths
import java.util.Objects
import java.util.stream.Collectors

class LegendEntityTest extends AnyFlatSpec {

  Logger.getLogger("Alloy Execution Server").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

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
    val fields = legend.getEntitySchema("databricks::entity::person").fields.map(_.name)
    assert(fields.toSet == Set("firstName", "lastName", "birthDate", "gender"))
  }

  it should "support supertype entities" in {
    val legend = LegendClasspathLoader.loadResources()
    assert(legend.getEntityNames.contains("databricks::entity::employee"))
    val personFields = legend.getEntitySchema("databricks::entity::person").fields.map(_.name)
    val employeeFields = legend.getEntitySchema("databricks::entity::employee").fields.map(_.name)
    assert(employeeFields.diff(personFields).toSet == Set("highFives", "sme", "id", "joinedDate"))
  }

  it should "include valid expectations" in {
    val legend = LegendClasspathLoader.loadResources()
    val expectations = legend.getEntityExpectations(
      "databricks::entity::employee"
    )
    assert(
      expectations.values.toSet == Set(
        "birthDate IS NOT NULL",
        "sme IS NULL OR sme IN ('Scala', 'Python', 'C', 'Java', 'R', 'SQL')",
        "id IS NOT NULL",
        "joinedDate IS NOT NULL",
        "firstName IS NOT NULL",
        "lastName IS NOT NULL"
      )
    )
  }

  "A legend mapping" should "be loaded from pure model" in {
    val legend = LegendClasspathLoader.loadResources()
    legend.getMapping("databricks::mapping::employee_delta")
  }

  it should "include a source schema" in {
    val legend = LegendClasspathLoader.loadResources()
    val fields = legend.getMappingSchema("databricks::mapping::employee_delta").fields.map(_.name).toSet
    assert(fields == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
  }

  it should "compile PURE expectations to SQL" in {
    val legend = LegendClasspathLoader.loadResources()
    val transform = legend.getMappingExpectations("databricks::mapping::employee_delta")
    assert(transform.values.toSet.contains("year(joined_date) - year(birth_date) > 18"))
    assert(transform.values.toSet.contains("(high_fives IS NOT NULL AND high_fives > 0)"))
  }

  it should "capture transformations" in {
    val legend = LegendClasspathLoader.loadResources()
    val transform = legend.getMappingTransformations("databricks::mapping::employee_delta")
    assert(transform.keys.toSet == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
    assert(transform.values.toSet == Set("high_fives", "joined_date", "last_name", "first_name", "birth_date", "id", "sme", "gender"))
  }

  it should "yield a spark schema" in {
    SparkSession.getActiveSession match {
      case Some(_) =>
      case _ => SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    }
    val legend = LegendClasspathLoader.loadResources()
    val schema = legend.getMappingSchema("databricks::mapping::employee_delta")
    assert(schema.fields.map(_.name).toSet == Set("highFives", "joinedDate", "lastName", "firstName", "birthDate", "id", "sme", "gender"))
  }

  it should "yield derivations" in {
    val legend = LegendClasspathLoader.loadResources()
    val derivations = legend.getDerivations("databricks::mapping::employee_delta")
    assert(derivations.keySet == Set("hiringAge", "age"))
    assert(derivations.values.toSet == Set(
      "year(joined_date) - year(birth_date) AS `hiringAge`",
      "year(current_date) - year(birth_date) AS `age`")
    )
  }

  "A dummy test" should "remain dummy" in {

    val inputStreamReader = new InputStreamReader(Objects.requireNonNull(this.getClass.getResourceAsStream("/pure/databricks.pure")))
    val bufferedReader = new BufferedReader(inputStreamReader)
    val contextData = PureGrammarParser.newInstance.parseModel(bufferedReader.lines.collect(Collectors.joining("\n")))
    val pureModel = Compiler.compile(contextData, null, null)

    val fetchFunction = contextData.getElementsOfType(classOf[Function]).stream.filter((x: Function) => "test::fetch" == x._package + "::" + x.name).findFirst.orElseThrow(() => new IllegalArgumentException("Unknown function"))

    PlanGenerator.generateExecutionPlan(
      HelperValueSpecificationBuilder.buildLambda(fetchFunction.body, fetchFunction.parameters, new CompileContext.Builder(pureModel).build),
      pureModel.getMapping("test::Map"),
      pureModel.getRuntime("test::Runtime"),
      null,
      pureModel,
      "vX_X_X", //TODO: Replace by PureVersion.production when https://github.com/finos/legend-pure/pull/507
      PlanPlatform.JAVA,
      null,
      null,
      null
    )
  }

  "A legend service" should "be compiled as SQL query" in {
    val legend = LegendClasspathLoader.loadResources()
    val sql = legend.generateSql("databricks::mapping::employee_delta")
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(sql)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    assert(select.getFromItem.getAlias.getName == "`root`")
  }

}
