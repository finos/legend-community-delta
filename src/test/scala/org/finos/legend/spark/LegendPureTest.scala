///*
// * SPDX-License-Identifier: Apache-2.0
// * Copyright 2021 Databricks - see NOTICE.md file
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.finos.legend.spark
//
//import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
//import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.AppliedFunction
//import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException
//import org.scalatest.flatspec.AnyFlatSpec
//import org.slf4j.{Logger, LoggerFactory}
//
//import scala.collection.JavaConverters._
//
//class LegendPureTest extends AnyFlatSpec {
//
//  val logger: Logger = LoggerFactory.getLogger(this.getClass)
//
//  "Pure model" should "be generated from SDLC entities" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val pureModel = legend.pureModel
//    assertThrows[EngineException] {
//      pureModel.getMapping("foo:bar")
//    }
//    val mapping = pureModel.getMapping("databricks::mapping::employee_delta")
//    assert(mapping._name() == "employee_delta")
//  }
//
//  "A lambda function" should "be generated from a string representation" in {
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.highFives > 20)"
//    val function = LegendUtils.buildLambda(lambdaString)
//    assert(function.isInstanceOf[AppliedFunction])
//    assert(function.asInstanceOf[AppliedFunction].function == "filter")
//  }
//
//  it should "be validated against a pure model" in {
//    val legend = LegendClasspathLoader.loadResources()
//    assertThrows[EngineException] {
//      val lambdaString = "test->getAll()->filter(x|$x.foo = 'bar')"
//      LegendUtils.buildLambda(lambdaString, legend.pureModel)
//    }
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.highFives > 20)"
//    val function = LegendUtils.buildLambda(lambdaString, legend.pureModel)
//    assert(function._expressionSequence().asScala.nonEmpty)
//  }
//
//  it should "generate a valid execution plan" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    assertThrows[EngineException] {
//      val lambdaString = "foobar->getAll()->filter(x|$x.foo = 'bar')"
//      LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    }
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.highFives > 20)"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    assert(plan.rootExecutionNode.executionNodes.get(0).isInstanceOf[SQLExecutionNode])
//  }
//
//  it should "support NULL functions" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.highFives > 20)"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlWhere(sqlPlan)
//    assert(sql == "(high_fives IS NOT NULL AND high_fives > 20)")
//  }
//
//  it should "support IN functions" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.firstName->in(['antoine', 'junta']))"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlWhere(sqlPlan)
//    assert(sql == "first_name IN ('antoine', 'junta')")
//  }
//
//  it should "support complex spark functions" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.id->isEmpty())"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlWhere(sqlPlan)
//    assert(sql == "id IS NULL")
//  }
//
//  "A more complex function" should "be converted as SQL clause" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee->getAll()->filter(x|$x.birthDate->dateDiff($x.joinedDate, DurationUnit.YEARS) > 20)"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlWhere(sqlPlan)
//    assert(sql == "year(joined_date) - year(birth_date) > 20")
//  }
//
//  "A qualified property" should "be use native spark operations" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee.all()->project([x|$x.age], ['age'])"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlSelect(sqlPlan)
//    assert(sql == "year(current_date) - year(birth_date) AS `age`")
//  }
//
//  it should "be converted as SQL clause" in {
//    val legend = LegendClasspathLoader.loadResources()
//    val mapping = legend.getMapping("databricks::mapping::employee_delta")
//    val lambdaString = "databricks::entity::employee.all()->project([x|$x.hiringAge], ['hiringAge'])"
//    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, legend.pureRuntime, legend.pureModel)
//    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
//    val sql = LegendUtils.parseSqlSelect(sqlPlan)
//    assert(sql == "year(joined_date) - year(birth_date) AS `hiringAge`")
//  }
//}
