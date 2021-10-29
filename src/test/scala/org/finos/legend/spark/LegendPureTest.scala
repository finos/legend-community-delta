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

import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.AppliedFunction
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class LegendPureTest extends AnyFlatSpec {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "Pure model" should "be generated from SDLC entities" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val pureModel = legend.pureModel

    assertThrows[EngineException] {
      pureModel.getMapping("foo:bar")
    }

    val mapping = pureModel.getMapping("databricks::lakehouse::emp2delta")
    assert(mapping._name() == "emp2delta")

    val runtime = pureModel.getRuntime("databricks::lakehouse::runtime")
    assert(runtime._connections().asScala.nonEmpty)

  }

  "A lambda function" should "be generated from a string representation" in {
    val lambdaString = "databricks::employee->getAll()->filter(x|$x.high_fives > 20)"
    val function = LegendUtils.buildLambda(lambdaString)
    assert(function.isInstanceOf[AppliedFunction])
    assert(function.asInstanceOf[AppliedFunction].function == "filter")
  }

  it should "be validated against a pure model" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))

    assertThrows[EngineException] {
      val lambdaString = "test->getAll()->filter(x|$x.foo = 'bar')"
      LegendUtils.buildLambda(lambdaString, legend.pureModel)
    }

    val lambdaString = "databricks::employee->getAll()->filter(x|$x.high_fives > 20)"
    val function = LegendUtils.buildLambda(lambdaString, legend.pureModel)
    assert(function._expressionSequence().asScala.nonEmpty)
  }

  it should "generate an execution plan" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))

    val mapping = legend.getMapping("databricks::lakehouse::emp2delta")
    val runtime = legend.getRuntime("databricks::lakehouse::runtime")

    assertThrows[EngineException] {
      val lambdaString = "foobar->getAll()->filter(x|$x.foo = 'bar')"
      LegendUtils.generateExecutionPlan(lambdaString, mapping, runtime, legend.pureModel)
    }

    val lambdaString = "databricks::employee->getAll()->filter(x|$x.high_fives > 20)"
    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, runtime, legend.pureModel)
    assert(plan.rootExecutionNode.executionNodes.get(0).isInstanceOf[SQLExecutionNode])
  }

  "An execution plan" should "be converted as SQL clause" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))

    val mapping = legend.getMapping("databricks::lakehouse::emp2delta")
    val runtime = legend.getRuntime("databricks::lakehouse::runtime")

    val lambdaString = "databricks::employee->getAll()->filter(x|$x.high_fives > 20)"
    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, runtime, legend.pureModel)
    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
    val sql = LegendUtils.parseSql(sqlPlan)
    assert(sql == "highfives > 20")
  }

  "A more complex function" should "be converted as SQL clause" in {
    val legend = LegendClasspathLoader.loadResources("model")
    assert(legend.getEntityNames.contains("databricks::employee"))

    val mapping = legend.getMapping("databricks::lakehouse::emp2delta")
    val runtime = legend.getRuntime("databricks::lakehouse::runtime")

    val lambdaString = "databricks::employee->getAll()->filter(x|$x.joined_date->dateDiff($x.birth_date, DurationUnit.YEARS) > 20)"
    val plan = LegendUtils.generateExecutionPlan(lambdaString, mapping, runtime, legend.pureModel)
    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
    val sql = LegendUtils.parseSql(sqlPlan)
    assert(sql == "year(joineddate) - year(birthdate) > 20")
  }

}
