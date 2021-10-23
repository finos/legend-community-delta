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

import java.io.StringReader
import java.util.Collections

import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.{PlainSelect, Select}
import org.finos.legend.engine.language.pure.compiler.toPureGraph.{CompileContext, HelperValueSpecificationBuilder}
import org.finos.legend.engine.plan.generation.PlanGenerator
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers
import org.finos.legend.engine.plan.platform.PlanPlatform
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.DatabaseType
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.Lambda
import org.finos.legend.engine.shared.core.operational.errorManagement.EngineException
import org.finos.legend.pure.generated.core_relational_relational_router_router_extension
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

class PureModelTest extends FunctionTest {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  "Pure model" should "be generated from SDLC entities" in {
    val legend = LegendClasspathLoader.loadResources("model")
    val pureModel = legend.toPure

    assertThrows[EngineException]{
      pureModel.getMapping("foo:bar")
    }

    val mapping = pureModel.getMapping("databricks::lakehouse::employee")
    assert(mapping._name() == "employee")

    val runtime = pureModel.getRuntime("databricks::lakehouse::runtime")
    assert(runtime._connections().asScala.nonEmpty)

  }

  def lambdaSelect(ctx: CompileContext, entity: String, filter: String = ""): LambdaFunction[_] = {
    val lambdaString = if (filter.nonEmpty) {
      s"|$entity->getAll()->filter($filter)"
    } else {
      s"|$entity->getAll()"
    }
    logger.info(s"Generating lambda $lambdaString")
    val lambda = new Lambda()
    lambda.body = Collections.singletonList(lambdaString.toValueSpec)
    HelperValueSpecificationBuilder.buildLambda(lambda, ctx)
  }

  "A SQL plan" should "be generated for [databricks::employee]" in {
    val legend = LegendFileLoader.loadResources("src/test/resources/model")
    val pureModel = legend.toPure
    val mapping = pureModel.getMapping("databricks::lakehouse::employee")
    val runtime = pureModel.getRuntime("databricks::lakehouse::runtime")
    val lambdas = lambdaSelect(pureModel.getContext, "databricks::employee")

    val plan = PlanGenerator.generateExecutionPlan(
      lambdas,
      mapping,
      runtime,
      null,
      pureModel,
      "vX_X_X",
      PlanPlatform.JAVA,
      null,
      core_relational_relational_router_router_extension.Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_(pureModel.getExecutionSupport),
      LegendPlanTransformers.transformers
    )

    // Retrieve database type
    val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
    assert(DatabaseType.Databricks == sqlExecPlan.connection.`type`)

    // Retrieve SQL statement
    val sql = sqlExecPlan.sqlQuery
    assert("select `root`.id as `pk_0`, `root`.id as `id`, `root`.first_name as `first_name`, `root`.last_name as `last_name`, `root`.birth_date as `birth_date`, `root`.sme as `sme`, `root`.joined_date as `joined_date`, `root`.high_fives as `high_fives` from legend.employee as `root`" == sql)

    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(sql)).asInstanceOf[Select]
    println(select.getSelectBody)

  }

  it should "generate where clause for constraints" in {
    val legend = LegendFileLoader.loadResources("src/test/resources/model")
    val pureModel = legend.toPure
    val mapping = pureModel.getMapping("databricks::lakehouse::employee")
    val runtime = pureModel.getRuntime("databricks::lakehouse::runtime")


    val filters = "x|$x.first_name->startsWith('A') && $x.last_name->endsWith('D')"
    val lambdas = lambdaSelect(pureModel.getContext, "databricks::employee", filters)

    val plan = PlanGenerator.generateExecutionPlan(
      lambdas,
      mapping,
      runtime,
      null,
      pureModel,
      "vX_X_X",
      PlanPlatform.JAVA,
      null,
      core_relational_relational_router_router_extension.Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_(pureModel.getExecutionSupport),
      LegendPlanTransformers.transformers
    )

    // Retrieve database type
    val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
    assert(DatabaseType.Databricks == sqlExecPlan.connection.`type`)

    // Retrieve SQL statement
    val sql = sqlExecPlan.sqlQuery

    // Parse SQL to retrieve WHERE clause and table alias
    // TODO: find a way not to generate the full SQL but only visit the lambda condition
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(sql)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val alias = s"${select.getFromItem.getAlias.getName}."
    val where = select.getWhere
    val expression = where.toString.replaceAll(alias, "")
    assert(expression == "(first_name LIKE 'A%' AND last_name LIKE '%D')")
    logger.info(expression)
  }

}
