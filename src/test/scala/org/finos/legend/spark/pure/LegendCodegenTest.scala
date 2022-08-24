/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 Databricks - see NOTICE.md file
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

package org.finos.legend.spark.pure

import org.apache.spark.sql.types._
import org.finos.legend.engine.language.pure.compiler.Compiler
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.spark.{Legend, LegendUtils}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID

class LegendCodegenTest extends AnyFlatSpec {

  def metadata(comment: String): Metadata = {
    new MetadataBuilder().putString("comment", comment).build()
  }

  "A namespace" should "be valid" in {
    assert("antoine::amend".isValidNamespace)
    assert(!" antoine::amend".isValidNamespace)
    assert(!"antoine.amend".isValidNamespace)
    assert(!"antoine::amend::".isValidNamespace)
    assert("antoine::amend::legend".isValidNamespace)
  }

  "Capitalizing field" should "work" in {
    assert("hello_world".camelCaseEntity == "HelloWorld")
    assert("helloWorld".camelCaseEntity == "HelloWorld")
    assert("helloworld".camelCaseEntity == "Helloworld")
    assert("hello_world".camelCaseField == "helloWorld")
    assert("helloWorld".camelCaseField == "helloWorld")
    assert("helloworld".camelCaseField == "helloworld")
  }

  "A nested spark schema" should "be converted as PURE" in {

    val child = StructType(List(
      StructField("nickname", StringType, nullable = true, metadata("Kids nickname")),
      StructField("age", IntegerType, nullable = true, metadata("Kids age"))
    ))

    val parent = StructType(List(
      StructField("first_name", StringType, nullable = true),
      StructField("last_name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true, metadata("Parent age")),
      StructField("children", ArrayType(child), nullable = true, metadata("All of their children"))
    ))

    val observed = LegendCodegen.generatePureFromSchema(
      "org::finos::legend::delta",
      "family",
      "parent",
      parent
    )

    val expected = """###Pure
                     |Class org::finos::legend::delta::classes::ParentChildren
                     |{
                     |  {meta::pure::profiles::doc.doc = 'Kids nickname'} nickname: String[0..1];
                     |  {meta::pure::profiles::doc.doc = 'Kids age'} age: Integer[0..1];
                     |}
                     |
                     |Class org::finos::legend::delta::classes::ParentBase
                     |{
                     |  firstName: String[0..1];
                     |  lastName: String[0..1];
                     |  {meta::pure::profiles::doc.doc = 'Parent age'} age: Integer[0..1];
                     |}
                     |
                     |Class org::finos::legend::delta::classes::Parent extends org::finos::legend::delta::classes::ParentBase
                     |{
                     |  {meta::pure::profiles::doc.doc = 'All of their children'} children: org::finos::legend::delta::classes::ParentChildren[0..*];
                     |}
                     |
                     |Class org::finos::legend::delta::classes::ParentSerializable extends org::finos::legend::delta::classes::ParentBase
                     |{
                     |  {meta::pure::profiles::doc.doc = 'JSON wrapper for nested property [children]'} children: String[0..*];
                     |}
                     |
                     |###Mapping
                     |Mapping org::finos::legend::delta::mapping::Parent
                     |(
                     |  *org::finos::legend::delta::classes::ParentSerializable: Relational
                     |  {
                     |    ~primaryKey
                     |    (
                     |      [org::finos::legend::delta::Store]family.parent.first_name,
                     |      [org::finos::legend::delta::Store]family.parent.last_name,
                     |      [org::finos::legend::delta::Store]family.parent.age,
                     |      [org::finos::legend::delta::Store]family.parent.children
                     |    )
                     |    ~mainTable [org::finos::legend::delta::Store]family.parent
                     |    firstName: [org::finos::legend::delta::Store]family.parent.first_name,
                     |    lastName: [org::finos::legend::delta::Store]family.parent.last_name,
                     |    age: [org::finos::legend::delta::Store]family.parent.age,
                     |    children: [org::finos::legend::delta::Store]family.parent.children
                     |  }
                     |)
                     |
                     |###Relational
                     |Database org::finos::legend::delta::Store
                     |(
                     |  Schema family
                     |  (
                     |    Table parent
                     |    (
                     |      first_name VARCHAR(2147483647),
                     |      last_name VARCHAR(2147483647),
                     |      age INTEGER,
                     |      children VARCHAR(2147483647)
                     |    )
                     |  )
                     |)
                     |""".stripMargin
    assert(observed == expected)
  }

  "Generated PURE model" should "be correctly interpreted" in {

    val schema = StructType(List(
      StructField("group_id", StringType, nullable = false),
      StructField("artifact_id", StringType, nullable = false),
      StructField("version", StringType, nullable = false),
    ))

    val namespace = "org::finos::legend"
    val pureModelString = LegendCodegen.generatePureFromSchema(
      namespace,
      "maven",
      "project",
      schema
    )

    val contextData = PureGrammarParser.newInstance.parseModel(pureModelString)
    val runtime = Legend.buildRuntime(UUID.randomUUID().toString)
    val pureModel = Compiler.compile(contextData, null, null)
    val mapping = pureModel.getMapping(s"$namespace::mapping::Project")
    val queryBase = s"$namespace::classes::Project.all()->project"
    val query = queryBase + "([x|$x.groupId,x|$x.artifactId,x|$x.version],['groupId','artifactId','version'])"
    val plan = LegendUtils.generateExecutionPlan(query, mapping, runtime, pureModel)
    val sqlPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
    val observed = sqlPlan.sqlQuery
    val expected =
      """select
        |`root`.group_id as `groupId`,
        |`root`.artifact_id as `artifactId`,
        |`root`.version as `version`
        |from maven.project as `root`""".stripMargin.replaceAll("\n", " ")
    assert(observed == expected)
  }

}
