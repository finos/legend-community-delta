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

import com.fasterxml.jackson.databind.ObjectMapper
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.{PlainSelect, Select}
import org.apache.spark.sql.types._
import org.finos.legend.engine.language.pure.compiler.toPureGraph.{HelperValueSpecificationBuilder, PureModel}
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser
import org.finos.legend.engine.language.pure.grammar.to.{DEPRECATED_PureGrammarComposerCore, PureGrammarComposerContext}
import org.finos.legend.engine.plan.generation.PlanGenerator
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers
import org.finos.legend.engine.plan.platform.PlanPlatform
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.ValueSpecification
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.Lambda
import org.finos.legend.engine.shared.core.ObjectMapperFactory
import org.finos.legend.pure.generated.core_relational_relational_router_router_extension.Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_
import org.finos.legend.pure.generated.{Root_meta_pure_alloy_connections_RelationalDatabaseConnection_Impl, Root_meta_pure_alloy_connections_alloy_specification_DatabricksDatasourceSpecification_Impl}
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime
import org.finos.legend.sdlc.domain.model.entity.Entity
import org.finos.legend.sdlc.language.pure.compiler.toPureGraph.PureModelBuilder
import org.finos.legend.spark.LegendUtils._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Legend(entities: Map[String, Entity]) {

  lazy val pureModel: PureModel = PureModelBuilder.newBuilder.withEntities(entities.values.asJava).build.getPureModel
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getEntityNames: Seq[String] = entities.keys.toSeq

  def getEntity(entityName: String): Entity = {
    require(entities.contains(entityName), s"could not find entity [$entityName]")
    entities(entityName)
  }

  def getMapping(mappingName: String): Mapping = {
    Try(pureModel.getMapping(mappingName)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException(s"could not load mapping [$mappingName]", e)
    }
  }

  def getRuntime(runtimeName: String): runtime.Runtime = {
    Try(pureModel.getRuntime(runtimeName)) match {
      case Success(value) =>
        val mainConnection = value._connections().asScala.head
        require(mainConnection.isInstanceOf[Root_meta_pure_alloy_connections_RelationalDatabaseConnection_Impl], "connection should be of type relational")
        val relational = mainConnection.asInstanceOf[Root_meta_pure_alloy_connections_RelationalDatabaseConnection_Impl]
        require(relational._datasourceSpecification.isInstanceOf[Root_meta_pure_alloy_connections_alloy_specification_DatabricksDatasourceSpecification_Impl], "connection should be of type [Databricks]")
        value
      case Failure(e) => throw new IllegalArgumentException(s"could not load runtimeName [$runtimeName]", e)
    }
  }

  /**
   * Creating a spark schema in line with Legend specification
   *
   * @param entityName the entity to load schema from, provided as [namespace::entity] format
   * @return the corresponding Spark schema for the provided entity name
   */
  def getEntitySchema(entityName: String): StructType = {
    val entity = getEntity(entityName)
    StructType(getLegendClassStructFields(entity.toLegendClass))
  }

  /**
   * TECHNICAL EXPECTATIONS
   * Programmatically extract all SQL constraints as defined in a legend schema for a given entity
   * We extract schema constraints (e.g. cardinality, nullable) that we convert into spark SQL
   *
   * @param entityName the entity of type [Class] to extract constraints from
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend schema
   */
  def getTechnicalExpectations(entityName: String): Seq[Filter] = {
    val entity = getEntity(entityName)
    getLegendClassExpectations(entity.toLegendClass)
  }

  /**
   * BUSINESS EXPECTATIONS
   * Programmatically generate all SQL constraints as defined in a legend PURE language for a given entity
   * We extract pure language constraints (e.g. |$this.score > 0) that we convert into spark SQL
   * Although technical expectations apply to a legend entities, business expectations can only be applied to the mapped
   * relational model. We generate SQL plan given a mapping and a runtime
   *
   * @param entityName  the entity of type [Class] to extract constraints from
   * @param mappingName the mapping entity used to transform entity onto a table
   * @param runtimeName the runtime to use against that specific mapping
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend definitions
   */
  def getBusinessExpectations(entityName: String, mappingName: String, runtimeName: String): Seq[Filter] = {

    val entity = getEntity(entityName)
    val mapping = getMapping(mappingName)
    val runtime = getRuntime(runtimeName)

    entity.toLegendClass.constraints.asScala.map(c => {

      // We generate code to query table with constraints as a WHERE clause
      val query = s"$entityName->getAll()->filter(this|${c.toLambda})"

      // We generate an execution plan
      val plan = Legend.generateExecutionPlan(query, mapping, runtime, pureModel)

      // We retrieve the SQL where clause
      val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
      Filter(c.name, Legend.parseSql(sqlExecPlan))
    })
  }

  /**
   * Given a legend mapping, we generate all transformations required to persist an entity to a relational table on delta lake
   *
   * @param entityName  the name of the entity to process
   * @param mappingName the name of the mapping to transform entity into a table
   * @param runtimeName the name of the runtime to use for that transformation
   * @return the set of transformations required
   */
  def transform(entityName: String, mappingName: String, runtimeName: String): Transform = {

    val relational = Try(pureModel.getMapping(mappingName)) match {
      case Success(value) => value.getRelationalTransformation
      case Failure(e) => throw new IllegalArgumentException(s"could not load mapping [$mappingName]", e)
    }

    Transform(
      getEntitySchema(entityName),
      getTechnicalExpectations(entityName),
      relational.getTransformations,
      getBusinessExpectations(entityName, mappingName, runtimeName),
      relational.getTable
    )

  }

  /**
   * We retrieve all rules from the Legend schema. This will create SQL expression to validate schema integrity (nullable, multiplicity)
   * as well as allowed values for enumerations
   *
   * @param legendProperty the legend property object to create rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules expressed as SQL expressions. Unchecked yet, we'll test for syntax later
   */
  private def getLegendPropertyExpectations(legendProperty: Property, parentField: String): Seq[Filter] = {

    // the first series of rules are simply derived from the nullability and multiplicity of each field
    val defaultRules: Seq[Filter] = getFieldExpectations(legendProperty, parentField)

    // we need to go through more complex structures, such as nested fields or enumerations
    if (legendProperty.`type`.contains("::")) {

      // Object being referenced externally, we have to ensure this was loaded
      val nestedEntity = getEntity(legendProperty.`type`)
      val nestedColumn = Legend.childFieldName(legendProperty.name, parentField)
      nestedEntity.getContent.get("_type").asInstanceOf[String].toLowerCase() match {

        case "class" =>
          // We need to validate each underlying object if field is not optional
          // We simply recurse the same logic at a child level
          if (legendProperty.isCollection) defaultRules else {
            val nestedRules: Seq[Filter] = getLegendClassExpectations(nestedEntity.toLegendClass, Legend.childFieldName(legendProperty.name, parentField))
            defaultRules ++ nestedRules
          }
        case "enumeration" =>
          // We simply validate field against available enum values
          val values = nestedEntity.toLegendEnumeration.values.asScala.map(_.value)
          val sql = s"$nestedColumn IS NULL OR $nestedColumn IN (${values.map(v => s"'$v'").mkString(", ")})"
          val allowedValues = Filter(s"[${Legend.cleanColumnName(nestedColumn)}] not allowed value", sql)
          defaultRules :+ allowedValues

        case _ => throw new IllegalArgumentException(
          s"nested entities should be [enumeration] or [class], got [${nestedEntity.getContent.get("_type").toString}]")
      }
    } else defaultRules

  }

  /**
   * Given a legend entity of type [Class], we return all its properties as StructField
   *
   * @param clazz the entity to get StructField from
   * @return the list of StructField objects
   */
  private def getLegendClassStructFields(clazz: Class): Seq[StructField] = {
    clazz.superTypes.asScala.flatMap(superType => {
      getLegendClassStructFields(getEntity(superType).toLegendClass)
    }) ++ clazz.properties.asScala.map(getLegendPropertyStructField)
  }

  /**
   * Given a field of an entity, we convert this property as a StructField object
   * If field is enum or class, legend specs must have been loaded as well. These may results in nested field in our spark schema
   *
   * @param property is a legend object of type [Property], capturing all field specifications
   * @return the corresponding StructField, capturing name, datatype, nullable and metadata
   */
  private def getLegendPropertyStructField(property: Property): StructField = {

    // this field is not a primitive, could be an enum or class
    if (property.`type`.contains("::")) {

      // We have to load the corresponding entity
      val nestedEntity = getEntity(property.`type`)

      // Retrieve underlying entity type
      nestedEntity.getContent.get("_type").asInstanceOf[String].toLowerCase() match {

        // Legend Entity is of type class
        case "class" =>

          // If the parent metadata is empty for that property, we can use the one from our nested object (if any)
          val nestedObject: Class = nestedEntity.toLegendClass
          val doc = if (property.getDoc.isEmpty && nestedObject.getDoc.isDefined) nestedObject.getDoc else property.getDoc

          // We retrieve the full schema of that nested object as a StructType
          // We need to capture nested objects recursively through the getEntityStructFields method
          val nestedSchema = StructType(getLegendClassStructFields(nestedObject))
          val dataType = if (property.isCollection) ArrayType(nestedSchema) else nestedSchema
          val metadata = if (doc.isDefined) new MetadataBuilder().putString("comment", doc.get).build() else new MetadataBuilder().build()

          // We define this field as a StructField with nested entity of datatype StructType
          StructField(property.name, dataType, property.isNullable, metadata)

        // Legend Entity is of type enumeration
        case "enumeration" =>

          // If the metadata is empty, we'll use the one from our nested object (if any)
          val nestedObject: Enumeration = nestedEntity.toLegendEnumeration
          val doc = if (property.getDoc.isEmpty && nestedObject.getDoc.isDefined) nestedObject.getDoc else property.getDoc

          // Even though entity is defined externally, it can be considered as type String instead of nested object
          // We do not have to go through each of its allowed value when defining schema (will do so when evaluating constraints)
          val dataType = if (property.isCollection) ArrayType(StringType) else StringType
          val metadata = if (doc.isDefined) new MetadataBuilder().putString("comment", doc.get).build() else new MetadataBuilder().build()

          // We define this field as a StructField of type StringType
          StructField(property.name, dataType, property.isNullable, metadata)

        // Neither an enumeration or a class object
        case _ => throw new IllegalArgumentException(
          s"referenced legend entities should be of type [enumeration] or [class]," +
            s" got [${nestedEntity.getContent.get("_type").toString}]")
      }

    } else {
      // Primitive type, becomes a simple mapping from LegendDataType to SparkDataType
      val dataType = if (property.isCollection) ArrayType(property.convertDataType) else property.convertDataType
      val metadata = if (property.getDoc.isDefined) new MetadataBuilder().putString("comment", property.getDoc.get).build() else new MetadataBuilder().build()
      StructField(property.name, dataType, property.isNullable, metadata)
    }
  }

  /**
   * We retrieve all constraints associated to a Legend entity of type [Class].
   * We find all constraints that are field specific (e.g. should not be null) as well as parsing domain expert constraints
   * expressed as a Pure Lambda function. All constraints are expressed as SQL statements that we further evaluate as a
   * spark expression (syntax check). Invalid rules (whether syntactically invalid - e.g. referencing a wrong field) or
   * illegal (unsupported PURE function) will still be returned as a Try[String] object
   *
   * @param legendClass the legend entity of type [Class]
   * @param parentField empty if top level object, it contains parent field for nested structure
   * @return the list of rules to evaluate dataframe against, as SQL expressions
   */
  private def getLegendClassExpectations(legendClass: Class, parentField: String = ""): Seq[Filter] = {
    legendClass.properties.asScala.flatMap(property => getLegendPropertyExpectations(property, parentField))
  }

  /**
   * The top level rules are the simplest rules to infer. Those are driven by the schema itself, checking for nullable
   * or multiplicity. Each rule has a name and an associated SQL expression. Unchecked yet, we'll test syntax later
   *
   * @param legendProperty the legend property object (i.e. the field) to infer rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules checking for mandatory value and multiplicity
   */
  private def getFieldExpectations(legendProperty: Property, parentField: String): Seq[Filter] = {

    // Ensure we have the right field name if this is a nested entity
    val fieldName = Legend.childFieldName(legendProperty.name, parentField)

    // Checking for non optional fields
    val mandatoryRule: Option[Filter] = if (!legendProperty.isNullable) {
      Some(Filter(s"[${Legend.cleanColumnName(fieldName)}] is mandatory", s"$fieldName IS NOT NULL"))
    } else None: Option[Filter]

    // Checking legend multiplicity if more than 1 value is allowed
    val multiplicityRule: Option[Filter] = if (legendProperty.isCollection) {
      if (legendProperty.multiplicity.isInfinite) {
        val sql = s"$fieldName IS NULL OR SIZE($fieldName) >= ${legendProperty.multiplicity.lowerBound}"
        Some(Filter(s"[${Legend.cleanColumnName(fieldName)}] has invalid size", sql))
      } else {
        val sql = s"$fieldName IS NULL OR SIZE($fieldName) BETWEEN ${legendProperty.multiplicity.lowerBound} AND ${legendProperty.multiplicity.getUpperBound.toInt}"
        Some(Filter(s"[${Legend.cleanColumnName(fieldName)}] has invalid size", sql))
      }
    } else None: Option[Filter]

    // Aggregate both mandatory and multiplicity rules
    Seq(mandatoryRule, multiplicityRule).flatten
  }
}

object Legend {

  lazy val objectMapper: ObjectMapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports
  lazy val grammarComposer: DEPRECATED_PureGrammarComposerCore = DEPRECATED_PureGrammarComposerCore.Builder.newInstance.withRenderStyle(PureGrammarComposerContext.RenderStyle.PRETTY).build

  /**
   * A complexity when dealing with nested fields is to make sure we call a field with a [parent.child] syntax
   *
   * @param fieldName       the name of the field
   * @param parentFieldName the name of the parent field this field is included into (empty for top level object)
   * @return the concatenation of [parent.child] to reference this field
   */
  def childFieldName(fieldName: String, parentFieldName: String): String =
    if (parentFieldName.isEmpty) s"`$fieldName`" else s"$parentFieldName.`$fieldName`"

  /**
   * We need to ensure fields (especially nested) are enclosed with backticks for spark SQL
   *
   * @param name of the field
   * @return cleaned version of the field to query
   */
  def cleanColumnName(name: String): String = name.replaceAll("`", "")

  /**
   * Simple mapping function that converts Legend data type into Spark SQL DataType
   *
   * @return the corresponding DataType for a given legend type
   */
  def convertDataTypeFromString(returnType: String): DataType = {
    returnType match {
      case "String" => StringType
      case "Boolean" => BooleanType
      case "Binary" => BinaryType
      case "Integer" => IntegerType
      case "Number" => LongType
      case "Float" => FloatType
      case "Decimal" => DoubleType
      case "Date" => DateType
      case "StrictDate" => DateType
      case "DateTime" => TimestampType
      case _ =>
        throw new IllegalArgumentException(s"entity of class [${returnType}] is not supported as primitive")
    }
  }

  /**
   * Parse SQL to retrieve WHERE clause and table alias
   * TODO: find a way not to generate the full SQL but only visit the lambda condition
   *
   * @param executionPlan generated SQL plan from legend engine
   * @return the WHERE clause of the generated SQL expression
   */
  def parseSql(executionPlan: SQLExecutionNode): String = {
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(executionPlan.sqlQuery)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val alias = s"${select.getFromItem.getAlias.getName}."
    val where = select.getWhere
    where.toString.replaceAll(alias, "")
  }

  def generateExecutionPlan(query: String, legendMapping: Mapping, legendRuntime: runtime.Runtime, pureModel: PureModel): SingleExecutionPlan = {
    PlanGenerator.generateExecutionPlan(
      Legend.buildLambda(query, pureModel),
      legendMapping,
      legendRuntime,
      null,
      pureModel,
      "vX_X_X",
      PlanPlatform.JAVA,
      "1.0",
      Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_(pureModel.getExecutionSupport),
      LegendPlanTransformers.transformers
    )
  }

  /**
   * Build the value specification for a lambda function
   *
   * @param lambdaString the string representation of the lambda function
   * @param pureModel    the pure model
   * @return the compiled function
   */
  def buildLambda(lambdaString: String, pureModel: PureModel): LambdaFunction[_] = {
    val function = buildLambda(lambdaString)
    val lambda = new Lambda()
    lambda.body = Collections.singletonList(function)
    HelperValueSpecificationBuilder.buildLambda(lambda, pureModel.getContext)
  }

  def buildLambda(lambdaString: String): ValueSpecification = {
    val parser = PureGrammarParser.newInstance()
    val parsed = parser.parseLambda(lambdaString, "id", true)
    require(parsed.body != null && parsed.body.size() > 0)
    parsed.body.get(0)
  }


}

