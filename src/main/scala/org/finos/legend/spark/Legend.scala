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
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime.{Runtime => PureRuntime}
import org.finos.legend.engine.language.pure.compiler.toPureGraph.{HelperValueSpecificationBuilder, PureModel}
import org.finos.legend.engine.language.pure.grammar.to.{DEPRECATED_PureGrammarComposerCore, PureGrammarComposerContext}
import org.finos.legend.engine.plan.generation.PlanGenerator
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers
import org.finos.legend.engine.plan.platform.PlanPlatform
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.DatabaseType
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.AppliedFunction
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.Lambda
import org.finos.legend.engine.shared.core.ObjectMapperFactory
import org.finos.legend.pure.generated.{Root_meta_pure_metamodel_function_LambdaFunction_Impl, Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl, Root_meta_relational_metamodel_relation_Table_Impl, core_relational_relational_router_router_extension}
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime
import org.finos.legend.sdlc.domain.model.entity.Entity
import org.finos.legend.sdlc.language.pure.compiler.toPureGraph.PureModelBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Legend(entities: Map[String, Entity]) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * With namespace fully loaded, we return the list of available legend entities
   *
   * @return the name of all available entities
   */
  def getEntityNames: Set[String] = entities.filter(_._2.isClass).keySet

  /**
   * Creating a spark schema in line with Legend specification
   *
   * @param entityName the entity to load schema from, provided as [namespace::entity] format
   * @return the corresponding Spark schema for the provided entity name
   */
  def getEntitySchema(entityName: String): StructType = {

    // We ensure the specified entity exists in the list of legend entities loaded
    logger.info("Loading Legend schema for entity [{}]", entityName)
    require(entities.contains(entityName), s"Could not find entity [$entityName]")
    val entity: Entity = entities(entityName)

    // Create spark schema as a StructType
    logger.debug("Creating spark schema for entity [{}]", entityName)
    StructType(getLegendClassStructFields(entity.toLegendClass))
  }

  /**
   * Programmatically extract all SQL constraints as defined in a legend schema for a given entity
   * We extract both schema constraints (e.g. cardinality, nullable) as well as Pure specific constraints
   *
   * @param entityName the entity of type [Class] to extract constraints from
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend schema
   */
  def getEntityExpectations(entityName: String): Seq[(String, String)] = {

    // We ensure the specified entity exists in the list of legend entities loaded
    require(entities.contains(entityName), s"Could not find entity [$entityName]")
    val entity = entities(entityName)

    // Retrieve all rules as SQL expressions
    logger.info("Creating expectations for entity [{}]", entityName)
    getLegendClassExpectations(entity.toLegendClass).flatMap(rule => {
      rule.expression match {
        case Failure(e) =>
          logger.warn(s"Error creating rule [${rule.name}], ${e.getMessage}")
          None
        case Success(sql) =>
          Some(rule.name, sql)
      }
    })
  }
//
//  def getMapping(mappingName: String): Mapping = {
//    logger.info(s"Retrieving mapping [$mappingName]")
//    Try(toPure.getMapping(mappingName)) match {
//      case Success(value) => value
//      case Failure(e) => throw new IllegalArgumentException("could not find mapping " + mappingName, e)
//    }
//  }
//
  private def getRelationalMapping(mapping: Mapping): Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl = {
    val transformations = mapping._classMappings().asScala
    require(transformations.nonEmpty)
    require(transformations.head.isInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl])
    val transformation = transformations.head.asInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl]

    require(transformation._mainTableAlias._relationalElement() != null)
    require(transformation._mainTableAlias._relationalElement().isInstanceOf[Root_meta_relational_metamodel_relation_Table_Impl])
    transformation
  }

  def getLambdaConstraints(pure: PureModel, entityName: String, mapping: Mapping, runtime: PureRuntime): Seq[(String, String)] = {

    require(entities.contains(entityName), "could not find entity " + entityName)
    val entity = entities(entityName)

    require(entity.isClass, s"Entity ${entityName} should be of type class")

    entity.toLegendClass.constraints.asScala.map(c => {
      val query = s"${entityName}->getAll()->filter(this|${c.toLambda})"
      val lambda = new Lambda()
      println(query)
      lambda.body = Collections.singletonList(query.toValueSpec)
      val function = HelperValueSpecificationBuilder.buildLambda(lambda, pure.getContext)
      (c.name, function)

      val plan = PlanGenerator.generateExecutionPlan(
        function,
        mapping,
        runtime,
        null,
        pure,
        "vX_X_X",
        PlanPlatform.JAVA,
        null,
        core_relational_relational_router_router_extension.Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_(pure.getExecutionSupport),
        LegendPlanTransformers.transformers
      )

      val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]
      (c.name, parseSql(sqlExecPlan))
    })
  }

  // Parse SQL to retrieve WHERE clause and table alias
  // TODO: find a way not to generate the full SQL but only visit the lambda condition
  def parseSql(executionPlan: SQLExecutionNode): String = {
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(executionPlan.sqlQuery)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val alias = s"${select.getFromItem.getAlias.getName}."
    val where = select.getWhere
    where.toString.replaceAll(alias, "")
  }

  /**
   * Given a legend mapping, we generate all transformations required to persist an entity to a relational table on delta lake
   *
   * @param mappingName the name of the mapping
   * @return the set of transformations required
   */
  def transform(entityName: String, mappingName: String, runtimeName: String): Transform = {

    val pureModel: PureModel = PureModelBuilder.newBuilder.withEntities(entities.values.asJava).build.getPureModel

    val entity = Try(entities(entityName)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("could not find entity " + entityName, e)
    }

    val mapping = Try(pureModel.getMapping(mappingName)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("could not find mapping " + mappingName, e)
    }

    val runtime = Try(pureModel.getRuntime(runtimeName)) match {
      case Success(value) => value
      case Failure(e) => throw new IllegalArgumentException("could not find runtimeName " + runtimeName, e)
    }

    val constraints = getLambdaConstraints(pureModel, entityName, mapping, runtime)
    val relational = getRelationalMapping(mapping)

    Transform(
      entityName,
      getEntitySchema(entityName),
      getEntityExpectations(entityName),
      relational.getTransformations,
      constraints,
      relational.getDstTable
    )

  }

  /**
   * Given a legend entity of type [Class], we return all its properties as StructField
   *
   * @param clazz the entity to get StructField from
   * @return the list of StructField objects
   */
  private def getLegendClassStructFields(clazz: Class): Seq[StructField] = {
    clazz.superTypes.asScala.flatMap(superType => {
      getLegendClassStructFields(entities(superType).toLegendClass)
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
      logger.debug("Loading definition for subclass [{}]", property.`type`)
      require(entities.contains(property.`type`), s"Cannot find declaration of nested object [${property.`type`}]")
      val nestedEntity = entities(property.`type`)

      // Retrieve underlying entity type
      nestedEntity.getContent.get("_type").asInstanceOf[String].toLowerCase() match {

        // Legend Entity is of type class
        case "class" =>

          // If the parent metadata is empty for that property, we can use the one from our nested object (if any)
          val nestedObject: Class = nestedEntity.toLegendClass
          logger.debug(s"Processing nested structure for field [${property.name}] of type [${nestedObject.name}]")
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
          logger.debug(s"Processing enumeration for field [${property.name}] of type [${nestedObject.name}]")
          val doc = if (property.getDoc.isEmpty && nestedObject.getDoc.isDefined) nestedObject.getDoc else property.getDoc

          // Even though entity is defined externally, it can be considered as type String instead of nested object
          // We do not have to go through each of its allowed value when defining schema (will do so when evaluating constraints)
          val dataType = if (property.isCollection) ArrayType(StringType) else StringType
          val metadata = if (doc.isDefined) new MetadataBuilder().putString("comment", doc.get).build() else new MetadataBuilder().build()

          // We define this field as a StructField of type StringType
          StructField(property.name, dataType, property.isNullable, metadata)

        // Neither an enumeration or a class object
        case _ => throw new IllegalArgumentException(
          s"Referenced legend entities should be of type [enumeration] or [class]," +
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
  private def getLegendClassExpectations(legendClass: Class, parentField: String = ""): Seq[Expectation] = {

    // Infer rules from the legend schema itself
    logger.debug(s"Converting schema specific constraints for class [${legendClass.name}]")
    val schemaConstraints = legendClass.properties.asScala.flatMap(property => getLegendPropertyExpectations(property, parentField))

    // Check that rules are syntactically valid SQL expression
    logger.debug(s"Validating SQL expression syntax for class [${legendClass.name}]")
    //    (pureConstraints ++ schemaConstraints).map(_.build)
    schemaConstraints.map(_.build)
  }

  /**
   * We retrieve all rules from the Legend schema. This will create SQL expression to validate schema integrity (nullable, multiplicity)
   * as well as allowed values for enumerations
   *
   * @param legendProperty the legend property object to create rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules expressed as SQL expressions. Unchecked yet, we'll test for syntax later
   */
  private def getLegendPropertyExpectations(legendProperty: Property, parentField: String): Seq[Expectation] = {

    // the first series of rules are simply derived from the nullability and multiplicity of each field
    val defaultRules: Seq[Expectation] = getFieldExpectations(legendProperty, parentField)

    // we need to go through more complex structures, such as nested fields or enumerations
    if (legendProperty.`type`.contains("::")) {

      // Object being referenced externally, we have to ensure this was loaded
      require(entities.contains(legendProperty.`type`), s"Cannot find declaration of nested object [${legendProperty.`type`}]")
      val nestedEntity = entities(legendProperty.`type`)
      val nestedColumn = childFieldName(legendProperty.name, parentField)
      nestedEntity.getContent.get("_type").asInstanceOf[String].toLowerCase() match {

        case "class" =>
          // We need to validate each underlying object if field is not optional
          // We simply recurse the same logic at a child level
          // FIXME: find a way to validate each underlying object of a list
          if (legendProperty.isCollection) defaultRules else {
            val nestedRules = getLegendClassExpectations(nestedEntity.toLegendClass, childFieldName(legendProperty.name, parentField))
            defaultRules ++ nestedRules
          }
        case "enumeration" =>
          // We simply validate field against available enum values
          val values = nestedEntity.toLegendEnumeration.values.asScala.map(_.value)
          val sql = s"$nestedColumn IN (${values.map(v => s"'$v'").mkString(", ")})"
          val allowedValues = Expectation(Seq(nestedColumn), s"[${cleanColumnName(nestedColumn)}] not allowed value", None, Try(sql))
          defaultRules :+ allowedValues

        case _ => throw new IllegalArgumentException(
          s"Nested entities should be [enumeration] or [class], got [${nestedEntity.getContent.get("_type").toString}]")
      }
    } else defaultRules

  }

  /**
   * The top level rules are the simplest rules to infer. Those are driven by the schema itself, checking for nullable
   * or multiplicity. Each rule has a name and an associated SQL expression. Unchecked yet, we'll test syntax later
   *
   * @param legendProperty the legend property object (i.e. the field) to infer rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules checking for mandatory value and multiplicity
   */
  private def getFieldExpectations(legendProperty: Property, parentField: String): Seq[Expectation] = {

    // Ensure we have the right field name if this is a nested entity
    val fieldName = childFieldName(legendProperty.name, parentField)

    // Checking for non optional fields
    val mandatoryRule: Option[Expectation] = if (!legendProperty.isNullable) {
      Some(Expectation(Seq.empty[String], s"[${cleanColumnName(fieldName)}] is mandatory", None, Try(s"$fieldName IS NOT NULL")))
    } else None: Option[Expectation]

    // Checking legend multiplicity if more than 1 value is allowed
    val multiplicityRule: Option[Expectation] = if (legendProperty.isCollection) {
      if (legendProperty.multiplicity.isInfinite) {
        val sql = s"SIZE($fieldName) >= ${legendProperty.multiplicity.lowerBound}"
        Some(Expectation(Seq(fieldName), s"[${cleanColumnName(fieldName)}] has invalid size", None, Try(sql)))
      } else {
        val sql = s"SIZE($fieldName) BETWEEN ${legendProperty.multiplicity.lowerBound} AND ${legendProperty.multiplicity.getUpperBound.toInt}"
        Some(Expectation(Seq(fieldName), s"[${cleanColumnName(fieldName)}] has invalid size", None, Try(sql)))
      }
    } else None: Option[Expectation]

    // Aggregate both mandatory and multiplicity rules
    Seq(mandatoryRule, multiplicityRule).flatten
  }
}

object Legend {
  lazy val objectMapper: ObjectMapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports
  lazy val grammarComposer: DEPRECATED_PureGrammarComposerCore = DEPRECATED_PureGrammarComposerCore.Builder.newInstance.withRenderStyle(PureGrammarComposerContext.RenderStyle.PRETTY).build
}

