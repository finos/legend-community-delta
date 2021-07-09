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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.finos.legend.engine.language.pure.grammar.to.{DEPRECATED_PureGrammarComposerCore, PureGrammarComposerContext}
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.AppliedFunction
import org.finos.legend.engine.shared.core.ObjectMapperFactory
import org.finos.legend.sdlc.domain.model.entity.Entity
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class Legend(entities: Map[String, Entity]) {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Return a legend entity given a namespace::object path
   * @param entityName the entity name to retrieve
   * @return the legend entity
   */
  def getEntity(entityName: String): Entity = {
    require(entities.contains(entityName), s"Could not find entity [$entityName]")
    entities(entityName)
  }

  /**
   * With namespace fully loaded, we return the list of available legend entities
   * @return the name of all available entities
   */
  def getEntityNames: Set[String] = entities.filter(_._2.isClass).keySet

  /**
   * Creating a spark schema in line with Legend specification
   * @param entityName the entity to load schema from, provided as [namespace::entity] format
   * @return the corresponding Spark schema for the provided entity name
   */
  def getEntitySchema(entityName: String): StructType = {

    // We ensure the specified entity exists in the list of legend entities loaded
    logger.info("Loading Legend schema for entity [{}]", entityName)
    require(entities.contains(entityName), s"Could not find entity [$entityName]")
    val entity = entities(entityName)

    // Create spark schema as a StructType
    logger.debug("Creating spark schema for entity [{}]", entityName)
    StructType(getLegendClassStructFields(entity.toLegendClass))
  }

  /**
   * Programmatically extract all SQL constraints as defined in a legend schema for a given entity
   * We extract both schema constraints (e.g. cardinality, nullable) as well as Pure specific constraints
   * @param entityName the entity of type [Class] to extract constraints from
   * @param validate whether or not we want to evaluate rules on an empty dataframe (requires an active spark session)
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend schema
   */
  def getEntityExpectations(entityName: String, validate: Boolean = true): Seq[Expectation] = {

    // We ensure the specified entity exists in the list of legend entities loaded
    logger.info("Creating expectations for entity [{}]", entityName)
    val schema = getEntitySchema(entityName)
    val entity = entities(entityName)

    // Retrieve all rules as SQL expressions
    logger.debug("Creating SQL expectations from legend schema for entity [{}]", entityName)
    val expectations = getLegendClassExpectations(entity.toLegendClass).map(rule => {
      rule.expression match {
        case Failure(e) => logger.warn(s"Error creating rule [${rule.name}], ${e.getMessage}")
        case Success(_) =>
      }
      rule
    })

    if(!validate) return expectations

    // Validate rules are schema compatible by evaluating against an empty dataframe
    // Requires an active spark session (hence optional, default = true)
    logger.debug("Evaluating {} expectation(s) against an empty dataframe", expectations.size)
    val spark = SparkSession.getActiveSession
    require(spark.isDefined, "A spark session should be active to validate rules")

    // Creating a dummy dataframe of valid schema
    val df = spark.get.createDataFrame(spark.get.sparkContext.emptyRDD[Row], schema)
    expectations.map(rule => {
      if (rule.expression.isFailure) rule else {
        Try(df.withColumn(rule.name, expr(rule.expression.get))) match {
          case Success(_) => rule // valid SQL rule
          case Failure(e) =>
            logger.warn(s"Error evaluating rule [${rule.name}], ${e.getMessage}")
            rule.copy(expression = Failure(e)) // invalid rule
        }
      }
    })
  }

  /**
   * Programmatically derive all SQL expression as defined in a legend derivations for a given entity
   * We retrieve the name of the field to derive from a row as well as the PURE function expressed as SQL
   * @param entityName the entity of type [Class] to extract derivations from
   * @param validate whether or not we want to evaluate expression on an empty dataframe (requires an active spark session)
   * @return the list of derivation as fieldName + fieldSQL code to apply to our dataframe
   */
  def getEntityDerivations(entityName: String, validate: Boolean = true): Seq[Derivation] = {

    // We ensure the specified entity exists in the list of legend entities loaded
    logger.info("Creating derivations for entity [{}]", entityName)
    val schema = getEntitySchema(entityName)
    val entity = entities(entityName)

    val entityClass = entity.toLegendClass
    val derivations = entityClass.qualifiedProperties.asScala.filter(p => p.body != null && !p.body.isEmpty).map(property => {
      val returnType = if(property.returnMultiplicity.isInfinite || property.returnMultiplicity.getUpperBound > 1) {
        ArrayType(convertDataTypeFromString(property.returnType))
      } else {
        convertDataTypeFromString(property.returnType)
      }
      val expression = Try(property.body.get(0).convertToSQL())
      val derivation = Derivation(s"`${property.name}`", returnType, expression, property.getDoc)
      derivation.build
    })

    if(!validate) return derivations

    // Validate rules are schema compatible by evaluating against an empty dataframe
    // Requires an active spark session (hence optional, default = true)
    logger.debug("Evaluating {} derivation(s) against an empty dataframe", derivations.size)
    val spark = SparkSession.getActiveSession
    require(spark.isDefined, "A spark session should be active to validate columns")

    // Creating a dummy dataframe of valid schema
    val df = spark.get.createDataFrame(spark.get.sparkContext.emptyRDD[Row], schema)
    derivations.map(rule => {
      if (rule.expression.isFailure) rule else {
        Try(df.withColumn(rule.fieldName, expr(rule.expression.get))) match {
          case Success(_) => rule // valid SQL expression
          case Failure(e) =>
            logger.warn(s"Error evaluating derivation [${rule.fieldName}], ${e.getMessage}")
            rule.copy(expression = Failure(e)) // invalid derivation
        }
      }
    })

  }

  /**
   * As we can programmatically infer Spark schema from a legend object of type [Class], we can detect schema compatibility
   * with an existing dataframe. We return eventual drift as a case class object that includes SQL statements one may have to
   * run to update an existing table against an updated schema.
   *
   * We capture new columns as well as description change (metadata)
   * Note that incompatible changes such as DELETE or ALTER DATATYPE will be flagged with isCompatible boolean
   *
   * @param entityName the name of the legend entity (as namespace::entity) to load Spark schema from
   * @param prevSchema the schema of a dataframe to validate new schema against
   * @return the list of SQL statements to update table with
   */
  def detectEntitySchemaDrift(entityName: String, prevSchema: StructType): SchemaDrift = {

    // Retrieve new schema from our legend entity
    logger.info("Detecting drift for entity [{}]", entityName)
    val newSchema = getEntitySchema(entityName)
    val oldSchemaMap = prevSchema.map(f => (f.name, f)).toMap

    // detect new columns
    val newFields = newSchema.filter(f => !oldSchemaMap.contains(f.name)).map(s => s"ADD COLUMNS (${s.toDDL})")
    newFields.foreach(s => logger.warn("New column detected! [{}]", s))

    // detect metadata changes
    val newSoftChanges = newSchema.filter(f => oldSchemaMap.contains(f.name)).filter(f => {
      oldSchemaMap(f.name).getComment() != f.getComment() && f.getComment().isDefined && oldSchemaMap(f.name).dataType == f.dataType
    }).map(f => s"ALTER COLUMN `${f.name}` COMMENT '${f.getComment().getOrElse("''").replaceAll("'", "\\'")}'")
    newFields.foreach(s => logger.debug("New metadata detected! [{}]", s))

    // Deletion is not permitted on delta
    val isDeleted = prevSchema.exists(f => {
      val missing = !newSchema.fields.map(_.name).contains(f.name)
      if(missing) logger.error("Column [{}] missing, incompatible schema", f.name)
      missing
    })

    // Changing datatype is not permitted, we need to flag schema incompatibility
    val isChanged = newSchema.exists(f => {
      val changed = oldSchemaMap.contains(f.name) && oldSchemaMap(f.name).dataType != f.dataType
      if(changed) logger.error("Column [{}] changed from [{}] to [{}], incompatible schema", f.name, oldSchemaMap(f.name).dataType, f.dataType)
      changed
    })

    // Return Drift object with SQL ALTER statements (if any) and backwards compatibility flag
    SchemaDrift(newFields ++ newSoftChanges, !isChanged && !isDeleted)
  }

  /**
   * Given a legend entity of type [Class], we return all its properties as StructField
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
   * @param legendClass the legend entity of type [Class]
   * @param parentField empty if top level object, it contains parent field for nested structure
   * @return the list of rules to evaluate dataframe against, as SQL expressions
   */
  private def getLegendClassExpectations(legendClass: Class, parentField: String = ""): Seq[Expectation] = {

    // Retrieve and evaluate all PURE domain expert constraints
    logger.debug(s"Converting user defined PURE constraints for class [${legendClass.name}]")
    val pureConstraints = legendClass.constraints.asScala.map(c => getLegendConstraintExpectations(c, parentField))

    // Infer rules from the legend schema itself
    logger.debug(s"Converting schema specific constraints for class [${legendClass.name}]")
    val schemaConstraints = legendClass.properties.asScala.flatMap(property => getLegendPropertyExpectations(property, parentField))

    // Check that rules are syntactically valid SQL expression
    logger.debug(s"Validating SQL expression syntax for class [${legendClass.name}]")
    (pureConstraints ++ schemaConstraints).map(_.build)
  }

  /**
   * We retrieve all rules from the Legend schema. This will create SQL expression to validate schema integrity (nullable, multiplicity)
   * as well as allowed values for enumerations
   * @param legendProperty the legend property object to create rules from
   * @param parentField empty if top level object, it contains parent field for nested structure
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
          if(legendProperty.isCollection) defaultRules else {
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
   * @param legendProperty the legend property object (i.e. the field) to infer rules from
   * @param parentField empty if top level object, it contains parent field for nested structure
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

  /**
   * The most complex part of this project, converting Legend user defined constraints from PURE language to SQL
   * When specified, these rules are expressed as JSON, converted into PURE and eventually converted as SQL expression
   * Since we cannot guarantee 1:1 compatibility from PURE to SQL, we ensure each rule is captured as a Try[String] to
   * not fail for legend incompatibility, only evaluating compatible rules
   * @param constraint the legend constraint as returned from the Legend entity json specs
   * @param parentField empty if top level object, we have to ensure we call [parent.child] for nested entities
   * @return the list of PURE constraints that could have been converted as SQL expression
   */
  private def getLegendConstraintExpectations(constraint: Constraint, parentField: String): Expectation = {
    require(constraint.functionDefinition != null && constraint.functionDefinition.body != null, s"Constraint should have functionDefinition")
    val body = constraint.functionDefinition.body.get(0)
    require(body.isInstanceOf[AppliedFunction], s"Constraint should be expressed as [AppliedFunction], got [${body.getClass.toString}]")
    val allFields = body.getFieldItAppliesTo(parentField).toList
    Try(body.convertToSQL(parentField)) match {
      case Success(sql) => Expectation(allFields, constraint.name, Some(constraint.toLambda), Success(sql))
      case Failure(e) => Expectation(allFields, constraint.name, Some(constraint.toLambda), Failure(e))
    }
  }

}

object Legend {
  lazy val objectMapper: ObjectMapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports
  lazy val grammarComposer: DEPRECATED_PureGrammarComposerCore = DEPRECATED_PureGrammarComposerCore.Builder.newInstance.withRenderStyle(PureGrammarComposerContext.RenderStyle.PRETTY).build
}

