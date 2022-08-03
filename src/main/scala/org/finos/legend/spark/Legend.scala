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

import java.util.UUID
import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.finos.legend.engine.language.pure.compiler.Compiler
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser
import org.finos.legend.engine.language.pure.grammar.to.DEPRECATED_PureGrammarComposerCore
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.engine.shared.core.ObjectMapperFactory
import org.finos.legend.engine.shared.core.api.grammar.RenderStyle
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.{Mapping => LegendMapping}
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime.{Runtime => LegendRuntime}
import org.finos.legend.sdlc.domain.model.entity.Entity
import org.finos.legend.sdlc.language.pure.compiler.toPureGraph.PureModelBuilder
import org.finos.legend.spark.LegendUtils._


import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.PureSingleExecution
import org.slf4j.LoggerFactory
import org.json4s.jackson.Json
import org.json4s.DefaultFormats

class Legend(entities: Map[String, Entity]) {

  lazy val pureModelBuilder: PureModelBuilder.PureModelWithContextData = PureModelBuilder.newBuilder.withEntities(entities.values.asJava).build
  lazy val pureModel: PureModel = pureModelBuilder.getPureModel
  lazy val pureModelContext: PureModelContextData = pureModelBuilder.getPureModelContextData
  lazy val pureRuntime: LegendRuntime = Legend.buildRuntime(UUID.randomUUID().toString)

  final val LOGGER = LoggerFactory.getLogger(this.getClass)

  Logger.getLogger("Alloy Execution Server").setLevel(Level.OFF)

  def getSchema(entityName: String): StructType = {
    val entity = getEntity(entityName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "class" => getEntitySchema(entityName)
      case "mapping" => getMappingSchema(entityName)
      case _ => throw new IllegalArgumentException(s"Only supporting classes and mapping, got $entityType")
    }
  }

  def getTransformations(mappingName: String): Map[String, String] = {
    val entity = getEntity(mappingName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "mapping" => getMappingTransformations(mappingName)
      case _ => throw new IllegalArgumentException(s"Only supporting mapping, got $entityType")
    }
  }

  // Pyspark wrapper, not supporting HashMap
  def getTransformationsJson(mappingName: String): String = {
    Json(DefaultFormats).write(getTransformations(mappingName))
  }

  def getExpectations(entityName: String): Map[String, Try[String]] = {
    val entity = getEntity(entityName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "class" => getEntityExpectations(entityName)
      case "mapping" => getMappingExpectations(entityName)
      case _ => throw new IllegalArgumentException(s"Only supporting classes and mapping, got $entityType")
    }
  }

  // Pyspark wrapper, not supporting HashMap
  def getExpectationsJson(entityName: String): String = {
    Json(DefaultFormats).write(getExpectations(entityName).filter(_._2.isSuccess).map(i => (i._1, i._2.get)))
  }

  def getDerivations(entityName: String): Map[String, String] = {
    val entity = getEntity(entityName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "mapping" =>
        val mapping = getMapping(entityName)
        val entity = getEntity(mapping.getEntityName)
        getEntityDerivations(entity, mapping).toMap
      case _ => throw new IllegalArgumentException(s"Only supporting mapping, got $entityType")
    }
  }

  // Pyspark wrapper, not supporting HashMap
  def getDerivationsJson(mappingName: String): String = {
    Json(DefaultFormats).write(getDerivations(mappingName))
  }

  def query(entityName: String): DataFrame = {
    assert(SparkSession.getActiveSession.isDefined, "A spark session should be defined")
    SparkSession.active.sql(generateSql(entityName))
  }

  def generateSql(entityName: String): String = {
    val entity = getEntity(entityName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "mapping" =>

        // Retrieve all entities required to read high quality data
        val mapping = getMapping(entityName)
        val expectations = getExpectations(entityName)
        val transformations = getTransformations(entityName).keys.toSeq
        val derivations = getEntity(mapping.getEntityName).toLegendClass.qualifiedProperties.asScala.map(_.name)
        val keys = (transformations ++ derivations).map(k => "x|$x." + k).mkString(",")
        val values = (transformations ++ derivations).map(k => s"'$k'").mkString(",")
        val lambda = s"${mapping.getEntityName}.all()->project([$keys],[$values])"
        val plan = LegendUtils.generateExecutionPlan(lambda, mapping, pureRuntime, pureModel)
        val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode].sqlQuery

        // applying valid constraints
        expectations.filter(_._2.isSuccess).toList.zipWithIndex.foldLeft(sqlExecPlan)((sql, c) => {
          if (c._2 == 0) {
            s"$sql WHERE ${c._1._2.get}"
          } else {
            s"$sql AND ${c._1._2.get}"
          }
        })

      case "service" =>
        val service = getEntity(entityName).toLegendService
        service.execution match {
          case execution: PureSingleExecution =>
            val mapping = getMapping(execution.mapping)
            val lambda = execution.func.toLambda
            val plan = LegendUtils.generateExecutionPlan(lambda, mapping, pureRuntime, pureModel)
            val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode].sqlQuery
            //TODO: find a way to apply SQL constraints
            // An option could be to create a view first or a with clause
            // Another option would be to manipulate pure entity and attach lambda definition prior to compilation
            sqlExecPlan
          case _ => throw new IllegalAccessException(s"Service $entityName should have a single execution, got ${service.execution.getClass}")
        }
      case _ => throw new IllegalArgumentException(s"Only supporting mapping or service, got $entityType")
    }
  }

  def getTable(mappingName: String): String = {
    val entity = getEntity(mappingName)
    val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
    entityType match {
      case "mapping" => getMappingTable(mappingName)
      case _ => throw new IllegalArgumentException(s"Only supporting mapping, got $entityType")
    }
  }

  def createTable(mappingName: String, path: String): String = {
    createTable(mappingName, Some(path))
  }

  def createTable(mappingName: String): String = {
    createTable(mappingName, None: Option[String])
  }

  def createTable(mappingName: String, path: Option[String] = None): String = {

    val mapping = getMapping(mappingName)
    val tableName = mapping.getRelationalTransformation.getMappingTable
    val entityName = mapping.getEntityName

    require(SparkSession.getActiveSession.isDefined, "a spark session must be active")
    val spark = SparkSession.active

    val srcSchema = getMappingSchema(mappingName)
    val transformations = getMappingTransformations(mappingName)

    LOGGER.info(s"Generating output schema for legend mapping [$mappingName]")
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], srcSchema)
    val dstSchema = transformations.foldLeft(df)((d, t) => d.withColumnRenamed(t._1, t._2)).schema

    // We do not want to enforced nullable constraints at write
    val schema = StructType(dstSchema.fields.map(_.copy(nullable = true)))

    LOGGER.info(s"Creating delta table for legend table [$tableName]")
    val dt = DeltaTable
      .createIfNotExists()
      .tableName(tableName)
      .comment(s"<Auto Generated> by Legend-Delta from PURE entity [$entityName]")
      .addColumns(schema)

    if(path.isDefined) {
      dt.location(path.get).execute()
    } else {
      dt.execute()
    }

    tableName
  }

  /**
   * @return all entities extracted from the supplied PURE model
   */
  def getEntityNames: Set[String] = entities.keys.toSet

  /**
   * Retrieve entity from the supplied PURE model
   *
   * @param entityName the entity name (fully qualified name) to retrieve
   * @return the legend entity object
   */
  def getEntity(entityName: String): Entity = {
    LOGGER.info(s"Retrieving legend entity [$entityName]")
    require(entities.contains(entityName), s"could not find entity [$entityName]")
    entities(entityName)
  }

  /**
   * Creating a spark schema in line with Legend specification
   *
   * @param entityName the entity to load schema from, provided as [namespace::entity] format
   * @return the corresponding Spark schema for the provided entity name
   */
  def getEntitySchema(entityName: String): StructType = {
    LOGGER.info(s"Retrieving schema for legend class [$entityName]")
    val entity = getEntity(entityName)
    StructType(getLegendClassStructFields(entity.toLegendClass))
  }

  /**
   * Programmatically generate all SQL constraints as defined in a legend PURE language for a given entity
   * Compared to relational model where we have a target schema, entities business constraints could not be compiled
   * as spark SQL. We only return technical constraints (e.g. field multiplicity) that we convert into spark SQL.
   * For Business constraints (PURE expression), please refer to `getMappingExpectations`
   *
   * @param entityName the entity to load constraints from, provided as [namespace::entity] format
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend definitions
   */
  def getEntityExpectations(entityName: String): Map[String, Try[String]] = {
    LOGGER.info(s"Retrieving expectations for legend class [$entityName]")
    val entity = getEntity(entityName)
    getLegendClassExpectations(entity.toLegendClass, pure = false)
  }

  /**
   * Retrieve mapping from the supplied PURE model. For now, only relational models are supported
   *
   * @param mappingName the mapping name (fully qualified name) to retrieve
   * @return the legend mapping object
   */
  def getMapping(mappingName: String): LegendMapping = {
    LOGGER.info(s"Retrieving legend mapping [$mappingName]")
    Try(pureModel.getMapping(mappingName)) match {
      case Success(mapping) =>
        require(mapping.isRelational, s"mapping [$mappingName] should be relational")
        mapping
      case Failure(e) => throw new IllegalArgumentException(s"could not load mapping [$mappingName]", e)
    }
  }

  /**
   * Retrieve all qualified properties for a given entity, compile as SQL
   * @param entity the entity to compile qualified properties from
   * @param mapping the relational mapping to compile qualified properties against
   * @return a map of each derived property field with corresponding SQL
   */
  def getEntityDerivations(entity: Entity, mapping: LegendMapping): Seq[(String, String)] = {
    val entityClass = entity.toLegendClass
    entityClass.qualifiedProperties.asScala.map(qp => {
      (qp.name, compileDerivation(qp.name, mapping.getEntityName, mapping))
    })
  }

  /**
   * Programmatically generate input spark schema for a relational mapping. Following mapping transformations and
   * constraints, we will be able to transform this entity into its desired state.
   *
   * @param mappingName the mapping entity used to transform entity onto a table
   * @return the spark schema for the Legend mapping entity
   */
  def getMappingSchema(mappingName: String): StructType = {
    LOGGER.info(s"Retrieving schema for legend mapping [$mappingName]")
    val mapping = getMapping(mappingName)
    val entityName = mapping.getEntityName
    getEntitySchema(entityName)
  }

  /**
   * Programmatically generate all SQL constraints as defined in a legend PURE language for a given relational mapping
   * We extract pure domain constraints (e.g. `|this.score > 0`) as well as technical constraints (e.g. mandatory) that
   * we convert into spark SQL
   *
   * @param mappingName the mapping entity used to transform entity onto a table
   * @return the list of rules as ruleName + ruleSQL code to maintain consistency with Legend definitions
   */
  def getMappingExpectations(mappingName: String): Map[String, Try[String]] = {
    LOGGER.info(s"Retrieving expectations for legend mapping [$mappingName]")
    val mapping = getMapping(mappingName)
    val entityName = mapping.getEntityName
    val entity = getEntity(entityName)
    val expectations = getLegendClassExpectations(entity.toLegendClass)
    expectations.filter(_._2.isSuccess).map({ case (name, expectation) =>
      (name, Try(compileExpectation(expectation.get, entityName, mapping)))
    })
  }

  /**
   * Given a legend mapping, we generate all transformations required to persist an entity to a relational table
   *
   * @param mappingName the name of the mapping to transform entity into a table
   * @return the set of transformations required
   */
  def getMappingTransformations(mappingName: String): Map[String, String] = {
    LOGGER.info(s"Retrieving transformations for legend mapping [$mappingName]")
    val mapping = getMapping(mappingName)
    val relational = mapping.getRelationalTransformation
    relational.getMappingFields
  }

  /**
   * Given a legend mapping, we retrieve definition of our target table.
   * @param mappingName the name of the mapping to transform entity into a table
   * @return the name of our target table
   */
  def getMappingTable(mappingName: String): String = {
    LOGGER.info(s"Retrieving target table for legend mapping [$mappingName]")
    val mapping = getMapping(mappingName)
    val relational = mapping.getRelationalTransformation
    relational.getMappingTable
  }

  /**
   * Given a set of expectations and a relational mapping, we can compile all of our PURE constraints (technical and
   * business constraints) into SQL operations. For that purpose, we create a Databricks runtime to generate an
   * execution plan.
   *
   * @param expectation the constraint to generate SQL from, as PURE expression
   * @param entityName the name of the entity this constraint applies to
   * @param mapping the relational mapping that will help us convert field into coolumn
   * @return the SQL generated constraint
   */
  private def compileExpectation(expectation: String, entityName: String, mapping: LegendMapping): String = {

    // We generate code to query table with constraints as a WHERE clause
    val query = "%1$s->getAll()->filter(this|%2$s)".format(entityName, expectation)

    // We generate an execution plan
    val plan = LegendUtils.generateExecutionPlan(query, mapping, pureRuntime, pureModel)

    // We retrieve the SQL where clause
    val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]

    // We update our expectations with actual SQL expressions
    LegendUtils.parseSqlWhere(sqlExecPlan)

  }

  private def compileDerivation(derivation: String, entityName: String, mapping: LegendMapping): String = {

    // Build our query plan in pure
    val query = "%1$s.all()->project([x|$x.%2$s],['%2$s'])".format(entityName, derivation)

    // We generate an execution plan
    val plan = LegendUtils.generateExecutionPlan(query, mapping, pureRuntime, pureModel)

    // We retrieve the SQL where clause
    val sqlExecPlan = plan.rootExecutionNode.executionNodes.get(0).asInstanceOf[SQLExecutionNode]

    // We update our expectations with actual SQL expressions
    LegendUtils.parseSqlSelect(sqlExecPlan)

  }

  /**
   * We retrieve all rules from the Legend schema. This will create SQL expression to validate schema integrity
   * as well as allowed values for enumerations
   *
   * @param legendProperty the legend property object to create rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules expressed as SQL expressions. Unchecked yet, we'll test for syntax later
   */
  private def getLegendPropertyExpectations(
                                             legendProperty: Property,
                                             parentField: String,
                                             pure: Boolean = true
                                           ): Map[String, Try[String]] = {

    // the first series of rules are simply derived from the nullability and multiplicity of each field
    val defaultRules: Map[String, Try[String]] = getLegendFieldExpectations(legendProperty, parentField, pure)

    // we need to go through more complex structures, such as nested fields or enumerations
    if (legendProperty.`type`.contains("::")) {

      // Object being referenced externally, we have to ensure this was loaded
      val nestedEntity = getEntity(legendProperty.`type`)
      val nestedColumn = LegendUtils.childFieldName(legendProperty.name, parentField)
      nestedEntity.getContent.get("_type").asInstanceOf[String].toLowerCase() match {

        case "class" =>
          // We need to validate each underlying object if field is not optional
          // We simply recurse the same logic at a child level
          if (legendProperty.isCollection) defaultRules else {
            val nestedRules: Map[String, Try[String]] = getLegendClassExpectations(
              nestedEntity.toLegendClass,
              LegendUtils.childFieldName(legendProperty.name, parentField),
              pure
            )
            defaultRules ++ nestedRules
          }
        case "enumeration" =>
          // We simply validate field against available enum values
          val values = nestedEntity.toLegendEnumeration.values.asScala.map(_.value)
          val constraint = if (pure) {
            Success("$this.%1$s->isEmpty() || $this.%1$s->in([%2$s])".format(
              nestedColumn, values.map(v => s"'$v'").mkString(", ")))
          } else {
            Success("%1$s IS NULL OR %1$s IN (%2$s)".format(
              nestedColumn, values.map(v => s"'$v'").mkString(", ")))
          }
          val allowedValues = Map(s"[$nestedColumn] not allowed value" -> constraint)
          defaultRules ++ allowedValues

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
   * If field is enum or class, legend specs must have been loaded as well.
   * These may results in nested field in our spark schema
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
          val doc = if (property.getDoc.isEmpty && nestedObject.getDoc.isDefined)
            nestedObject.getDoc else property.getDoc

          // We retrieve the full schema of that nested object as a StructType
          // We need to capture nested objects recursively through the getEntityStructFields method
          val nestedSchema = StructType(getLegendClassStructFields(nestedObject))
          val dataType = if (property.isCollection) ArrayType(nestedSchema) else nestedSchema

          // We define this field as a StructField with nested entity of datatype StructType
          StructField(property.name, dataType, property.isNullable, getDocToMetadata(doc))

        // Legend Entity is of type enumeration
        case "enumeration" =>

          // If the metadata is empty, we'll use the one from our nested object (if any)
          val nestedObject: Enumeration = nestedEntity.toLegendEnumeration
          val doc = if (property.getDoc.isEmpty && nestedObject.getDoc.isDefined)
            nestedObject.getDoc else property.getDoc

          // Even though entity is defined externally, it can be considered as type String instead of nested object
          // We do not have to go through each of its allowed value when defining schema
          val dataType = if (property.isCollection) ArrayType(StringType) else StringType

          // We define this field as a StructField of type StringType
          StructField(property.name, dataType, property.isNullable, getDocToMetadata(doc))

        // Neither an enumeration or a class object
        case _ => throw new IllegalArgumentException(
          s"referenced legend entities should be of type [enumeration] or [class]," +
            s" got [${nestedEntity.getContent.get("_type").toString}]")
      }

    } else {
      // Primitive type, becomes a simple mapping from LegendDataType to SparkDataType
      val dataType = if (property.isCollection) ArrayType(property.convertDataType) else property.convertDataType
      val metadata = if (property.getDoc.isDefined)
        new MetadataBuilder().putString("comment", property.getDoc.get).build() else new MetadataBuilder().build()
      StructField(property.name, dataType, property.isNullable, metadata)
    }
  }

  /**
   * We retrieve all constraints associated to a Legend entity of type [Class].
   * We find all constraints that are field specific as well as parsing domain expert constraints
   * expressed as a Pure Lambda function. All constraints are expressed as SQL statements that we further evaluate as a
   * spark expression (syntax check). Invalid rules (whether syntactically invalid - e.g. referencing a wrong field) or
   * illegal (unsupported PURE function) will still be returned as a Try[String] object
   *
   * @param legendClass the legend entity of type [Class]
   * @param parentField empty if top level object, it contains parent field for nested structure
   * @return the list of rules to evaluate dataframe against, as SQL expressions
   */
  private def getLegendClassExpectations(
                                          legendClass: Class,
                                          parentField: String = "",
                                          pure: Boolean = true): Map[String, Try[String]] = {

    val supertypes: Map[String, Try[String]] = legendClass.superTypes.asScala.flatMap(superType => {
      getLegendClassExpectations(getEntity(superType).toLegendClass, parentField, pure)
    }).toMap

    val expectations: Map[String, Try[String]] = legendClass.properties.asScala.flatMap(property => {
      getLegendPropertyExpectations(property, parentField, pure)
    }).toMap

    val constraints = if (pure) {
      legendClass.constraints.asScala.map(c => {
        (c.name, Try(c.toLambda))
      }).toMap
    } else {
      Map.empty[String, Try[String]]
    }

    supertypes ++
      expectations ++
      constraints

  }

  /**
   * The top level rules are the simplest rules to infer. Those are driven by the schema itself, checking for nullable
   * or multiplicity. Each rule has a name and an associated SQL expression. Unchecked yet, we'll test syntax later
   *
   * @param legendProperty the legend property object (i.e. the field) to infer rules from
   * @param parentField    empty if top level object, it contains parent field for nested structure
   * @return the list of rules checking for mandatory value and multiplicity
   */
  private def getLegendFieldExpectations(
                                          legendProperty: Property,
                                          parentField: String,
                                          pure: Boolean = true): Map[String, Try[String]] = {

    // Ensure we have the right field name if this is a nested entity
    val fieldName = LegendUtils.childFieldName(legendProperty.name, parentField)

    // Checking for non optional fields
    val mandatoryRule: Map[String, Try[String]] = if (!legendProperty.isNullable) {
      val constraint = if (pure) {
        Success("$this.%1$s->isNotEmpty()".format(fieldName))
      } else {
        Success("%1$s IS NOT NULL".format(fieldName))
      }
      Map(s"[$fieldName] is mandatory" -> constraint)
    } else Map.empty[String, Try[String]]

    // Checking legend multiplicity if more than 1 value is allowed
    val multiplicityRule: Map[String, Try[String]] = if (legendProperty.isCollection) {
      if (legendProperty.multiplicity.isInfinite) {
        val constraint = if (pure) {
          Success("$this.%1$s->isEmpty() || $this.%1$s->size() >= %2$s".format(
            fieldName, legendProperty.multiplicity.lowerBound))
        } else {
          Success("%1$s IS NULL OR SIZE(%1$s) >= %2$s".format(
            fieldName, legendProperty.multiplicity.lowerBound))
        }
        Map(s"[$fieldName] has invalid size" -> constraint)
      } else {
        val constraint = if (pure) {
          Success("$this.%1$s->isEmpty() || ($this.%1$s->size() >= %2$s && $this.%1$s->size() <= %3$s)".format(
            fieldName, legendProperty.multiplicity.lowerBound, legendProperty.multiplicity.getUpperBound.toInt))
        } else {
          Success("%1$s IS NULL OR (SIZE(%1$s) BETWEEN %2$s AND %3$s)".format(
            fieldName, legendProperty.multiplicity.lowerBound, legendProperty.multiplicity.getUpperBound.toInt))
        }
        Map(s"[$fieldName] has invalid size" -> constraint)
      }
    } else Map.empty[String, Try[String]]

    // Aggregate both mandatory and multiplicity rules
    mandatoryRule ++ multiplicityRule
  }

}

object Legend {

  lazy val objectMapper: ObjectMapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports
  lazy val grammarComposer: DEPRECATED_PureGrammarComposerCore =
    DEPRECATED_PureGrammarComposerCore.Builder.newInstance.withRenderStyle(RenderStyle.PRETTY).build
  lazy val pureModelString: String =
    """
      |###Connection
      |RelationalDatabaseConnection %1$s::connection
      |{
      |  store: %1$s::store;
      |  type: Databricks;
      |  specification: Databricks
      |  {
      |    hostname: 'my';
      |    port: 'name';
      |    protocol: 'is';
      |    httpPath: 'antoine';
      |  };
      |  auth: ApiToken
      |  {
      |    apiToken: 'foobar';
      |  };
      |}
      |
      |###Relational
      |Database %1$s::store
      |(
      |  Schema foo
      |  (
      |    Table bar
      |    (
      |    )
      |  )
      |)
      |
      |###Mapping
      |Mapping %1$s::mapping
      |(
      |  *%1$s::entity: Relational
      |  {
      |    ~mainTable [%1$s::store]foo.bar
      |  }
      |)
      |
      |###Pure
      |Class %1$s::entity
      |{
      |}
      |
      |###Runtime
      |Runtime %1$s::runtime
      |{
      |  mappings: [%1$s::mapping];
      |  connections: [%1$s::store: [c: %1$s::connection]];
      |}""".stripMargin

  /**
   * We generate a runtime that can be used to map entities using a spark backend.
   * writing a DatabricksSourceSpecification and authentication strategy just to process data transformations on spark
   * We create a minimalistic runtime with dummy entities to indicate the framework target is spark SQL
   * Although the mapping used by user and runtime are disconnected, we want to minimize possible side effects of
   * conflicting entities by using a unique identifier.
   *
   * @param uuid a unique identifier to minimize conflicts with user defined pure model
   * @return a legend runtime of type Databricks that can be used to build SQL code
   */
  def buildRuntime(uuid: String): LegendRuntime = {
    val uniqueIdentifier = uuid.replaceAll("-", "")
    val contextData: PureModelContextData = PureGrammarParser.newInstance.parseModel(
      pureModelString.format(uniqueIdentifier)
    )
    val additionalPure = Compiler.compile(contextData, null, null)
    additionalPure.getRuntime(s"${uniqueIdentifier}::runtime")
  }

}

