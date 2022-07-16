package org.finos.legend.spark

import java.io.StringReader
import java.util.Collections
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.select.{PlainSelect, Select}
import org.apache.spark.sql.types._
import org.finos.legend.engine.language.pure.compiler.toPureGraph.{CompileContext, HelperValueSpecificationBuilder, PureModel}
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser
import org.finos.legend.engine.plan.generation.PlanGenerator
import org.finos.legend.engine.plan.generation.transformers.LegendPlanTransformers
import org.finos.legend.engine.plan.platform.PlanPlatform
import org.finos.legend.engine.protocol.pure.PureClientVersions
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.nodes.SQLExecutionNode
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.service.Service
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.ValueSpecification
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.Lambda
import org.finos.legend.pure.generated.{Root_meta_relational_mapping_RelationalPropertyMapping_Impl, Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl, Root_meta_relational_metamodel_TableAliasColumn_Impl, Root_meta_relational_metamodel_relation_Table_Impl, core_relational_relational_router_router_extension}
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime
import org.finos.legend.sdlc.domain.model.entity.Entity

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

object LegendUtils {

  @tailrec
  private def getMappingEntityName(pack: org.finos.legend.pure.m3.coreinstance.Package, fqn: String): String = {
    if (pack._package() == null || pack._package()._name() == "Root") {
      pack._name() + "::" + fqn
    } else {
      getMappingEntityName(pack._package(), pack._name() + "::" + fqn)
    }
  }

  implicit class MappingUtilsImpl(mapping: Mapping) {

    def getRelationalTransformation: Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl = {
      val transformations = mapping._classMappings().asScala
      require(transformations.nonEmpty)
      require(transformations.head.isInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl])
      val transformation = transformations.head.asInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl]
      require(transformation._mainTableAlias._relationalElement() != null)
      require(transformation._mainTableAlias._relationalElement().isInstanceOf[Root_meta_relational_metamodel_relation_Table_Impl])
      transformation
    }

    def isRelational: Boolean = {
      Try(mapping.getRelationalTransformation).isSuccess
    }

    def getEntityName: String = {
      val classMappings = mapping._classMappings().asScala.toList
      getMappingEntityName(
        classMappings.head._class()._package(),
        classMappings.head._class()._name()
      )
    }
  }

  implicit class EntityUtilsImpl(entity: Entity) {

    def toLegendMapping: Mapping = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "mapping", s"Entity should be of type [mapping], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Mapping])
    }

    def toLegendService: Service = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "service", s"Entity should be of type [service], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Service])
    }

    def toLegendClass: Class = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "class", s"Entity should be of type [class], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Class])
    }

    def toLegendEnumeration: Enumeration = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "enumeration", s"Entity should be of type [enumeration], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Enumeration])
    }

    def isClass: Boolean = entity.getContent.get("_type").asInstanceOf[String].toLowerCase() == "class"

  }

  /**
   * A complexity when dealing with nested fields is to make sure we call a field with a [parent.child] syntax
   *
   * @param fieldName       the name of the field
   * @param parentFieldName the name of the parent field this field is included into (empty for top level object)
   * @return the concatenation of [parent.child] to reference this field
   */
  def childFieldName(fieldName: String, parentFieldName: String): String =
    if (parentFieldName.isEmpty) fieldName else s"$parentFieldName.$fieldName"

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
   *
   * @param executionPlan generated SQL plan from legend engine
   * @return the WHERE clause of the generated SQL expression
   */
  def parseSqlWhere(executionPlan: SQLExecutionNode): String = {
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(executionPlan.sqlQuery)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val alias = s"${select.getFromItem.getAlias.getName}."
    val where = select.getWhere
    where.toString.replaceAll(alias, "")
  }

  /**
   * Parse SQL to retrieve SELECT clause
   *
   * @param executionPlan generated SQL plan from legend engine
   * @return the selected field for the generated SQL expression
   */
  def parseSqlSelect(executionPlan: SQLExecutionNode): String = {
    val parserRealSql = new CCJSqlParserManager()
    val select = parserRealSql.parse(new StringReader(executionPlan.sqlQuery)).asInstanceOf[Select].getSelectBody.asInstanceOf[PlainSelect]
    val alias = s"${select.getFromItem.getAlias.getName}."
    select.getSelectItems.get(0).toString.replaceAll(alias, "")
  }

  def generateExecutionPlan(query: String, legendMapping: Mapping, legendRuntime: runtime.Runtime, pureModel: PureModel): SingleExecutionPlan = {
    PlanGenerator.generateExecutionPlan(
      buildLambda(query, pureModel),
      legendMapping,
      legendRuntime,
      null,
      pureModel,
      "vX_X_X",
      PlanPlatform.JAVA,
      null,
      core_relational_relational_router_router_extension.Root_meta_pure_router_extension_defaultRelationalExtensions__RouterExtension_MANY_(pureModel.getExecutionSupport),
      LegendPlanTransformers.transformers
    )
  }

  def getDocToMetadata(doc: Option[String]): Metadata = {
    if (doc.isDefined) {
      new MetadataBuilder().putString("comment", doc.get).build()
    } else {
      new MetadataBuilder().build()
    }
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


  /**
   * Utility class to manipulate Legend Property object (i.e. a Field) easily
   *
   * @param property the Legend Property object
   */
  implicit class PropertyImpl(property: Property) {

    /**
     * If multiplicity has lowerBound of 0, field is optional
     *
     * @return true if field is optional, false otherwise
     */
    def isNullable: Boolean = property.multiplicity.lowerBound == 0

    /**
     * If multiplicity has upperBound not set to 1, field accepts multiple values
     *
     * @return true if field is of type array, false otherwise
     */
    def isCollection: Boolean = property.multiplicity.isInfinite || property.multiplicity.getUpperBound > 1

    /**
     * Field may have an associated Doc as a Tagged value
     *
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = property.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")

    /**
     * Simple mapping function that converts Legend data type into Spark SQL DataType
     *
     * @return the corresponding DataType for a given legend type
     */
    def convertDataType: DataType = convertDataTypeFromString(property.`type`)
  }

  implicit class ClassImpl(clazz: Class) {
    /**
     * Class may have an associated Doc as a Tagged value
     *
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = clazz.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class QualifiedPropertyImpl(property: QualifiedProperty) {
    /**
     * Class may have an associated Doc as a Tagged value
     *
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = property.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class EnumerationImpl(enumeration: Enumeration) {
    /**
     * Entity may have an associated Doc as a Tagged value
     *
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = enumeration.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class ConstraintImpl(constraint: Constraint) {
    /**
     * Convert a JSON constraint into its lambda definition
     * Constraints are defined as anonymous lambda function incompatible with Pure execution plan
     *
     * @return the Lambda representation of a constraint
     */
    def toLambda: String = {
      Legend
        .objectMapper
        .writeValueAsString(constraint.functionDefinition.accept(Legend.grammarComposer))
        .dropRight(1)
        .drop(2)
        .replaceAll("\\\\n\\s*", "")
    }
  }

  implicit class LambdaImpl(lambda: Lambda) {
    /**
     * Convert a JSON constraint into its lambda definition
     * Constraints are defined as anonymous lambda function incompatible with Pure execution plan
     *
     * @return the Lambda representation of a constraint
     */
    def toLambda: String = {
      Legend
        .objectMapper
        .writeValueAsString(lambda.accept(Legend.grammarComposer))
        .dropRight(1)
        .drop(2)
        .replaceAll("\\\\n\\s*", "")
    }
  }

  implicit class TransformationImpl(transformation: Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl) {

    def getMappingFields: Map[String, String] = {

      transformation._propertyMappings.asScala.flatMap({ o =>
        o match {
          case p: Root_meta_relational_mapping_RelationalPropertyMapping_Impl =>
            p._relationalOperationElement match {
              case e: Root_meta_relational_metamodel_TableAliasColumn_Impl =>
                Some((p._property()._name(), e._columnName()))
              case _ => None
            }
          case _ =>
            None
        }
      }).toMap
    }

    def getMappingTable: String = {
      val target = transformation._mainTableAlias._relationalElement().asInstanceOf[Root_meta_relational_metamodel_relation_Table_Impl]
      target._schema._name() + "." + target._name()
    }

  }

}
