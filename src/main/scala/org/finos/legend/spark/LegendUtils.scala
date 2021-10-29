package org.finos.legend.spark

import org.apache.spark.sql.types.DataType
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain._
import org.finos.legend.pure.generated.{Root_meta_relational_mapping_RelationalPropertyMapping_Impl, Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl, Root_meta_relational_metamodel_TableAliasColumn_Impl, Root_meta_relational_metamodel_relation_Table_Impl}
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping
import org.finos.legend.sdlc.domain.model.entity.Entity

import scala.collection.JavaConverters._

object LegendUtils {

  implicit class MappingImpl(mapping: Mapping) {
    def getRelationalTransformation: Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl = {
      val transformations = mapping._classMappings().asScala
      require(transformations.nonEmpty)
      require(transformations.head.isInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl])
      val transformation = transformations.head.asInstanceOf[Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl]
      require(transformation._mainTableAlias._relationalElement() != null)
      require(transformation._mainTableAlias._relationalElement().isInstanceOf[Root_meta_relational_metamodel_relation_Table_Impl])
      transformation
    }
  }

  /**
   * Utility class to manipulate Legend Entity object easily, deserializing Entity content as a Class or Enumeration
   *
   * @param entity the Legend entity
   */
  implicit class EntityImpl(entity: Entity) {
    def toLegendClass: Class = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "class", s"Could only create a schema from an entity of type [class], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Class])
    }

    def toLegendEnumeration: Enumeration = {
      val entityType = entity.getContent.get("_type").asInstanceOf[String].toLowerCase()
      require(entityType == "enumeration", s"Could only create a schema from an entity of type [enumeration], got [${entityType}]")
      Legend.objectMapper.convertValue(entity.getContent, classOf[Enumeration])
    }

    def isClass: Boolean = entity.getContent.get("_type").asInstanceOf[String].toLowerCase() == "class"

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
    def convertDataType: DataType = Legend.convertDataTypeFromString(property.`type`)
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
     * We fix that with a simple hack for now //TODO: fix this hack
     *
     * @return the Lambda representation of a constraint
     */
    def toLambda: String = {
      Legend
        .objectMapper
        .writeValueAsString(constraint.functionDefinition.accept(Legend.grammarComposer))
        .dropRight(1)
        .drop(2)
        .replaceAll("\\\\n", "")
        .replaceAll("\\s+", "")
    }
  }


  implicit class TransformationImpl(transformation: Root_meta_relational_mapping_RootRelationalInstanceSetImplementation_Impl) {

    def getTransformations: Seq[WithColumnRenamed] = {
      transformation._propertyMappings.asScala.flatMap({ o =>
        o match {
          case p: Root_meta_relational_mapping_RelationalPropertyMapping_Impl =>
            p._relationalOperationElement match {
              case e: Root_meta_relational_metamodel_TableAliasColumn_Impl =>
                Some(WithColumnRenamed(p._property()._name(), e._columnName()))
              case _ => None
            }
          case _ =>
            None
        }
      }).toSeq
    }

    def getTable: String = {
      val target = transformation._mainTableAlias._relationalElement().asInstanceOf[Root_meta_relational_metamodel_relation_Table_Impl]
      target._schema._name() + "." + target._name()
    }

  }

}
