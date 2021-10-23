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

package org.finos.legend

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.{Class, Constraint, Enumeration, Property, QualifiedProperty}
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.ValueSpecification
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.{AppliedFunction, AppliedProperty}
import org.finos.legend.sdlc.domain.model.entity.Entity

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * UTILITY FUNCTIONS AND IMPLICIT CLASSES
 */
package object spark {

  implicit class DataFrameImpl(df: DataFrame) {

    /**
     * Given a list of expectations, we evaluate them on a dataframe. Each row will be enriched
     * with an additional column listing all constraints that failed validation
     *
     * @param rules   the list of expectations to evaluate
     * @param colName the name of the output field containing expectation results
     * @return a dataframe enriched with failed constraints
     */
    def legendExpectations(rules: Seq[Expectation], colName: String = "legend"): DataFrame = {
      val combine = udf((xs: Seq[Boolean], ys: Seq[String]) => xs.zip(ys).filter(!_._1).map(_._2))
      df
        .withColumn("_legend_rules", array(rules.map(_.expression.get).map(expr): _*))
        .withColumn("_legend_names", array(rules.map(_.name).map(lit): _*))
        .withColumn(colName, combine(col("_legend_rules"), col("_legend_names")))
        .drop("_legend_rules", "_legend_names")
    }
  }

  /**
   * A complexity when dealing with nested fields is to make sure we call a field with a [parent.child] syntax
   *
   * @param fieldName       the name of the field
   * @param parentFieldName the name of the parent field this field is included into (empty for top level object)
   * @return the concatenation of [parent.child] to reference this field
   */
  def childFieldName(fieldName: String, parentFieldName: String): String =
    if (parentFieldName.isEmpty) s"`$fieldName`" else s"$parentFieldName.`$fieldName`"

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
        throw new IllegalArgumentException(s"Entity of class [${returnType}] is not supported as primitive")
    }
  }

  /**
   * Capturing legend entity expectations as SQL expressions. As we do not want to fail because of non compatibility of
   * certain legend rules, we ensure SQL expressions are duly evaluated and wrapped as a Try[String] object
   * We check for field nullity first. Reason is we do not want to fail a constraint for a null and mandatory field
   * that obviously does not match constraint (LENGTH(field) > 12) should still be valid for null string
   *
   * @param nullableFields the name of the fields this expression applies to that we want to check nullity first
   * @param lambda         the lambda representation of a constraint
   * @param name           the name of the expression (business specific)
   * @param expression     the expression evaluated as a SQL expression
   */
  case class Expectation(nullableFields: Seq[String], name: String, lambda: Option[String], expression: Try[String]) {

    /**
     * Given a rule expressed as a SQL string, we evaluate syntax by passing string into a Spark function
     *
     * @return the rule further evaluated for SQL syntax check
     */
    def validate: Expectation = {
      if (expression.isFailure) return this
      val sql = nullableFields.length match {
        case 0 => expression.get
        case 1 => s"(${nullableFields.head} IS NULL) OR (${expression.get})"
        case _ => s"(${nullableFields.map(f => s"$f IS NULL").mkString(" OR ")}) OR (${expression.get})"
      }
      Try(expr(sql)) match {
        case Success(_) => this.copy(expression = Success(sql))
        case Failure(e) => this.copy(expression = Failure(e))
      }
    }
  }

  /**
   * Capturing drift of a legend entity given a predefined schema. If backwards compatible, we store all alter statements
   * one could run to update a Spark table, including new columns or metadata
   *
   * @param alterStatements alter SQL statements to update a table with
   * @param isCompatible    if new schema resulted in datatype change or delete columns, we breack backwards compatibility
   */
  case class SchemaDrift(alterStatements: Seq[String], isCompatible: Boolean)

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
     * We fix that with a simple hack for now //TODO: fix this hack
     *
     * @return the Lambda representation of a constraint
     */
    def toLambda: String = {
      Legend.objectMapper.writeValueAsString(constraint.functionDefinition.accept(Legend.grammarComposer))
    }
  }

}
