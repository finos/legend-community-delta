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

package org.finos.legend

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{array, col, expr, lit, udf}
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.domain.{Class, Constraint, Enumeration, Property, QualifiedProperty}
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.{ValueSpecification, Variable}
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.application.{AppliedFunction, AppliedProperty}
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.raw.{CBoolean, CDateTime, CDecimal, CFloat, CInteger, CStrictDate, CStrictTime, CString, Enum, EnumValue}
import org.finos.legend.sdlc.domain.model.entity.Entity
import org.finos.legend.spark.functions.LegendPureFunctions._
import org.finos.legend.spark.functions._

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
     * @param rules the list of expectations to evaluate
     * @param colName the name of the output field containing expectation results
     * @return a dataframe enriched with failed constraints
     */
    def legendExpectations(rules: Seq[Expectation], colName: String = "legend"): DataFrame = {
      val combine = udf((xs: Seq[Boolean], ys: Seq[String]) => xs.zip(ys).filter(!_._1).map(_._2))
      df
        .withColumn("_legend_rules", array(rules.map(_.expression.get).map(expr):_*))
        .withColumn("_legend_names", array(rules.map(_.name).map(lit):_*))
        .withColumn(colName, combine(col("_legend_rules"), col("_legend_names")))
        .drop("_legend_rules", "_legend_names")
    }

    /**
     * Given a list of SQL expressions, we enrich our initial dataframe with derivations
     * These derivations where dynamically created from PURE functions into SQL
     * @param derivations the list of derivations to apply
     * @return the original dataframe enriched with derivation fields
     */
    def legendDerivations(derivations: Seq[Derivation]): DataFrame = {
      derivations.filter(_.expression.isSuccess).foldLeft(df)((dataframe, derivation) => {
        dataframe.withColumn(derivation.fieldName, expr(derivation.expression.get))
      })
    }
  }

  /**
   * Capturing legend entity expectations as SQL expressions. As we do not want to fail because of non compatibility of
   * certain legend rules, we ensure SQL expressions are duly evaluated and wrapped as a Try[String] object
   * We check for field nullity first. Reason is we do not want to fail a constraint for a null and mandatory field
   * that obviously does not match constraint (LENGTH(field) > 12) should still be valid for null string
   * @param nullableFields the name of the fields this expression applies to that we want to check nullity first
   * @param lambda the lambda representation of a constraint
   * @param name the name of the expression (business specific)
   * @param expression the expression evaluated as a SQL expression
   */
  case class Expectation(nullableFields: Seq[String], name: String, lambda: Option[String], expression: Try[String]) {

    /**
     * Given a rule expressed as a SQL string, we evaluate syntax by passing string into a Spark function
     * @return the rule further evaluated for SQL syntax check
     */
    def build: Expectation = {
      if(expression.isFailure) return this
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
   * Capturing legend entity expectations as SQL expressions. As we do not want to fail because of non compatibility of
   * certain legend rules, we ensure SQL expressions are duly evaluated and wrapped as a Try[String] object
   * @param fieldName the field we derive from PURE function
   * @param returnType the expected return type from SQL expression
   * @param expression the expression evaluated as a SQL expression
   * @param description the metadata associated to that transformation - if any
   */
  case class Derivation(fieldName: String, returnType: DataType, expression: Try[String], description: Option[String]) {

    /**
     * Given a derivation expressed as a SQL string, we evaluate syntax by passing string into a Spark function
     * @return the rule further evaluated for SQL syntax check
     */
    def build: Derivation = {
      if(expression.isFailure) return this
      Try(expr(expression.get)) match {
        case Success(_) => this
        case Failure(e) => this.copy(expression = Failure(e))
      }
    }

    /**
     * A derived field can be added to a delta table schema
     * Given a case class object and a SQL expression, we eventually return the corresponding DDL statement
     * We only return native functions (UDFs not supported) - "A generated column cannot use a user-defined function"
     * @return the DDL statement for the generated column
     */
    def toDDL: Try[String] = {
      expression.map(e => {
        val isUDF = legendUDFs.keys.exists(e.contains)
        if(isUDF) {
          throw new IllegalArgumentException("A generated column cannot use a user-defined function")
        } else {
          if(description.isEmpty) {
            s"$fieldName ${returnType.sql} GENERATED ALWAYS AS ($e)"
          } else {
            s"$fieldName ${returnType.sql} GENERATED ALWAYS AS ($e) COMMENT '${description.get.replaceAll("'", "\'")}'"
          }
        }
      })
    }
  }

  /**
   * Capturing drift of a legend entity given a predefined schema. If backwards compatible, we store all alter statements
   * one could run to update a Spark table, including new columns or metadata
   * @param alterStatements alter SQL statements to update a table with
   * @param isCompatible if new schema resulted in datatype change or delete columns, we breack backwards compatibility
   */
  case class SchemaDrift(alterStatements: Seq[String], isCompatible: Boolean)

  /**
   * A complexity when dealing with nested fields is to make sure we call a field with a [parent.child] syntax
   * @param fieldName the name of the field
   * @param parentFieldName the name of the parent field this field is included into (empty for top level object)
   * @return the concatenation of [parent.child] to reference this field
   */
  def childFieldName(fieldName: String, parentFieldName: String): String =
    if (parentFieldName.isEmpty) s"`$fieldName`" else s"$parentFieldName.`$fieldName`"

  /**
   * Utility class to convert a PURE constraint into SQL expression
   * @param valueSpec the legend constraint expressed as a PURE object
   */
  implicit class ValueSpecImpl(valueSpec: ValueSpecification) {

    /**
     * Retrieve the name of the field this constraint may applies to
     * This will be used to test for nullity
     * @param parentField the name of the parent field
     * @return the field it applies to if any
     */
    def getFieldItAppliesTo(parentField: String = ""): Seq[String] = {
      valueSpec match {
        case p: AppliedProperty => Seq(childFieldName(p.property, parentField))
        case f: AppliedFunction => f.parameters.asScala.flatMap(_.getFieldItAppliesTo(parentField))
        case _ => Seq.empty[String]
      }
    }

    /**
     * As a best effort, we try to convert as many PURE functions as possible into SQL expressions
     * Some rules cannot be converted as non Spark existent and would need to be coded and registered as UDFs
     * @param parentField empty for top level object, we have to make sure we call fields with [parent.child]
     * @return the SQL equivalent of the PURE constraint
     */
    def convertToSQL(parentField: String = ""): String = {
      valueSpec match {
        case p: AppliedProperty   => {
          if (p.parameters.isEmpty || p.parameters.get(0).isInstanceOf[Variable]) {
            childFieldName(p.property, parentField) // fieldName
          } else if (p.parameters.isEmpty || p.parameters.get(0).isInstanceOf[Enum]) {
            s"'${p.property}'" // enum value, like DurationUnit.Days
          } else {
            throw new IllegalArgumentException(
              s"Only [Variable] of [EnumValue] are supported, got [${p.parameters.get(0).getClass.toString}]")
          }
        }
        case boolean: CBoolean    => boolean.values.asScala.map(_.toString.toUpperCase()).mkString(", ")
        case decimal: CDecimal    => decimal.values.asScala.map(_.toString).mkString(", ")
        case float: CFloat        => float.values.asScala.map(_.toString).mkString(", ")
        case integer: CInteger    => integer.values.asScala.map(_.toString).mkString(", ")
        case string: CString      => string.values.asScala.map(s => s"'$s'").mkString(", ")
        case dtime: CDateTime     => dtime.values.asScala.map(s => s"'$s'").mkString(", ")
        case time: CStrictTime    => time.values.asScala.map(s => s"'$s'").mkString(", ")
        case date: CStrictDate    => date.values.asScala.map(s => s"'$s'").mkString(", ")
        case enum: EnumValue      => s"'${enum.value}'"
        case f: AppliedFunction   =>
          if (LEGEND_FUNCTIONS_FOLD.contains(f.function)) f.convertFunctionFold(parentField)
          else if (LEGEND_FUNCTIONS_LR.contains(f.function)) f.convertFunctionLR(parentField)
          else throw new IllegalArgumentException(s"Unsupported function [${f.function}]")
        case _ =>
          throw new IllegalArgumentException(
            s"Only [AppliedProperty] of [AppliedFunction] are supported, got [${valueSpec.getClass.toString}]")
      }
    }
  }

  /**
   * Utility class to convert a PURE constraint into SQL expression
   * @param f the legend function expressed as a PURE object
   */
  implicit class AppliedFunctionImpl(f: AppliedFunction) {

    /**
     * Converting comparator functions as SQL expression
     * We ensure each group on the left and right side is enclosed in bracket so that each component can be made of multiple functions
     * @return the SQL representation
     */
    def convertFunctionLR(parentField: String): String = {
      require(f.parameters.size() >= 2, "At least 2 parameters must be specified for left right functions")
      val params = f.parameters.asScala
      val left = params.head.convertToSQL(parentField)
      val remaining = f.parameters.asScala.drop(1).map(_.convertToSQL(parentField)).mkString(", ")
      s"($left) ${LEGEND_FUNCTIONS_LR(f.function)} ($remaining)"
    }

    /**
     * Converting simple functions as SQL expression
     * All parameters are unfolded as a function argument, each of them further evaluated as functions
     * @return the SQL representation
     */
    def convertFunctionFold(parentField: String): String = {
      if (f.parameters.isEmpty)  s"${LEGEND_FUNCTIONS_FOLD(f.function)}()" else {
        s"${LEGEND_FUNCTIONS_FOLD(f.function)}(${f.parameters.asScala.map(_.convertToSQL(parentField)).mkString(", ")})"
      }
    }

  }

  def cleanColumnName(name: String): String = name.replaceAll("`", "")

  /**
   * Simple mapping function that converts Legend data type into Spark SQL DataType
   * @return the corresponding DataType for a given legend type
   */
  def convertDataTypeFromString(returnType: String): DataType = {
    returnType match {
      case "String"     => StringType
      case "Boolean"    => BooleanType
      case "Binary"     => BinaryType
      case "Integer"    => IntegerType
      case "Number"     => LongType
      case "Float"      => FloatType
      case "Decimal"    => DoubleType
      case "Date"       => DateType
      case "StrictDate" => DateType
      case "DateTime"   => TimestampType
      case _            =>
        throw new IllegalArgumentException(s"Entity of class [${returnType}] is not supported as primitive")
    }
  }

  /**
   * Utility class to manipulate Legend Entity object easily, deserializing Entity content as a Class or Enumeration
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
   * @param property the Legend Property object
   */
  implicit class PropertyImpl(property: Property) {

    /**
     * If multiplicity has lowerBound of 0, field is optional
     * @return true if field is optional, false otherwise
     */
    def isNullable: Boolean = property.multiplicity.lowerBound == 0

    /**
     * If multiplicity has upperBound not set to 1, field accepts multiple values
     * @return true if field is of type array, false otherwise
     */
    def isCollection: Boolean = property.multiplicity.isInfinite || property.multiplicity.getUpperBound > 1

    /**
     * Field may have an associated Doc as a Tagged value
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = property.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")

    /**
     * Simple mapping function that converts Legend data type into Spark SQL DataType
     * @return the corresponding DataType for a given legend type
     */
    def convertDataType: DataType = convertDataTypeFromString(property.`type`)
  }

  implicit class ClassImpl(clazz: Class) {
    /**
     * Class may have an associated Doc as a Tagged value
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = clazz.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class QualifiedPropertyImpl(property: QualifiedProperty) {
    /**
     * Class may have an associated Doc as a Tagged value
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = property.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class EnumerationImpl(enumeration: Enumeration) {
    /**
     * Entity may have an associated Doc as a Tagged value
     * @return the Doc metadata from tagged value, if any. This will be used as a COMMENT in our Spark schema
     */
    def getDoc: Option[String] = enumeration.taggedValues.asScala.map(t => (t.tag.value, t.value)).toMap.get("doc")
  }

  implicit class ConstraintImpl(constraint: Constraint) {
    /**
     * Convert a JSON constraint into its lambda definition
     * @return the Lambda representation of a constraint
     */
    def toLambda: String = Legend.objectMapper.writeValueAsString(constraint.functionDefinition.accept(Legend.grammarComposer))
  }

}
