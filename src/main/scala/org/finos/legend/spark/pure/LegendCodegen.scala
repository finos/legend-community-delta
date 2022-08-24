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

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

class LegendCodegen(namespace: String, tableName: String, schema: StructType) {

  var store = new ListBuffer[PureClass]()

  def generate: Array[PureClass] = {
    val entityName = tableName.camelCaseEntity
    processEntity(entityName, schema)
    store.toArray
  }

  private def processEntity(entityName: String, schema: StructType, isNested: Boolean = false): Unit = {
    val fields = processEntityFields(entityName, schema.fields)
    store.append(PureClass(tableName, s"$namespace::$NAMESPACE_classes::$entityName", fields, isNested))
  }

  private def processEntityFields(entityName: String, fields: Array[StructField]): Array[PureField] = {
    fields.map(f => {
      f.dataType match {
        case s: StructType =>
          processEntity(s"$entityName${f.name.camelCaseEntity}", s, isNested = true)
          PureField(f.name, f.cardinality, f.toPureType(entityName), f.description, isComplex = true)
        case a: ArrayType =>
          a.elementType match {
            case structType: StructType =>
              processEntity(s"$entityName${f.name.camelCaseEntity}", structType, isNested = true)
              PureField(f.name, f.cardinality, f.toPureType(entityName), f.description, isComplex = true)
            case _ => PureField(f.name, f.cardinality, f.toPureType(entityName), f.description)
          }
        case _ => PureField(f.name, f.cardinality, f.toPureType(entityName), f.description)
      }
    })
  }

  implicit class StructFieldImpl(f: StructField) {

    def cardinality: String = f.dataType match {
      case _: ArrayType => if (f.nullable) "[0..*]" else "[1..*]"
      case _ => if (f.nullable) "[0..1]" else "[1]"
    }

    def toPureType(entityName: String): PureDatatype = {
      f.dataType match {
        case _: FloatType => PureDatatype("Float", "DOUBLE")
        case _: DoubleType => PureDatatype("Decimal", "DOUBLE")
        case _: ByteType => PureDatatype("Integer", "TINYINT")
        case _: ShortType => PureDatatype("Integer", "SMALLINT")
        case _: IntegerType => PureDatatype("Integer", "INTEGER")
        case _: LongType => PureDatatype("Number", "BIGINT")
        case _: StringType => PureDatatype("String", s"VARCHAR(${Int.MaxValue})")
        case _: BooleanType => PureDatatype("Boolean", "BIT")
        case _: BinaryType => PureDatatype("Binary", s"BINARY(${Int.MaxValue})")
        case _: DateType => PureDatatype("Date", "DATE")
        case _: TimestampType => PureDatatype("DateTime", "TIMESTAMP")
        case _: StructType =>
          // Nested elements handled as String on legend SQL
          val nestedEntityReference = s"$namespace::$NAMESPACE_classes::$entityName${f.name.camelCaseEntity}"
          PureDatatype(nestedEntityReference, s"VARCHAR(${Int.MaxValue})")
        case _: ArrayType =>
          // Array elements go through recursion
          val elementType = f.dataType.asInstanceOf[ArrayType].elementType
          f.copy(dataType = elementType).toPureType(entityName)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported field type [${f.dataType}] for field [${f.name}]")
      }
    }

    def description: Option[String] = if (f.metadata.contains("comment"))
      Some(f.metadata.getString("comment")) else None: Option[String]
  }
}

object LegendCodegen {

  final val LOGGER = LoggerFactory.getLogger(this.getClass)

  /**
   * Given a database, we retrieve all delta tables, get their spark schema and generate their corresponding legend
   * PURE data model. We create entities, relational store and mappings so that delta tables can be queries straight
   * away from the legend studio or query interface
   * @param namespace the legend namespace, in the form of [a-z]+::[a-z]+::.*
   * @param databaseName the name of the delta database to read metadata from
   * @return a validated PURE model that can be copied / pasted in legend studio
   */
  def generatePureFromDatabase(namespace: String, databaseName: String): String = {
    generatePure(namespace, databaseName)
  }

  def generatePure(namespace: String, databaseName: String): String = {
    require(SparkSession.getDefaultSession.isDefined, "A spark session should be active")
    require(namespace.isValidNamespace, "namespace should be in the form of group::artifact::.*")
    val spark = SparkSession.active
    val schemas = spark
      .sql(s"SHOW TABLES IN $databaseName")
      .rdd
      .collect()
      .map(r => r.getAs[String]("tableName"))
      .flatMap(tableName => {
        val table = s"$databaseName.$tableName"
        LOGGER.info(s"Accessing spark schema for table [$table]")
        Try(DeltaTable.forName(table).toDF.schema) match {
          case Success(schema) => Some((tableName, schema))
          case Failure(exception) =>
            LOGGER.error(s"Could not read delta table [$table], ${exception.getMessage}", exception)
            None
        }
      }).toMap

    LOGGER.info(s"Generating PURE model for ${schemas.size} table(s)")
    generatePureFromSchemas(namespace, databaseName, schemas)
  }

  /**
   * Given a database and a delta table, we retrieve its spark schema and generate its corresponding legend
   * PURE data model. We create entities, relational store and mappings so that delta tables can be queries straight
   * away from the legend studio or query interface
   * @param namespace the legend namespace, in the form of [a-z]+::[a-z]+::.*
   * @param databaseName the name of the delta database to read metadata from
   * @param tableName the name of the delta table to read metadata from
   * @return a validated PURE model that can be copied / pasted in legend studio
   */
  def generatePureFromTable(namespace: String, databaseName: String, tableName: String): String = {
    generatePure(namespace, databaseName, tableName)
  }

  def generatePure(namespace: String, databaseName: String, tableName: String): String = {
    require(SparkSession.getDefaultSession.isDefined, "A spark session should be active")
    require(namespace.isValidNamespace, "namespace should be in the form of group::artifact::.*")
    val table = s"$databaseName.$tableName"
    LOGGER.info(s"Accessing spark schema for table [$table]")
    Try(DeltaTable.forName(table).toDF.schema) match {
      case Success(schema) =>
        LOGGER.info(s"Generating PURE model for table [$table]")
        generatePureFromSchema(namespace, databaseName, tableName, schema)
      case Failure(exception) =>
        LOGGER.error(s"Could not read delta table [$table], ${exception.getMessage}", exception)
        throw exception
    }
  }

  private[pure] def generatePureFromSchemas(namespace: String, databaseName: String, schemas: Map[String, StructType]): String = {
    require(namespace.isValidNamespace, "namespace should be in the form of group::artifact::.*")
    val pureClasses = schemas.map({ case (tableName, schema) =>
      new LegendCodegen(namespace, tableName, schema).generate
    }).reduce(_++_)
    val model = PureModel(databaseName, pureClasses).toPure(namespace)
    require(model.isValidPureModel, "Could not compile generated model \n" + model)
    model
  }

  private[pure] def generatePureFromSchema(namespace: String, databaseName: String, tableName: String, schema: StructType): String = {
    require(namespace.isValidNamespace, "namespace should be in the form of group::artifact::.*")
    val pureClasses = new LegendCodegen(namespace, tableName, schema).generate
    val model = PureModel(databaseName, pureClasses).toPure(namespace)
    require(model.isValidPureModel, "Could not compile generated model \n" + model)
    model
  }

}
