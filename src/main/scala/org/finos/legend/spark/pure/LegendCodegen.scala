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

class LegendCodegen(namespace: String, tableName: String, schema: StructType) {

  var store = new ListBuffer[PureClass]()

  def generate: List[PureClass] = {
    val entityName = tableName.camelCase
    processEntity(entityName, schema)
    store.toList
  }

  private def processEntity(entityName: String, schema: StructType, isNested: Boolean = false): Unit = {
    val fields = processEntityFields(entityName, schema.fields)
    store.append(PureClass(tableName, s"$namespace::$NAMESPACE_classes::$entityName", fields, isNested))
  }

  private def processEntityFields(entityName: String, fields: Array[StructField]): Array[PureField] = {
    fields.map(f => {
      f.dataType match {
        case s: StructType =>
          processEntity(s"$entityName${f.name.camelCase}", s, isNested = true)
          PureField(f.name, f.cardinality, f.toPureType(entityName), f.description, isComplex = true)
        case a: ArrayType =>
          a.elementType match {
            case structType: StructType =>
              processEntity(s"$entityName${f.name.camelCase}", structType, isNested = true)
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
        case _: StructType => PureDatatype(s"$namespace::$NAMESPACE_classes::$entityName${f.name.camelCase}", s"VARCHAR(${Int.MaxValue})") // Nested elements handled as String on legend SQL
        case _: ArrayType => f.copy(dataType = f.dataType.asInstanceOf[ArrayType].elementType).toPureType(entityName)
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

  def parseDatabase(databaseName: String): String = {
    require(SparkSession.getDefaultSession.isDefined)
    val spark = SparkSession.active
    val tables = spark
      .sql(s"SHOW TABLES IN $databaseName")
      .rdd
      .collect()
      .map(r => r.getAs[String]("tableName")).map(tableName => {
        val table = s"$databaseName.$tableName"
        LOGGER.info(s"Accessing spark schema for [$table]")
        (tableName, DeltaTable.forName(table).toDF.schema)
      }).toMap
    parseSchemas(databaseName, tables)
  }

  def parseSchemas(databaseName: String, schemas: Map[String, StructType]): String = {
    val namespace = databaseName.split("_")
      .map(_.toLowerCase()).foldLeft(NAMESPACE_prefix)((prefix, suffix) => s"$prefix::$suffix")
    val pureClasses = schemas.map({ case (entityName, schema) =>
      LOGGER.info(s"Converting spark entity [$entityName] to PURE collection")
      new LegendCodegen(namespace, entityName, schema).generate
    }).foldLeft(Array.empty[PureClass])((previousClasses, newClasses) => previousClasses ++ newClasses)
    PureModel(databaseName, pureClasses).toPure(namespace)
  }

}
