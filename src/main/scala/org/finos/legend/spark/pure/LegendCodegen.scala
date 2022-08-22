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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object LegendCodegen {

  def codeGen(entityName: String, tableName: String, dataframe: DataFrame): String = {
    require(isValidEntityName(entityName), "Entity should be in the form of namespace::*::name")
    val fields = processFields(dataframe.schema.fields)
    val (database, table) = getDatabaseTableNames(tableName)
    val namespace = getNamespaceFromEntityName(entityName)
    PureTable(entityName, namespace, database, table, fields).toPure
  }

  def codeGen(entityName: String, tableName: String, schema: StructType): String = {
    require(isValidEntityName(entityName), "Entity should be in the form of namespace::*::name")
    val fields = processFields(schema.fields)
    val (database, table) = getDatabaseTableNames(tableName)
    val namespace = getNamespaceFromEntityName(entityName)
    PureTable(entityName, namespace, database, table, fields).toPure
  }

  def codeGen(entityName: String, tableName: String): String = {
    require(isValidEntityName(entityName), "Entity should be in the form of namespace::*::name")
    val schema = DeltaTable.forName(tableName).toDF.schema
    val fields = processFields(schema.fields)
    val (database, table) = getDatabaseTableNames(tableName)
    val namespace = getNamespaceFromEntityName(entityName)
    PureTable(entityName, namespace, database, table, fields).toPure
  }

  private def getNamespaceFromEntityName(entityName: String): String = {
    entityName.split("::").dropRight(1).mkString("::")
  }

  private def isValidEntityName(entityName: String): Boolean = {
    entityName.split("::").length > 1
  }

  private def getDatabaseTableNames(tableName: String): (Option[String], String) = {
    tableName.split("\\.") match {
      case Array(db, tb) => (Some(db), tb)
      case Array(tb) => (None: Option[String], tb)
    }
  }

  private def processFields(fields: Array[StructField]): Array[PureField] = {
    fields.map(f => {
      f.dataType match {
        case _: StructType =>
          throw new IllegalArgumentException(s"Field [${f.name}] is a nested property not compatible with Legend query")
        case a: ArrayType =>
          if (a.elementType.isInstanceOf[StructType])
            throw new IllegalArgumentException(s"Field [${f.name}] is a nested property not compatible with Legend query")
          processFieldArray(f)
        case _ =>
          processFieldPrimitive(f)
      }
    })
  }

  private def processFieldPrimitive(field: StructField): PureField = {
    val cardinality = if (field.nullable) "[0..1]" else "[1]"
    val pureDatatype = convertSparkToPureDataType(field.dataType, isArray = false)
    val description = getFieldDescription(field)
    PureField(field.name, cardinality, pureDatatype, description)
  }

  private def processFieldArray(field: StructField): PureField = {
    val cardinality = if (field.nullable) "[0..*]" else "[1..*]"
    val pureDatatype = convertSparkToPureDataType(field.dataType.asInstanceOf[ArrayType].elementType, isArray = true)
    val description = getFieldDescription(field)
    PureField(field.name, cardinality, pureDatatype, description)
  }

  private def getFieldDescription(field: StructField): String = {
    if (field.metadata.contains("comment")) {
      field.metadata.getString("comment")
    } else {
      "auto-generated property"
    }
  }

  private def convertSparkToPureDataType(d: DataType, isArray: Boolean): PureDatatype = {
    val pureDatatype = d match {
      case _: FloatType => PureDatatype("Float", "FLOAT")
      case _: DoubleType => PureDatatype("Decimal", "DOUBLE")
      case _: IntegerType => PureDatatype("Integer", "INT")
      case _: LongType => PureDatatype("Number", "LONG")
      case _: StringType => PureDatatype("String", "VARCHAR")
      case _: BooleanType => PureDatatype("Boolean", "BOOLEAN")
      case _: BinaryType => PureDatatype("Binary", "BINARY")
      case _: DateType => PureDatatype("Date", "DATE")
      case _: TimestampType => PureDatatype("DateTime", "TIMESTAMP")
      case _ => throw new IllegalArgumentException(s"Unsupported field type [$d]")
    }
    if (isArray) {
      pureDatatype.copy(pureRelationalType = s"ARRAY<${pureDatatype.pureRelationalType}>")
    } else {
      pureDatatype
    }
  }


}
