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

  private final val DELTA_DEFAULT_DB = "default"
  private final val DELTA_DEFAULT_PROPERTY = "auto-generated property"

  def codeGen(dataframe: DataFrame, tableName: String): String = {
    val fields = processFields(dataframe.schema.fields)
    val (database, table) = getDatabaseTableName(tableName)
    PureDatabase(database, Array(PureTable(table, fields))).toPure
  }

  def codeGen(schema: StructType, tableName: String): String = {
    val fields = processFields(schema.fields)
    val (database, table) = getDatabaseTableName(tableName)
    PureDatabase(database, Array(PureTable(table, fields))).toPure
  }

  def codeGen(tableName: String): String = {
    val schema = DeltaTable.forName(tableName).toDF.schema
    val fields = processFields(schema.fields)
    val (database, table) = getDatabaseTableName(tableName)
    PureDatabase(database, Array(PureTable(table, fields))).toPure
  }

  private def getDatabaseTableName(tableName: String): (String, String) = {
    tableName.split("\\.").take(2) match {
      case Array(db, tb) => (db, tb)
      case Array(tb) => (DELTA_DEFAULT_DB, tb)
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
        case _ => processFieldPrimitive(f)
      }
    })
  }

  private def processFieldArray(field: StructField): PureField = {
    val cardinality = if (field.nullable) "[0..*]" else "[1..*]"
    val pureDatatype = convertSparkToPureDataType(field.dataType.asInstanceOf[ArrayType].elementType)
    val description = getFieldDescription(field)
    PureField(field.name, cardinality, pureDatatype, description)
  }

  private def processFieldPrimitive(field: StructField): PureField = {
    val cardinality = if (field.nullable) "[0..1]" else "[1]"
    val pureDatatype = convertSparkToPureDataType(field.dataType)
    val description = getFieldDescription(field)
    PureField(field.name, cardinality, pureDatatype, description)
  }

  private def getFieldDescription(field: StructField): String = {
    if (field.metadata.contains("comment"))
      field.metadata.getString("comment")
    else DELTA_DEFAULT_PROPERTY
  }

  private def convertSparkToPureDataType(d: DataType): PureDatatype = {
    d match {
      case _: FloatType => PureDatatype("Float", "DOUBLE")
      case _: DecimalType => PureDatatype("Decimal", "DOUBLE")
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
      case _ => throw new IllegalArgumentException(s"Unsupported field type [$d]")
    }
  }
}
