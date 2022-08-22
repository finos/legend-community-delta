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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.util.Try

object LegendCodegen {

  private final val DELTA_DEFAULT_PROPERTY = "auto-generated property"
  final val LOGGER = LoggerFactory.getLogger(this.getClass)

  def codeGen(database: String): String = {

    val sparkOpt = SparkSession.getDefaultSession
    require(sparkOpt.isDefined, "A spark session should be active")
    val spark = sparkOpt.get

    // Generate PURE model for each table
    val tables = spark.sql(s"SHOW TABLES IN $database").rdd.filter(r => {
      !r.getAs[Boolean]("isTemporary")
    }).map(_.getAs[String]("tableName")).collect().flatMap(tableName => {
      LOGGER.info(s"Generate PURE model for table $tableName")
      Try {
        val schema = DeltaTable.forName(s"$database.$tableName").toDF.schema
        codeGen(schema, tableName)
      }.toOption
    })

    // Serialize model
    PureDatabase(database, tables).toPure

  }

  def codeGen(dataframe: DataFrame, tableName: String): PureTable = {
    val fields = processFields(dataframe.schema.fields)
    PureTable(tableName, fields)
  }

  def codeGen(schema: StructType, tableName: String): PureTable = {
    val fields = processFields(schema.fields)
    PureTable(tableName, fields)
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
