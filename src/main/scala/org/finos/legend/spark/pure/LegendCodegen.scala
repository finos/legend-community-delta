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
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class LegendCodegen(namespacePrefix: String, entityName: String, schema: StructType) {

  var store = new ListBuffer[PureClass]()

  def registerClass(pureClass: PureClass): Unit = {
    println(s"Registered class ${pureClass.entityFQN}")
    store.append(pureClass)
  }

  private def parseEntitySchema(entityFQN: String, schema: StructType, isNested: Boolean = false): Unit = {
    val fields = processFields(schema.fields)
    registerClass(PureClass(entityFQN, fields, isNested))
  }

  def generate: List[PureClass] = {
    parseEntitySchema(s"$namespacePrefix::$NAMESPACE_classes::$entityName", schema)
    store.toList
  }

  private def processFields(fields: Array[StructField]): Array[PureField] = {
    fields.map(f => {
      f.dataType match {
        case s: StructType =>
          parseEntitySchema(s"$namespacePrefix::$NAMESPACE_classes::${f.name}", s, isNested = true)
          PureField(f.name, f.cardinality, f.toPureType, f.description, isComplex = true)
        case a: ArrayType =>
          a.elementType match {
            case structType: StructType =>
              parseEntitySchema(s"$namespacePrefix::$NAMESPACE_classes::${f.name}", structType, isNested = true)
              PureField(f.name, f.cardinality, f.toPureType, f.description, isComplex = true)
            case _ => PureField(f.name, f.cardinality, f.toPureType, f.description, isComplex = false)
          }
        case _ => PureField(f.name, f.cardinality, f.toPureType, f.description, isComplex = false)
      }
    })
  }

  implicit class StructFieldImpl(field: StructField) {

    def cardinality: String = field.dataType match {
      case _: ArrayType => if (field.nullable) "[0..*]" else "[1..*]"
      case _ => if (field.nullable) "[0..1]" else "[1]"
    }

    def toPureType: PureDatatype = {
      field.dataType match {
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
        case _: StructType => PureDatatype(s"$namespacePrefix::$NAMESPACE_classes::${field.name}", s"VARCHAR(${Int.MaxValue})") // Nested elements handled as String on legend SQL
        case _: ArrayType => field.copy(dataType = field.dataType.asInstanceOf[ArrayType].elementType).toPureType
        case _ =>
          throw new IllegalArgumentException(s"Unsupported field type [${field.dataType}] for field [${field.name}]")
      }
    }

    def description: String = if (field.metadata.contains("comment"))
      field.metadata.getString("comment") else AUTO_GENERATED
  }
}

object LegendCodegen {

  final val LOGGER = LoggerFactory.getLogger(this.getClass)

  def parseDatabase(namespace: String, databaseName: String, tableName: String) = {
    DeltaTable.forName(s"$databaseName.$tableName").toDF
  }

  def parseDatabase(namespace: String, databaseName: String, dataFrames: Map[String, DataFrame]): String = {
    val pureClasses = dataFrames.map({ case (entityName, dataFrame) =>
      new LegendCodegen(namespace, entityName, dataFrame.schema).generate
    }).foldLeft(Array.empty[PureClass])((previousClasses, newClasses) => previousClasses ++ newClasses)
    PureModel(databaseName, pureClasses).toPure(namespace)
  }

  def parseDataframe(namespace: String, dataFrameName: String, dataframe: DataFrame): String = {
    parseStructType(namespace, dataFrameName, dataframe.schema)
  }

  def parseStructType(namespace: String, entityName: String, schema: StructType): String = {
    new LegendCodegen(namespace, entityName, schema).generate.map(_.toPure).mkString("\n\n")
  }
}
