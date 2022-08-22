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

package org.finos.legend.spark

package object pure {

  val NAMESPACE: String = "legend::delta::generated"

  case class PureDatatype(pureType: String, pureRelationalType: String)

  case class PureField(name: String, cardinality: String, pureType: PureDatatype, description: String) {

    def toPure: String = {
      s"  {meta::pure::profiles::doc.doc = '$description'} $name: ${pureType.pureType}$cardinality;"
    }

    def toRelational: String = {
      s"    $name ${pureType.pureRelationalType}"
    }

    def toMapping(tableName: String): String = {
      s"    $name: [$NAMESPACE::store::Schema]$tableName.$name"
    }

  }

  case class PureTable(database: Option[String], tableName: String, fields: Array[PureField]) {

    val dbTableName: String = if (database.isEmpty) tableName else s"${database.get}.$tableName"
    val primaryKey: String = fields.head.name

    def toRelational: String = {
      if (database.isEmpty) {
        s"""Database $NAMESPACE::store::Schema
           |(
           |  Table $tableName
           |  (
           |${fields.map(_.toRelational).mkString(",\n")}
           |  )
           |)""".stripMargin
      } else {
        s"""Database $NAMESPACE::store::Schema
           |(
           |  Schema ${database.get}
           |  (
           |    Table $tableName
           |    (
           |${fields.zipWithIndex.map(f => s"  ${f._1.toRelational(f._2)}").mkString(",\n")}
           |    )
           |  )
           |)""".stripMargin
      }
    }

    def toMapping: String = {
      s"""Mapping $NAMESPACE::mapping::${tableName}Mapping
        |(
        |  *$NAMESPACE::class::$tableName: Relational
        |  {
        |    ~primaryKey
        |    (
        |      [$NAMESPACE::store::Schema]$dbTableName.$primaryKey
        |    )
        |    ~mainTable [$NAMESPACE::store::Schema]$dbTableName
        |${fields.map(_.toMapping(dbTableName)).mkString(",\n")}
        |  }
        |)""".stripMargin
    }

    def toPure: String = {
      s"""Class $NAMESPACE::class::$tableName
         |{
         |${fields.map(_.toPure).mkString("\n")}
         |}""".stripMargin
    }
  }
}
