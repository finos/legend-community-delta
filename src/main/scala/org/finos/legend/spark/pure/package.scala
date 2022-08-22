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
      s"      $name ${pureType.pureRelationalType}"
    }

    def toMapping(databaseName: String, tableName: String): String = {
      s"    $name: [$NAMESPACE::store::Schema]$databaseName.$tableName.$name"
    }

  }

  case class PureTable(tableName: String, fields: Array[PureField]) {

    val primaryKey: String = fields.head.name

    def toRelational: String = {
      s"""    Table $tableName
         |    (
         |${fields.map(_.toRelational).mkString(",\n")}
         |    )""".stripMargin
    }

    def toMapping(databaseName: String): String = {
      s"""Mapping $NAMESPACE::mapping::$tableName
        |(
        |  *$NAMESPACE::class::$tableName: Relational
        |  {
        |    ~primaryKey
        |    (
        |      [$NAMESPACE::store::Schema]$databaseName.$tableName.$primaryKey
        |    )
        |    ~mainTable [$NAMESPACE::store::Schema]$databaseName.$tableName
        |${fields.map(_.toMapping(databaseName, tableName)).mkString(",\n")}
        |  }
        |)""".stripMargin
    }

    def toPure: String = {
      s"""Class <<$NAMESPACE::Profile.'auto-generated'>> $NAMESPACE::class::$tableName
         |{
         |${fields.map(_.toPure).mkString("\n")}
         |}""".stripMargin
    }

  }

  case class PureDatabase(databaseName: String, pureTables: Array[PureTable]) {
    def toPure: String = {
      s"""###Pure
         |Profile $NAMESPACE::Profile
         |{
         |  stereotypes: ['auto-generated'];
         |}
         |
         |${pureTables.map(_.toPure).mkString("\n\n")}
         |
         |###Relational
         |Database $NAMESPACE::store::Schema
         |(
         |  Schema $databaseName
         |  (
         |${pureTables.map(_.toRelational).mkString("\n")}
         |  )
         |)
         |
         |###Mapping
         |${pureTables.map(_.toMapping(databaseName)).mkString("\n\n")}""".stripMargin
    }
  }
}
