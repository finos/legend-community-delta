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

  val DEFAULT_NAMESPACE: String = "legend::delta::generated"
  val AUTO_GENERATED: String = "auto-generated"

  case class PureDatatype(pureType: String, pureRelationalType: String)

  case class PureField(name: String, cardinality: String, pureType: PureDatatype, description: String, isComplex: Boolean = false) {
    def toPure: String = {
      s"{meta::pure::profiles::doc.doc = '$description'} $name: ${pureType.pureType}$cardinality;"
    }
    def toRelational: String = {
      s"$name ${pureType.pureRelationalType}"
    }
    def toPrimaryKey(namespace: String, databaseName: String, tableName: String):String = {
      s"[$namespace::lakehouse::schema]$databaseName.$tableName.$name"
    }
    def toMapping(namespace: String, databaseName: String, tableName: String): String = {
      require(!isComplex, "Nested properties cannot be mapped to relational objects on legend")
      s"$name: [$namespace::lakehouse::schema]$databaseName.$tableName.$name"
    }
  }

  case class PureClass(entityFQN: String, fields: Array[PureField], isNested: Boolean = false) {
    val tableName: String = entityFQN.split("::").last
    def toPure: String = {
      s"""Class $entityFQN
         |{
         |  ${fields.map(_.toPure).mkString("\n  ")}
         |}""".stripMargin
    }
    def toRelational: String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"""    Table $tableName
         |    (
         |      ${fields.map(_.toRelational).mkString(",\n      ")}
         |    )""".stripMargin
    }

    def toMapping(namespace: String, databaseName: String): String = {
      /*
      Delta does not support PRIMARY KEY in strict sense (not needed)
      We might rely on some field metadata and CONSTRAINT columns, or here define all fields as a composite key
      Assert failure at (resource:/platform/pure/corefunctions/test.pure line:22 column:5),
      "There is no primary key defined on the table legend_primitive.
      A primary key must be defined in the table definition in PURE to use this feature"
      */
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"""Mapping $namespace::mapping::$tableName
         |(
         |  *$entityFQN: Relational
         |  {
         |    ~primaryKey
         |    (
         |      ${fields.filter(!_.isComplex).map(_.toPrimaryKey(namespace, databaseName, tableName)).mkString(",\n      ")}
         |    )
         |    ~mainTable [$namespace::lakehouse::schema]$databaseName.$tableName
         |    ${fields.filter(!_.isComplex).map(_.toMapping(namespace, databaseName, tableName)).mkString(",\n    ")}
         |  }
         |)
         |""".stripMargin
    }
  }

  case class PureModel(databaseName: String, pureTables: Array[PureClass]) {
    def toPure(namespace: String): String = {
      s"""###Pure
         |${pureTables.map(_.toPure).mkString("\n\n")}
         |###Relational
         |Database $namespace::lakehouse::schema
         |(
         |  Schema $databaseName
         |  (
         |${pureTables.filter(!_.isNested).map(_.toRelational).mkString("\n")}
         |  )
         |)
         |
         |###Mapping
         |${pureTables.filter(!_.isNested).map(_.toMapping(namespace, databaseName)).mkString("\n\n")}
         |###Connection
         |RelationalDatabaseConnection $namespace::lakehouse::connection
         |{
         |  store: $namespace::lakehouse::schema;
         |  type: Databricks;
         |  specification: Databricks
         |  {
         |    hostname: 'TODO';
         |    port: 'TODO';
         |    protocol: 'TODO';
         |    httpPath: 'TODO';
         |  };
         |  auth: ApiToken
         |  {
         |    apiToken: 'TODO';
         |  };
         |}
         |
         |###Runtime
         |Runtime $namespace::lakehouse::runtime
         |{
         |  mappings:
         |  [
         |    ${pureTables.filter(!_.isNested).map(t => s"$namespace::mapping::${t.tableName}").mkString(",\n    ")}
         |  ];
         |  connections:
         |  [
         |    $namespace::lakehouse::schema:
         |    [
         |      environment: $namespace::lakehouse::connection
         |    ]
         |  ];
         |}
         |""".stripMargin
    }
  }
}
