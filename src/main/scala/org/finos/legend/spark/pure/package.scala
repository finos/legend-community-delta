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

  val AUTO_GENERATED: String = "auto-generated"

  val NAMESPACE_prefix: String = "legend::delta::generated"
  val NAMESPACE_nested: String = "nested"

  val NAMESPACE_mapping: String = "mapping"
  val NAMESPACE_classes: String = "classes"
  val NAMESPACE_connect: String = "lakehouse"

  case class PureDatatype(pureType: String, pureRelationalType: String)

  case class PureField(name: String, cardinality: String, pureType: PureDatatype, description: String, isComplex: Boolean = false) {

    def toPure(nested_counterpart: Boolean = false): String = {
      if (nested_counterpart && isComplex) {
        s"{meta::pure::profiles::doc.doc = 'JSON wrapper for field [$name]'} $name: String$cardinality;"
      } else {
        s"{meta::pure::profiles::doc.doc = '$description'} $name: ${pureType.pureType}$cardinality;"
      }
    }

    def toRelational: String = {
      s"$name ${pureType.pureRelationalType}"
    }

    def toPrimaryKey(namespace: String, databaseName: String, tableName: String):String = {
      s"[$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName.$name"
    }

    def toMapping(namespace: String, databaseName: String, tableName: String): String = {
      s"$name: [$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName.$name"
    }

  }

  case class PureClass(entityFQN: String, fields: Array[PureField], isNested: Boolean = false) {

    val tableName: String = entityFQN.split("::").last
    val hasNested: Boolean = fields.exists(_.isComplex)

    def getNestedCompanion: String = {
      val xs = entityFQN.split("::")
      xs.dropRight(1).mkString("::") + s"::$NAMESPACE_nested::" + xs.last
    }

    def toPure: String = {
      val coreClass = s"""Class $entityFQN
         |{
         |  ${fields.map(_.toPure()).mkString("\n  ")}
         |}
         |""".stripMargin
      if(hasNested) {
        coreClass + s"""\nClass $getNestedCompanion
                       |{
                       |  ${fields.map(_.toPure(true)).mkString("\n  ")}
                       |}
                       |""".stripMargin
      } else coreClass
    }

    def toRelational: String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"""    Table $tableName
         |    (
         |      ${fields.map(_.toRelational).mkString(",\n      ")}
         |    )""".stripMargin
    }

    def getMappingName(namespace: String): String = {
      if (hasNested) s"$namespace::$NAMESPACE_mapping::$NAMESPACE_nested::$tableName" else s"$namespace::$NAMESPACE_mapping::$tableName"
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
      val mappingName = getMappingName(namespace)
      s"""Mapping $mappingName
         |(
         |  *${if (hasNested) getNestedCompanion else entityFQN}: Relational
         |  {
         |    ~primaryKey
         |    (
         |      ${fields.map(_.toPrimaryKey(namespace, databaseName, tableName)).mkString(",\n      ")}
         |    )
         |    ~mainTable [$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName
         |    ${fields.map(_.toMapping(namespace, databaseName, tableName)).mkString(",\n    ")}
         |  }
         |)
         |""".stripMargin
    }
  }

  case class PureModel(databaseName: String, pureTables: Array[PureClass]) {
    def toPure(namespace: String): String = {
      s"""###Pure
         |${pureTables.map(_.toPure).mkString("\n")}
         |###Relational
         |Database $namespace::$NAMESPACE_connect::DatabricksSchema
         |(
         |  Schema $databaseName
         |  (
         |${pureTables.filter(!_.isNested).map(_.toRelational).mkString("\n")}
         |  )
         |)
         |
         |###Mapping
         |${pureTables.filter(!_.isNested).map(_.toMapping(namespace, databaseName)).mkString("\n")}
         |###Connection
         |RelationalDatabaseConnection $namespace::$NAMESPACE_connect::DatabricksJDBCConnection
         |{
         |  store: $namespace::$NAMESPACE_connect::DatabricksSchema;
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
         |Runtime $namespace::$NAMESPACE_connect::DatabricksRuntime
         |{
         |  mappings:
         |  [
         |    ${pureTables.filter(!_.isNested).map(_.getMappingName(namespace)).mkString(",\n    ")}
         |  ];
         |  connections:
         |  [
         |    $namespace::$NAMESPACE_connect::DatabricksSchema:
         |    [
         |      environment: $namespace::$NAMESPACE_connect::DatabricksJDBCConnection
         |    ]
         |  ];
         |}
         |""".stripMargin
    }
  }
}