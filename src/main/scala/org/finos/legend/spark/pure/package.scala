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

  val NAMESPACE_prefix: String = "com::databricks"
  val NAMESPACE_mapping: String = "mapping"
  val NAMESPACE_classes: String = "classes"
  val NAMESPACE_connect: String = "connect"

  implicit class StringImpl(string: String) {
    def camelCase: String = string.split("_").map(_.capitalize).mkString("")
    def nestedField: String = s"__${string}__"
  }

  case class PureDatatype(
                           pureType: String,
                           pureRelationalType: String
                         )

  case class PureField(
                        name: String,
                        cardinality: String,
                        pureType: PureDatatype,
                        description: Option[String],
                        isComplex: Boolean = false
                      ) {

    def toClass: String = {
      if (description.isDefined) {
        s"{meta::pure::profiles::doc.doc = '$description'} $name: ${pureType.pureType}$cardinality;"
      } else s"$name: ${pureType.pureType}$cardinality;"
    }

    def toPureComplex: String = {
      require(isComplex, s"Wrapper for nested properties only, [$name] is not a nested object")
      s"{meta::pure::profiles::doc.doc = 'JSON wrapper for nested property [$name]'} " +
        s"${name.nestedField}: String$cardinality;"
    }

    def toRelational: String = s"$name ${pureType.pureRelationalType}"

    def toPrimaryKey(namespace: String, databaseName: String, tableName: String):String = {
      s"[$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName.$name"
    }

    def toService: String = {
      if (isComplex) "x|$x." + name.nestedField else "x|$x." + name
    }

    def toServiceName: String = s"'$name'"

    def toMapping(namespace: String, databaseName: String, tableName: String): String = {
      if (isComplex) {
        s"${name.nestedField}: [$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName.$name"
      } else {
        s"$name: [$namespace::$NAMESPACE_connect::DatabricksSchema]$databaseName.$tableName.$name"
      }
    }
  }

  case class PureClass(
                        tableName: String,
                        entityFQN: String,
                        fields: Array[PureField],
                        isNested: Boolean = false
                      ) {

    val hasNested: Boolean = fields.exists(_.isComplex)

    def toService(namespace: String): String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      val entityName = entityFQN.split("::").last
      val projection = s"[${fields.map(_.toService).mkString(",")}], [${fields.map(_.toServiceName).mkString(",")}]"
      s"""Service $namespace::$NAMESPACE_connect::$entityName
         |{
         |  pattern: '/$entityName';
         |  documentation: 'Simple REST Api to query [$entityFQN] entities';
         |  autoActivateUpdates: true;
         |  execution: Single
         |  {
         |    query: |$namespace::$NAMESPACE_classes::$entityName.all()->project($projection);
         |    mapping: $namespace::$NAMESPACE_mapping::$entityName;
         |    runtime: $namespace::$NAMESPACE_connect::DatabricksRuntime;
         |  }
         |}""".stripMargin
    }

    def toClass: String = {
      val pureFields = fields.map(_.toClass) ++ fields.filter(_.isComplex).map(_.toPureComplex)
      s"""Class $entityFQN
         |{
         |  ${pureFields.mkString("\n  ")}
         |}
         |""".stripMargin
    }

    def toRelational: String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"""    Table $tableName
         |    (
         |      ${fields.map(_.toRelational).mkString(",\n      ")}
         |    )""".stripMargin
    }

    def getMappingName(namespace: String): String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"$namespace::$NAMESPACE_mapping::${entityFQN.split("::").last}"
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
         |  *$entityFQN: Relational
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
         |${pureTables.map(_.toClass).mkString("\n")}
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
         |    hostname: 'Databricks hostname';
         |    port: '443';
         |    protocol: 'https';
         |    httpPath: 'Databricks cluster HTTP path';
         |  };
         |  auth: ApiToken
         |  {
         |    apiToken: 'Databricks token reference on legend vault';
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
         |
         |###Service
         |${pureTables.filter(!_.isNested).map(_.toService(namespace)).mkString("\n\n")}
         |""".stripMargin
    }
  }
}
