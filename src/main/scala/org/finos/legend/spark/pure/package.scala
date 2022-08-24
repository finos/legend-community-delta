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

  val NAMESPACE_default: String = "org::finos::legend"
  val NAMESPACE_mapping: String = "mapping"
  val NAMESPACE_classes: String = "classes"

  implicit class StringImpl(string: String) {
    def camelCaseEntity: String = string.split("_").map(_.capitalize).mkString("")
    def camelCaseField: String = {
      val capitalized = string.camelCaseEntity
      val xs = capitalized.toCharArray.map(_.toString)
      xs.tail.foldLeft(xs.head.toLowerCase)((x1, x2) => s"$x1$x2")
    }
    def isValidNamespace: Boolean = {
      val r = "^[a-z]+(?:\\:\\:[a-z]+)+$".r
      r.findFirstMatchIn(string).isDefined
    }
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

    val fieldName: String = name.camelCaseField

    def toClassField: String = {
      if (description.isDefined) {
        s"{meta::pure::profiles::doc.doc = '$description'} $fieldName: ${pureType.pureType}$cardinality;"
      } else s"$fieldName: ${pureType.pureType}$cardinality;"
    }

    def toClassComplexField: String = {
      require(isComplex, s"Wrapper for nested properties only, [$fieldName] is not a nested object")
      val metadata = s"'JSON wrapper for nested property [$fieldName]'"
      s"{meta::pure::profiles::doc.doc = $metadata} $fieldName: String$cardinality;"
    }

    def toRelationalField: String = {
      s"$name ${pureType.pureRelationalType}"
    }

    def toPrimaryKeyField(namespace: String, databaseName: String, tableName: String): String = {
      s"[$namespace::Store]$databaseName.$tableName.$name"
    }

    def toMappingField(namespace: String, databaseName: String, tableName: String): String = {
      s"$fieldName: [$namespace::Store]$databaseName.$tableName.$name"
    }

  }

  case class PureClass(
                        entityName: String,
                        entityFQN: String,
                        fields: Array[PureField],
                        isNested: Boolean = false
                      ) {

    val hasNested: Boolean = fields.exists(_.isComplex)

    def toClass: String = {
      if (hasNested) {
        val baseFields = fields.filter(!_.isComplex)
        val nestedFields = fields.filter(_.isComplex)
        s"""Class ${entityFQN}Base
           |{
           |  ${baseFields.map(_.toClassField).mkString("\n  ")}
           |}
           |
           |Class $entityFQN extends ${entityFQN}Base
           |{
           |  ${nestedFields.map(_.toClassField).mkString("\n  ")}
           |}
           |
           |Class ${entityFQN}Serializable extends ${entityFQN}Base
           |{
           |  ${nestedFields.map(_.toClassComplexField).mkString("\n  ")}
           |}
           |""".stripMargin
      } else {
        val baseFields = fields.filter(!_.isComplex)
        s"""Class $entityFQN
           |{
           |  ${baseFields.map(_.toClassField).mkString("\n  ")}
           |}
           |""".stripMargin
      }
    }

    def toRelationalTable: String = {
      require(!isNested, "Nested entities cannot be mapped to relational objects on legend")
      s"""    Table $entityName
         |    (
         |      ${fields.map(_.toRelationalField).mkString(",\n      ")}
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
      val entityFQNReference = if (hasNested) s"${entityFQN}Serializable" else entityFQN
      val mappingName = getMappingName(namespace)
      s"""Mapping $mappingName
         |(
         |  *$entityFQNReference: Relational
         |  {
         |    ~primaryKey
         |    (
         |      ${fields.map(_.toPrimaryKeyField(namespace, databaseName, entityName)).mkString(",\n      ")}
         |    )
         |    ~mainTable [$namespace::Store]$databaseName.$entityName
         |    ${fields.map(_.toMappingField(namespace, databaseName, entityName)).mkString(",\n    ")}
         |  }
         |)
         |""".stripMargin
    }

    def getMappingName(namespace: String): String = {
      s"$namespace::$NAMESPACE_mapping::${entityName.camelCaseEntity}"
    }
  }

  case class PureModel(databaseName: String, pureTables: Array[PureClass]) {
    def toPure(namespace: String): String = {
      s"""###Pure
         |${pureTables.map(_.toClass).mkString("\n")}
         |###Mapping
         |${pureTables.filter(!_.isNested).map(_.toMapping(namespace, databaseName)).mkString("\n")}
         |###Relational
         |Database $namespace::Store
         |(
         |  Schema $databaseName
         |  (
         |${pureTables.filter(!_.isNested).map(_.toRelationalTable).mkString("\n")}
         |  )
         |)
         |""".stripMargin
    }
  }
}
