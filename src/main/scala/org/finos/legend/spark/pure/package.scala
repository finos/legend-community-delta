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

  case class PureDatatype(pureType: String, pureRelationalType: String)

  case class PureField(name: String, cardinality: String, pureType: PureDatatype, description: String) {
    def toPure: String = {
      s"  {meta::pure::profiles::doc.doc = '$description'} $name: ${pureType.pureType}$cardinality;"
    }
  }

  case class PureTable(entityName: String, namespace: String, database: Option[String],
                       tableName: String, fields: Array[PureField]) {

    def toPure: String = {
      s"""###Pure
         |Class $entityName
         |{
         |${fields.map(_.toPure).mkString("\n")}
         |}""".stripMargin
    }
  }
}
