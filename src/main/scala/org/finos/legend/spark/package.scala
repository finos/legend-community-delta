/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2021 Databricks - see NOTICE.md file
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

package org.finos.legend

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.types._


package object spark {

  case class Filter(
                     name: String,
                     sql: String
                   )

  case class Transformation(
                                from: String,
                                to: String
                              )

  case class TransformStrategy(
                                schema: StructType,
                                filters: Seq[Filter],
                                transformations: Seq[Transformation],
                                constraints: Seq[Filter],
                                table: String
                      ) {

    def execute(dr: DataFrameReader, inputPath: String, outputPath: String): Unit = {

      // Load and schematize
      val rawDF = dr.schema(schema).load(inputPath)

      // Apply technical expectations
      val filterDF = filters.foldLeft(rawDF)((df, f) => df.filter(f.sql))

      // Transform as table
      val transformDF = transformations.foldLeft(filterDF)((df, w) => df.withColumnRenamed(w.from, w.to))

      // Run business expectations
      val goldDF = constraints.foldLeft(transformDF)((df, f) => df.filter(f.sql))

      // Persist change
      goldDF.write.format("delta").save(outputPath)

    }

  }

}
