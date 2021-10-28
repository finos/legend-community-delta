package org.finos.legend.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

case class Transform(
                      srcEntity: String,
                      srcSchema: StructType,
                      srcExpectations: Seq[(String, String)],
                      transformations: Seq[(String, String)],
                      constraints: Seq[(String, String)],
                      dstTable: String
                    ) {

    def transform(df: DataFrame): DataFrame = {
      transformations.foldLeft(df)((df, field) => df.withColumnRenamed(field._1, field._2))
    }
}
