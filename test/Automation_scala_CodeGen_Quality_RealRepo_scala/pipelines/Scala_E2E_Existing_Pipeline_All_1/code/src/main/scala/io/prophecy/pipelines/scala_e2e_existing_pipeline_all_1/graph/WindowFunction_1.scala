package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object WindowFunction_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
        "c_short",
        row_number().over(
          Window
            .partitionBy(col("c_int"), col("c_long"))
            .orderBy(col("c_float").asc,
                     col("c_string").desc,
                     col("c_string1").asc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
      .withColumn(
        "c_boolean",
        row_number().over(
          Window
            .partitionBy(col("c_int"), col("c_long"))
            .orderBy(col("c_float").asc,
                     col("c_string").desc,
                     col("c_string1").asc
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
      )
  }

}
