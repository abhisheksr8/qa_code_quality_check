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

object FlattenSchema_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val flattened = in
      .withColumn("c_array_int",       explode_outer(col("c_array_int")))
      .withColumn("c_array_string",    explode_outer(col("c_array_string")))
      .withColumn("c_array_date",      explode_outer(col("c_array_date")))
      .withColumn("c_array_timestamp", explode_outer(col("c_array_timestamp")))
      .withColumn("c_struct-c_array_int",
                  explode_outer(col("c_struct.c_array_int"))
      )
    flattened.select(
      if (flattened.columns.contains("c_array_int")) col("c_array_int")
      else col("c_array_int"),
      if (flattened.columns.contains("c_array_string")) col("c_array_string")
      else col("c_array_string"),
      if (flattened.columns.contains("c_struct-c_array_int"))
        col("c_struct-c_array_int")
      else col("c_struct.c_array_int").as("c_struct-c_array_int"),
      if (flattened.columns.contains("c_array_date")) col("c_array_date")
      else col("c_array_date"),
      if (flattened.columns.contains("c_array_timestamp"))
        col("c_array_timestamp")
      else col("c_array_timestamp"),
      if (flattened.columns.contains("c_short")) col("c_short")
      else col("c_short"),
      if (flattened.columns.contains("c_int")) col("c_int") else col("c_int"),
      if (flattened.columns.contains("c_long")) col("c_long")
      else col("c_long"),
      if (flattened.columns.contains("c_string")) col("c_string")
      else col("c_string"),
      if (flattened.columns.contains("c_boolean")) col("c_boolean")
      else col("c_boolean"),
      if (flattened.columns.contains("c_float")) col("c_float")
      else col("c_float"),
      if (flattened.columns.contains("c_string1")) col("c_string1")
      else col("c_string").as("c_string1")
    )
  }

}
