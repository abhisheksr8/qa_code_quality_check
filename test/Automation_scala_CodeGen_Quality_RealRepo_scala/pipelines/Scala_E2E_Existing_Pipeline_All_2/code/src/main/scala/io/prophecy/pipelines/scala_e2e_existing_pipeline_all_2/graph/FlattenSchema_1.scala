package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object FlattenSchema_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      if (in.columns.contains("customer_id")) col("customer_id")
      else col("customer_id"),
      if (in.columns.contains("first_name")) col("first_name")
      else col("first_name"),
      if (in.columns.contains("last_name")) col("last_name")
      else col("last_name"),
      if (in.columns.contains("phone")) col("phone") else col("phone"),
      if (in.columns.contains("email")) col("email") else col("email"),
      if (in.columns.contains("country_code")) col("country_code")
      else col("country_code"),
      if (in.columns.contains("account_open_date")) col("account_open_date")
      else col("account_open_date"),
      if (in.columns.contains("account_flags")) col("account_flags")
      else col("account_flags")
    )

}
