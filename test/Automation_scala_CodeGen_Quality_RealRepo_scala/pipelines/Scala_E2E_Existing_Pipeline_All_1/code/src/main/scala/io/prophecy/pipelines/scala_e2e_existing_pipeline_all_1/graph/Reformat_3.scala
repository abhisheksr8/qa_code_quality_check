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

object Reformat_3 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      concat(lit(Config.c_string), lit(Config.c_int)).as("c1"),
      udf_multiply(lit(2)).as("c2"),
      lookup("TestLookup", col("first_name"), col("last_name"))
        .getField("phone")
        .as("c3"),
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("email"),
      col("country_code"),
      col("account_open_date"),
      col("account_flags")
    )
  }

}
