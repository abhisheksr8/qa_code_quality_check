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

object Aggregate_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("first_name"), col("last_name"))
      .agg(first(col("customer_id")).as("customer_id"),
           first(col("email")).as("email"),
           first(col("country_code")).as("country_code")
      )

}
