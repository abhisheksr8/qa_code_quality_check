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

object Join_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.C_BIRTH_YEAR") === col("in1.C_BIRTH_YEAR"),
            "inner"
      )
      .select(
        col("in0.C_CUSTOMER_SK").as("C_CUSTOMER_SK"),
        col("in1.C_CUSTOMER_ID").as("C_CUSTOMER_ID"),
        col("in0.C_CURRENT_CDEMO_SK").as("C_CURRENT_CDEMO_SK"),
        col("in0.C_CURRENT_HDEMO_SK").as("C_CURRENT_HDEMO_SK"),
        col("in0.C_CURRENT_ADDR_SK").as("C_CURRENT_ADDR_SK"),
        col("in0.C_FIRST_SHIPTO_DATE_SK").as("C_FIRST_SHIPTO_DATE_SK"),
        col("in0.C_FIRST_SALES_DATE_SK").as("C_FIRST_SALES_DATE_SK"),
        col("in0.C_SALUTATION").as("C_SALUTATION"),
        col("in0.C_FIRST_NAME").as("C_FIRST_NAME"),
        col("in0.C_LAST_NAME").as("C_LAST_NAME"),
        col("in0.C_PREFERRED_CUST_FLAG").as("C_PREFERRED_CUST_FLAG"),
        col("in0.C_BIRTH_DAY").as("C_BIRTH_DAY"),
        col("in0.C_BIRTH_MONTH").as("C_BIRTH_MONTH"),
        col("in0.C_BIRTH_YEAR").as("C_BIRTH_YEAR"),
        col("in0.C_BIRTH_COUNTRY").as("C_BIRTH_COUNTRY"),
        col("in0.C_LOGIN").as("C_LOGIN"),
        col("in0.C_EMAIL_ADDRESS").as("C_EMAIL_ADDRESS"),
        col("in0.C_LAST_REVIEW_DATE").as("C_LAST_REVIEW_DATE")
      )

}
