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

object Hashing_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{concat, sha1}
    in.withColumn(
      "c_hashed",
      sha1(
        concat(
          col("C_CUSTOMER_SK"),
          col("C_CUSTOMER_ID"),
          col("C_CURRENT_CDEMO_SK"),
          col("C_CURRENT_HDEMO_SK"),
          col("C_CURRENT_ADDR_SK"),
          col("C_FIRST_SHIPTO_DATE_SK"),
          col("C_FIRST_SALES_DATE_SK"),
          col("C_SALUTATION"),
          col("C_FIRST_NAME"),
          col("C_LAST_NAME"),
          col("C_PREFERRED_CUST_FLAG"),
          col("C_BIRTH_DAY"),
          col("C_BIRTH_MONTH"),
          col("C_BIRTH_YEAR"),
          col("C_BIRTH_COUNTRY"),
          col("C_LOGIN"),
          col("C_EMAIL_ADDRESS"),
          col("C_LAST_REVIEW_DATE")
        )
      )
    )
  }

}
