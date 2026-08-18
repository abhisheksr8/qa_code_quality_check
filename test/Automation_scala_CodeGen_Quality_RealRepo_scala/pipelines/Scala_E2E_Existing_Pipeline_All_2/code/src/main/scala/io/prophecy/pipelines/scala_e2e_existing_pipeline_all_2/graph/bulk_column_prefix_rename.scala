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

object bulk_column_prefix_rename {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.ProphecyDataFrame
    ProphecyDataFrame
      .extendedDataFrame(in)
      .evaluate_expression("concat('pre_',column_name)",
                           List("C_CUSTOMER_SK",
                                "C_CUSTOMER_ID",
                                "C_CURRENT_CDEMO_SK",
                                "C_CURRENT_HDEMO_SK"
                           ),
                           context.spark
      )
  }

}
