package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dataset_cust_in_1 = dataset_cust_in_1(context)
    Lookup_1(context, df_dataset_cust_in_1)
    val df_customer      = customer(context)
    val df_Reformat_4    = Reformat_4(context,    df_customer)
    val df_Join_1        = Join_1(context,        df_Reformat_4, df_Reformat_4)
    val df_Repartition_1 = Repartition_1(context, df_Join_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) =
      RowDistributor_1(context, df_Repartition_1)
    val df_src_parquet_all_type_no_partition =
      src_parquet_all_type_no_partition(context)
    val df_FlattenSchema_1 =
      FlattenSchema_1(context, df_src_parquet_all_type_no_partition)
    val df_Limit_2 = Limit_2(context, df_FlattenSchema_1)
    val df_bulk_column_prefix_rename =
      bulk_column_prefix_rename(context, df_customer)
    val df_add_suffix_to_columns =
      add_suffix_to_columns(context, df_bulk_column_prefix_rename)
    val df_dataset_cust_in  = dataset_cust_in(context)
    val df_Reformat_1       = Reformat_1(context,       df_dataset_cust_in)
    val df_WindowFunction_1 = WindowFunction_1(context, df_Limit_2)
    val df_Aggregate_1      = Aggregate_1(context,      df_WindowFunction_1)
    val df_Reformat_2 =
      if (context.config.c_int == 1000)
        Reformat_2(context, df_Reformat_1)
      else null
    val df_Reformat_2_1 =
      if (context.config.c_int == 1000) Reformat_2_1(context, df_Reformat_1)
      else df_Reformat_1
    val (df_Subgraph_1_out0, df_Subgraph_1_out1) = Subgraph_1.apply(
      Subgraph_1.config.Context(context.spark, context.config.Subgraph_1),
      df_Reformat_1
    )
    dest_scala_1(context, df_Subgraph_1_out0)
    val (df_scalaSG_out0, df_scalaSG_out1) = scalaSG.apply(
      scalaSG.config.Context(context.spark, context.config.scalaSG),
      df_Reformat_1
    )
    val df_SetOperation_1 =
      SetOperation_1(context, df_Subgraph_1_out1, df_scalaSG_out0)
    val df_Script_1  = Script_1(context,  df_SetOperation_1)
    val df_OrderBy_1 = OrderBy_1(context, df_Script_1)
    val df_Filter_1  = Filter_1(context,  df_WindowFunction_1)
    val (df_SQLStatement_1_out, df_SQLStatement_1_out1) =
      SQLStatement_1(context, df_RowDistributor_1_out0)
    val df_Reformat_3      = Reformat_3(context,      df_dataset_cust_in)
    val df_Deduplicate_1_1 = Deduplicate_1_1(context, df_Reformat_2_1)
    val df_SchemaTransform_1 = if (context.config.c_int == 1000) {
      val df_Deduplicate_1 = Deduplicate_1(context, df_Reformat_2)
      SchemaTransform_1(context, df_Deduplicate_1)
    } else
      null
    val df_CompareColumns_1 =
      if (context.config.c_int == 1000)
        CompareColumns_1(context, df_SchemaTransform_1, df_SchemaTransform_1)
      else null
    val df_Limit_1 = Limit_1(context, df_scalaSG_out1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/Scala_E2E_Existing_Pipeline_All_1"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/Scala_E2E_Existing_Pipeline_All_1"
    ) {
      apply(context)
    }
  }

}
