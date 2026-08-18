package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.config._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_dataset_cust_in = dataset_cust_in(context)
    Lookup_1(context, df_dataset_cust_in)
    val df_Reformat_1       = Reformat_1(context,       df_dataset_cust_in)
    val df_Filter_1         = Filter_1(context,         df_Reformat_1)
    val df_Deduplicate_1    = Deduplicate_1(context,    df_Filter_1)
    val df_Reformat_2       = Reformat_2(context,       df_dataset_cust_in).cache()
    val df_WindowFunction_1 = WindowFunction_1(context, df_Reformat_2)
    val df_CompareColumns_1 =
      CompareColumns_1(context, df_WindowFunction_1, df_WindowFunction_1)
    val df_Reformat_4 = if (context.config.c_string == "arasdnas123n1l23asd") {
      val df_Reformat_3 =
        if (context.config.c_string == "arasdnas123n1l23asd")
          Reformat_3(context, df_CompareColumns_1)
        else df_CompareColumns_1
      Reformat_4(context, df_Reformat_3)
    } else
      null
    val (df_scalaSG_out0, df_scalaSG_out1) = scalaSG.apply(
      scalaSG.config.Context(context.spark, context.config.scalaSG),
      df_dataset_cust_in
    )
    val df_Aggregate_1          = Aggregate_1(context,   df_WindowFunction_1)
    val df_OrderBy_1            = OrderBy_1(context,     df_Aggregate_1).cache()
    val df_Join_1               = Join_1(context,        df_OrderBy_1, df_OrderBy_1)
    val df_Repartition_1        = Repartition_1(context, df_Join_1).cache()
    val df_qa_database_customer = qa_database_customer(context)
    val df_bulk_column_prefix_rename =
      bulk_column_prefix_rename(context, df_qa_database_customer)
    val df_add_suffix_to_columns =
      add_suffix_to_columns(context, df_bulk_column_prefix_rename)
    val df_FlattenSchema_1 = FlattenSchema_1(context, df_Deduplicate_1)
    val df_SetOperation_1 =
      SetOperation_1(context, df_FlattenSchema_1, df_FlattenSchema_1)
    val df_Limit_1           = Limit_1(context,           df_SetOperation_1)
    val df_SchemaTransform_1 = SchemaTransform_1(context, df_Limit_1)
    val (df_RowDistributor_1_out0, df_RowDistributor_1_out1) = {
      val (df_RowDistributor_1_out0_temp, df_RowDistributor_1_out1_temp) =
        RowDistributor_1(context, df_Repartition_1)
      (df_RowDistributor_1_out0_temp.cache(),
       df_RowDistributor_1_out1_temp.cache()
      )
    }
    val df_SQLStatement_1 = SQLStatement_1(context, df_RowDistributor_1_out1)
    val df_Script_1       = Script_1(context,       df_SchemaTransform_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/Scala_E2E_Existing_Pipeline_All_2"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/Scala_E2E_Existing_Pipeline_All_2"
    ) {
      apply(context)
    }
  }

}
