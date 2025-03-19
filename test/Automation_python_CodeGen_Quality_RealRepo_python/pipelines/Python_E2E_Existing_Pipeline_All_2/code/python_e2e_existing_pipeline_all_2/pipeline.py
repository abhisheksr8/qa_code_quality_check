from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *
from prophecy.utils import *
from python_e2e_existing_pipeline_all_2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dataset_cust_in = dataset_cust_in(spark)
    Lookup_1(spark, df_dataset_cust_in)
    df_Reformat_1 = Reformat_1(spark, df_dataset_cust_in)
    df_Deduplicate_1 = Deduplicate_1(spark, df_Reformat_1)
    df_Join_1 = Join_1(spark, df_Deduplicate_1, df_Deduplicate_1)
    df_Script_1 = Script_1(spark, df_Join_1)
    df_pythonSG_out0, df_pythonSG_out1 = pythonSG(spark, Config.pythonSG, df_dataset_cust_in)
    df_bulk_rename_columns = bulk_rename_columns(spark, df_dataset_cust_in)
    df_bulk_column_casting = bulk_column_casting(spark, df_bulk_rename_columns)
    df_unpivot_customer_data = unpivot_customer_data(spark, df_bulk_column_casting)
    df_dynamic_column_selection = dynamic_column_selection(spark, df_unpivot_customer_data)
    df_Reformat_2 = Reformat_2(spark, df_dataset_cust_in)
    df_Reformat_2 = df_Reformat_2.cache()
    df_SetOperation_1 = SetOperation_1(spark, df_Reformat_2, df_Reformat_2)
    df_Repartition_1 = Repartition_1(spark, df_SetOperation_1)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_Repartition_1)
    df_SchemaTransform_1 = df_SchemaTransform_1.cache()
    df_SQLStatement_1 = SQLStatement_1(spark, df_SchemaTransform_1)

    if Config.c_string == "testasdasdasdasdasdasd":
        if (Config.c_string == "testasdasdasdasdasdasd"):
            df_Reformat_3 = Reformat_3(spark, df_SQLStatement_1)
        else:
            df_Reformat_3 = df_SQLStatement_1
    else:
        df_Reformat_3 = None

    df_Filter_1 = Filter_1(spark, df_Deduplicate_1)
    df_Filter_1 = df_Filter_1.cache()
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, df_Filter_1)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = (df_RowDistributor_1_out0.cache(),                                                           df_RowDistributor_1_out1.cache())
    df_Aggregate_1 = Aggregate_1(spark, df_RowDistributor_1_out0)
    df_Filter_3 = Filter_3(spark, df_SQLStatement_1)
    df_SampleRows_1 = SampleRows_1(spark, df_bulk_column_casting)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_RowDistributor_1_out1)
    df_OrderBy_1 = OrderBy_1(spark, df_FlattenSchema_1)
    df_reformat_to_json = reformat_to_json(spark, df_dynamic_column_selection)

    if Config.c_string == "testasdasdasdasdasdasd":
        if (Config.c_string == "testasdasdasdasdasdasd"):
            df_Reformat_3_1 = Reformat_3_1(spark, df_Script_1)
        else:
            df_Reformat_3_1 = df_Script_1
    else:
        df_Reformat_3_1 = None

    df_WindowFunction_1 = WindowFunction_1(spark, df_OrderBy_1)
    df_Filter_2 = Filter_2(spark, df_Script_1)
    df_Limit_1 = Limit_1(spark, df_Aggregate_1)
    dest_all_target(spark, df_Limit_1)
    df_json_column_parser = json_column_parser(spark, df_reformat_to_json)
    df_CompareColumns_1 = CompareColumns_1(spark, df_pythonSG_out0, df_pythonSG_out0)
    df_data_cleansing_operations = data_cleansing_operations(spark, df_bulk_column_casting)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Python_E2E_Existing_Pipeline_All_2")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Python_E2E_Existing_Pipeline_All_2", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
