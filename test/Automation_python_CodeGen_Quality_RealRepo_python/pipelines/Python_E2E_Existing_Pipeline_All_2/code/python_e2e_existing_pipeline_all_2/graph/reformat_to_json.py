from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def reformat_to_json(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id_final"), 
        col("account_flags"), 
        col("country_code"), 
        col("var_col_name"), 
        col("var_col_value"), 
        to_json(
            struct(
              col("customer_id_final"), 
              col("account_flags"), 
              col("country_code"), 
              col("var_col_name"), 
              col("var_col_value")
            )
          )\
          .alias("json_as_string")
    )
