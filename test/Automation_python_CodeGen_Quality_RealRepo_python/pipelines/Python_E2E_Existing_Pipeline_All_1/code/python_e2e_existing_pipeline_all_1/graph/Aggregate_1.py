from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("first_name"), col("last_name"))
    df2 = df1.pivot("customer_lifetime_value", ["concat(first_name, last_name)"])

    return df2.agg(
        first(col("most_recent_order")).alias("most_recent_order"), 
        first(col("number_of_orders")).alias("number_of_orders")
    )
