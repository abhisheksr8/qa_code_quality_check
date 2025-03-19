from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "last_name",
          row_number()\
            .over(Window.partitionBy(col("customer_id"), col("country_code")).orderBy(col("first_name").asc()))
        )\
        .withColumn("phone", row_number()\
        .over(Window.partitionBy(col("customer_id"), col("country_code")).orderBy(col("first_name").asc())))
