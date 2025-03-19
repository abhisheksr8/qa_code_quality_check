from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "customer_id",
          row_number()\
            .over(Window\
            .partitionBy(col("customer_id"), col("first_name"))\
            .orderBy(col("phone").asc(), col("email").asc())\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .withColumn(
          "country_code",
          row_number()\
            .over(Window\
            .partitionBy(col("customer_id"), col("first_name"))\
            .orderBy(col("phone").asc(), col("email").asc())\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .withColumn("account_open_date", row_number()\
        .over(Window\
        .partitionBy(col("customer_id"), col("first_name"))\
        .orderBy(col("phone").asc(), col("email").asc())\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
