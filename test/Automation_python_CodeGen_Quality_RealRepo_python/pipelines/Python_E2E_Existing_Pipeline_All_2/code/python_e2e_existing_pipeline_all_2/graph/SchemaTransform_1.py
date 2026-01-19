from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out = in0\
              .withColumn("c_full_name", concat(col("first_name"), col("last_name")))\
              .drop("account_flags")\
              .withColumnRenamed("country_code", "countrys_code")

    if "phone" not in in0.columns:
        out = in0\
                  .withColumn("c_full_name", concat(col("first_name"), col("last_name")))\
                  .drop("account_flags")\
                  .withColumnRenamed("country_code", "countrys_code")\
                  .withColumn("phone", lit(9999999))

    return out
