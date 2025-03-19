from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out = in0\
              .withColumn("c_new", concat(col("first_name"), col("last_name")))\
              .drop("c_new")\
              .withColumnRenamed("FIRST_NAME", "first_name_new1")

    if "LAST_NAME" not in in0.columns:
        out = in0\
                  .withColumn("c_new", concat(col("first_name"), col("last_name")))\
                  .drop("c_new")\
                  .withColumnRenamed("FIRST_NAME", "first_name_new1")\
                  .withColumn("LAST_NAME", lit(22))

    return out
