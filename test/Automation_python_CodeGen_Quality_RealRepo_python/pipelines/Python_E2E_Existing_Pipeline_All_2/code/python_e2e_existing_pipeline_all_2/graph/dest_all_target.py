from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def dest_all_target(spark: SparkSession, in0: DataFrame):
    in0.write.format("parquet").mode("overwrite").save("dbfs:/tmp/e2e/dest_all_target_random_python_candel")
