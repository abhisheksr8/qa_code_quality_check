from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def qa_database_customers(spark: SparkSession) -> DataFrame:
    return spark.read.table("`hive_metastore`.`qa_database`.`customers`")
