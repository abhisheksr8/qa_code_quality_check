from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.columns
    selectCols = [col("FIRST_NAME") if "FIRST_NAME" in flt_col else col("FIRST_NAME"),                   col("LAST_NAME") if "LAST_NAME" in flt_col else col("LAST_NAME")]

    return in0.select(*selectCols)
