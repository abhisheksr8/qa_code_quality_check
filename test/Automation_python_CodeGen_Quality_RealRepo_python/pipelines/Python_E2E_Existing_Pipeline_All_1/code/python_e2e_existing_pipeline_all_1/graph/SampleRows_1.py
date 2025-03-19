from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def SampleRows_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.sample(withReplacement = False, fraction = 0.5)
