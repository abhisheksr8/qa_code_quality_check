from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
