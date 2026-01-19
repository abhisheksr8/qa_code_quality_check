from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def RowDistributor_1(spark: SparkSession, in0: DataFrame) -> (DataFrame, DataFrame):
    df1 = in0.filter((col("customer_id") > lit(2)))
    df2 = in0.filter((col("customer_id") > lit(4)))

    return df1, df2
