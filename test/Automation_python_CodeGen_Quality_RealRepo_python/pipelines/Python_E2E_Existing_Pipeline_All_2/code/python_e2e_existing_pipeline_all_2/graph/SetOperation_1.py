from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def SetOperation_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    nonEmptyDf = [x for x in [in0, in1] if x is not None]
    res = nonEmptyDf[0]
    rest = nonEmptyDf[1:]

    for inDF in rest:
        res = res.unionAll(inDF)

    return res
