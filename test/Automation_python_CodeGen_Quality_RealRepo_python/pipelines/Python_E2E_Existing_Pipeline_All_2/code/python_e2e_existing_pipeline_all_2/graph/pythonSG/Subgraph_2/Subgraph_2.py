from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *
from . import *
from .config import *

def Subgraph_2(spark: SparkSession, subgraph_config: SubgraphConfig, in0: DataFrame) -> (DataFrame, DataFrame):
    Config.update(subgraph_config)
    df_RowDistributor_1_out0, df_RowDistributor_1_out1 = RowDistributor_1(spark, in0)
    subgraph_config.update(Config)

    return df_RowDistributor_1_out0, df_RowDistributor_1_out1
