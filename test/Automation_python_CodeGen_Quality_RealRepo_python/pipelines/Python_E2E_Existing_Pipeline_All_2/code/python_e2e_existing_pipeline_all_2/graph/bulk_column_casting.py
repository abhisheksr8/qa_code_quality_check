from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def bulk_column_casting(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        *(
          [expr("cast('customer_id' as int)").alias("customer_id_final").cast(IntegerType())]
          + [col("`" + colName + "`") for colName in sorted(set(in0.columns) - {"customer_id"})]
          + [expr("`customer_id`")]
        )
    )
