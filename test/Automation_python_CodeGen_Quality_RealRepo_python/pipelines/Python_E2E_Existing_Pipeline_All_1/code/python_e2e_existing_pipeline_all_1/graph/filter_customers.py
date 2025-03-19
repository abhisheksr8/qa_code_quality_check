from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *
from python_e2e_existing_pipeline_all_1.udfs.UDFs import *

def filter_customers(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> (DataFrame, DataFrame):

    try:
        registerUDFs(spark)
    except NameError:
        print("registerUDFs not working")

    in0.createOrReplaceTempView("in0")
    in1.createOrReplaceTempView("in1")
    df1 = spark.sql(
        "select * from in0 where in0.phone not like '%sdasdasd%' and in0.customer_id not in (select count(*) from in1)"
    )
    df2 = spark.sql("select * from in0 where customer_id > 20")

    return df1, df2
