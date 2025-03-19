from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.columns
    selectCols = [col("customer_id") if "customer_id" in flt_col else col("customer_id"),                   col("first_name") if "first_name" in flt_col else col("first_name"),                   col("last_name") if "last_name" in flt_col else col("last_name"),                   col("phone") if "phone" in flt_col else col("phone"),                   col("email") if "email" in flt_col else col("email"),                   col("country_code") if "country_code" in flt_col else col("country_code"),                   col("account_open_date") if "account_open_date" in flt_col else col("account_open_date"),                   col("account_flags") if "account_flags" in flt_col else col("account_flags")]

    return in0.select(*selectCols)
