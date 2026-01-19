from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def unpivot_customer_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.unpivot(
        ["customer_id_final", "account_flags", "country_code"],
        ["account_flags", "first_name", "last_name"],
        "var_col_name",
        "var_col_value"
    )
