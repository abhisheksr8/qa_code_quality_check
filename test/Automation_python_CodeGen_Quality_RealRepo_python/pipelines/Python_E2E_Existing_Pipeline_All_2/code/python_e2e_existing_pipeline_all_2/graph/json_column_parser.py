from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *
from python_e2e_existing_pipeline_all_2.udfs.UDFs import *

def json_column_parser(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.libs.utils import json_parse

    return json_parse(in0, "json_as_string", "parseAuto", None, None, 40)
