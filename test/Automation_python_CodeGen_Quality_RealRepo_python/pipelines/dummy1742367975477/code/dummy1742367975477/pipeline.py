from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dummy1742367975477.config.ConfigStore import *
from dummy1742367975477.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("dummy1742367975477").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/dummy1742367975477")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/dummy1742367975477", config = Config)(pipeline)

if __name__ == "__main__":
    main()
