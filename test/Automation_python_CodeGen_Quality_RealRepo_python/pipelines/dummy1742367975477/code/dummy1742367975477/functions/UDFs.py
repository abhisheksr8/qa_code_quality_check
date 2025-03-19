from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.lookups import (
    createLookup,
    createRangeLookup,
    lookup,
    lookup_last,
    lookup_match,
    lookup_count,
    lookup_row,
    lookup_row_reverse,
    lookup_nth
)

def registerUDFs(spark: SparkSession):
    spark.udf.register("factorial_udf", factorial_udf)
    spark.udf.register("squared", squared)
    spark.udf.register("udf_prime", udf_prime)
    

    try:
        from prophecy.utils import ScalaUtil
        ScalaUtil.initializeUDFs(spark)
    except :
        pass

def factorial_udfGenerator():
    initial = 10

    @udf(returnType = IntegerType())
    def func(input):
        input = int(input) if input is not None else 2

        return int(input) * int(input) if input is not None else initial

    return func

factorial_udf = factorial_udfGenerator()

def squaredGenerator():
    initial = 10

    @udf(returnType = IntegerType())
    def func(input):
        input = int(input) if input is not None else 2

        return int(input) * int(input) * initial if input is not None else initial

    return func

squared = squaredGenerator()

def udf_primeGenerator():
    initial = 10

    @udf(returnType = BooleanType())
    def func(x):
        if num > 1:
            # check for factors
            for i in range(2, num):
                if (num % i) == 0:
                    return False
            else:
                return True
        else:
            return False

    return func

udf_prime = udf_primeGenerator()
