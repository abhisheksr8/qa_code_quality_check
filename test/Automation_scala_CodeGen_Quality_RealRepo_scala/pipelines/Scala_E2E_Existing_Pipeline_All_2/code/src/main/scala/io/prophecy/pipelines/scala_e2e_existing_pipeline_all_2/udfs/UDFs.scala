package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) = {
    spark.udf.register("udf_multiply",      udf_multiply)
    spark.udf.register("udf_string_length", udf_string_length)
    spark.udf.register("udf_random_number", udf_random_number)
    try registerAllUDFs(spark)
    catch {
      case _ => ()
    }
  }

  def udf_multiply = {
    var int_value = 10
    udf((value: Int) => value * int_value)
  }

  def udf_string_length =
    udf((s: String) => s.length)

  def udf_random_number = {
    import org.apache.spark.sql.functions._
    udf(() => Math.random())
  }

}

object PipelineInitCode extends Serializable
