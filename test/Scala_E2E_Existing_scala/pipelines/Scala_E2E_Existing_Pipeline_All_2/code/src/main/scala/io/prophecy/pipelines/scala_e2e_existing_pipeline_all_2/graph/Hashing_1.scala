package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Hashing_1 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{concat, sha1}
    in.withColumn("hashed_value",
                  sha1(concat(concat(col("first_name"), col("last_name"))))
    )
  }

}
