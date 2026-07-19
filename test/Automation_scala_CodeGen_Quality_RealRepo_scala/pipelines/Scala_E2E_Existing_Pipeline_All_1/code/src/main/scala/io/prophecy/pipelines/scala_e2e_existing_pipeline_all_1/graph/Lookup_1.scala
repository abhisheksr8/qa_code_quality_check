package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config.Context
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Lookup_1 {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("TestLookup",
                 in0,
                 context.spark,
                 List("last_name", "first_name"),
                 "phone"
    )

}
