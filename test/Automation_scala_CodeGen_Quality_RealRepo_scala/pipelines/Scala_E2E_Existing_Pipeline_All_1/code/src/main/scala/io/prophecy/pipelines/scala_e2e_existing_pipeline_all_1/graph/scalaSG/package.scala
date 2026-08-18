package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph.scalaSG.config._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph.scalaSG.Subgraph_2
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object scalaSG {

  def apply(context: Context, in0: DataFrame): Subgraph2 = {
    val df_Filter_1  = Filter_1(context,  in0)
    val df_OrderBy_1 = OrderBy_1(context, df_Filter_1)
    val (df_Subgraph_2_out0, df_Subgraph_2_out1) = Subgraph_2.apply(
      Subgraph_2.config.Context(context.spark, context.config.Subgraph_2),
      df_OrderBy_1
    )
    (df_Subgraph_2_out0, df_Subgraph_2_out1)
  }

}
