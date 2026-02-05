package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph

import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.PipelineInitCode._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.udfs.UDFs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object CompareColumns_1 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val joined = in0
      .select(
        col("customer_id"),
        explode_outer(
          map(
            (in0.columns.toSet -- List("customer_id").toSet).toSeq.flatMap(c =>
              List(lit(c), col(c).cast("string"))
            ): _*
          )
        ).as(List("column_name", "##value##"))
      )
      .as("exploded1")
      .join(
        in1
          .select(
            col("customer_id"),
            explode_outer(
              map(
                (in0.columns.toSet -- List("customer_id").toSet).toSeq
                  .flatMap(c => List(lit(c), col(c).cast("string"))): _*
              )
            ).as(List("column_name", "##value##"))
          )
          .as("exploded2"),
        lit(true)
          .and(col("exploded1.column_name") === col("exploded2.column_name"))
          .and(col("exploded1.customer_id") === col("exploded2.customer_id")),
        "full_outer"
      )
      .select(
        coalesce(col("exploded1.column_name"), col("exploded2.column_name"))
          .as("column_name"),
        coalesce(col("exploded1.customer_id"), col("exploded2.customer_id"))
          .as("customer_id"),
        col("exploded1.##value##").as("##left_value##"),
        col("exploded2.##value##").as("##right_value##")
      )
      .withColumn(
        "match_count",
        when(
          coalesce(col("##left_value##") === col("##right_value##"),
                   col("##left_value##").isNull && col("##right_value##").isNull
          ),
          lit(1)
        ).otherwise(lit(0))
      )
      .withColumn(
        "mismatch_count",
        when(coalesce(
               col("##left_value##") =!= col("##right_value##"),
               !(col("##left_value##").isNull && col("##right_value##").isNull)
             ),
             lit(1)
        ).otherwise(lit(0))
      )
    joined
      .drop("##left_value##")
      .drop("##right_value##")
      .withColumn("mismatch_example_left",  lit(null))
      .withColumn("mismatch_example_right", lit(null))
      .union(
        joined
          .filter(col("mismatch_count").gt(lit(0)))
          .withColumn("##row_number###",
                      row_number.over(
                        Window
                          .partitionBy(col("column_name"), col("customer_id"))
                          .orderBy(col("customer_id"))
                      )
          )
          .filter(col("##row_number###") === lit(1))
          .select(
            col("column_name"),
            col("customer_id"),
            lit(0).as("match_count"),
            lit(0).as("mismatch_count"),
            col("##left_value##").as("mismatch_example_left"),
            col("##right_value##").as("mismatch_example_right")
          )
          .dropDuplicates("column_name")
      )
      .groupBy("column_name")
      .agg(
        sum("match_count").as("match_count"),
        sum("mismatch_count").as("mismatch_count"),
        first(col("mismatch_example_left"), ignoreNulls = true)
          .as("mismatch_example_left"),
        first(col("mismatch_example_right"), ignoreNulls = true)
          .as("mismatch_example_right"),
        first(when(coalesce(col("mismatch_example_left"),
                            col("mismatch_example_right")
                   ).isNotNull,
                   col("customer_id")
              ).otherwise(lit(null)),
              ignoreNulls = true
        ).as("mismatch_example_customer_id")
      )
      .orderBy(col("mismatch_count").desc, col("column_name"))
  }

}
