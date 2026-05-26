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

object Deduplicate_1 {

  case class DeduplicateProperties(
    columnsSelector: List[String] = Nil,
    dedupType:       String = "any",
    dedupColumns:    List[StringColName] = Nil,
    useOrderBy:      Option[Boolean] = Some(false),
    orders:          Option[List[OrderByRule]] = Some(Nil)
  )

  case class StringColName(colName: String)

  case class OrderByRule(
    expression: org.apache.spark.sql.Column,
    sortType:   String,
    nullsType:  Option[String] = None
  )

  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark  = context.spark
    val Config = context.config
    val props = DeduplicateProperties(
      columnsSelector =
        List("0FVDXR5o3WF7EKpc9UTYg$$EshYytlEPVmjcEeqzAgx2##last_name"),
      dedupType = "any",
      dedupColumns = List(StringColName(colName = "last_name")),
      useOrderBy = Some(false),
      orders = Some(List())
    )
    val df =
      if (props.dedupType == "distinct")
        in.distinct()
      else {
        import org.apache.spark.sql.expressions.Window
        val typeToKeep     = props.dedupType
        val groupByColumns = props.dedupColumns.map(_.colName)
        val orderRules = if (props.useOrderBy.contains(true)) {
          props.orders.get.map(x =>
            x.nullsType match {
              case Some("nulls_first") =>
                x.sortType match {
                  case "asc" =>
                    x.expression.asc_nulls_first
                  case _ =>
                    x.expression.desc_nulls_first
                }
              case Some("nulls_last") =>
                x.sortType match {
                  case "asc" =>
                    x.expression.asc_nulls_last
                  case _ =>
                    x.expression.desc_nulls_last
                }
              case None =>
                x.sortType match {
                  case "asc" =>
                    x.expression.asc
                  case _ =>
                    x.expression.desc
                }
            }
          )
        } else
          List(lit(1))
        val window = Window
          .partitionBy(groupByColumns.head, groupByColumns.tail: _*)
          .orderBy(orderRules: _*)
        val windowForCount =
          Window.partitionBy(groupByColumns.head, groupByColumns.tail: _*)
        typeToKeep match {
          case "any" =>
            val columns =
              if (groupByColumns.isEmpty) in.columns.toList else groupByColumns
            in.dropDuplicates(columns)
          case "first" =>
            val dataFrameWithRowNumber =
              in.withColumn("row_number", row_number().over(window))
            dataFrameWithRowNumber
              .filter(col("row_number") === lit(1))
              .drop("row_number")
          case "last" =>
            val dataFrameWithRowNumber = in
              .withColumn("row_number", row_number().over(window))
              .withColumn("count",      count("*").over(windowForCount))
            dataFrameWithRowNumber
              .filter(col("row_number") === col("count"))
              .drop("row_number")
              .drop("count")
          case "unique_only" =>
            val dataFrameWithCount =
              in.withColumn("count", count("*").over(windowForCount))
            dataFrameWithCount.filter(col("count") === lit(1)).drop("count")
        }
      }
    df
  }

}
