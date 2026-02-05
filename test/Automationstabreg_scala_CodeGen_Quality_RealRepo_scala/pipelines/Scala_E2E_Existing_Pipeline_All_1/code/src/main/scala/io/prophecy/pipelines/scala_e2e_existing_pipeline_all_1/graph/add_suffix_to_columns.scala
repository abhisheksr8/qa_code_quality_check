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

object add_suffix_to_columns {

  def apply(context: Context, in: DataFrame): DataFrame = {
    var allExpressions = List(
      expr(
        "concat(column_name, 'test_value')"
          .replace("column_value", "`pre_C_CUSTOMER_ID`")
          .replace("column_name",  "'pre_C_CUSTOMER_ID'")
      ).as("pre_C_CUSTOMER_ID_suff").cast(StringType),
      expr(
        "concat(column_name, 'test_value')"
          .replace("column_value", "`C_PREFERRED_CUST_FLAG`")
          .replace("column_name",  "'C_PREFERRED_CUST_FLAG'")
      ).as("C_PREFERRED_CUST_FLAG_suff").cast(StringType),
      expr(
        "concat(column_name, 'test_value')"
          .replace("column_value", "`C_BIRTH_COUNTRY`")
          .replace("column_name",  "'C_BIRTH_COUNTRY'")
      ).as("C_BIRTH_COUNTRY_suff").cast(StringType),
      expr(
        "concat(column_name, 'test_value')"
          .replace("column_value", "`C_LAST_NAME`")
          .replace("column_name",  "'C_LAST_NAME'")
      ).as("C_LAST_NAME_suff").cast(StringType),
      expr(
        "concat(column_name, 'test_value')"
          .replace("column_value", "`C_FIRST_NAME`")
          .replace("column_name",  "'C_FIRST_NAME'")
      ).as("C_FIRST_NAME_suff").cast(StringType)
    ) ++ (in.columns.toSet -- List("pre_C_CUSTOMER_ID",
                                   "C_PREFERRED_CUST_FLAG",
                                   "C_BIRTH_COUNTRY",
                                   "C_LAST_NAME",
                                   "C_FIRST_NAME"
    ).toSet).map(columnName => col("`" + columnName + "`"))
    allExpressions = allExpressions ++ List(col("`pre_C_CUSTOMER_ID`"),
                                            col("`C_PREFERRED_CUST_FLAG`"),
                                            col("`C_BIRTH_COUNTRY`"),
                                            col("`C_LAST_NAME`"),
                                            col("`C_FIRST_NAME`")
    )
    in.select(allExpressions: _*)
  }

}
