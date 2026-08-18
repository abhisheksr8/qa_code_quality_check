package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
