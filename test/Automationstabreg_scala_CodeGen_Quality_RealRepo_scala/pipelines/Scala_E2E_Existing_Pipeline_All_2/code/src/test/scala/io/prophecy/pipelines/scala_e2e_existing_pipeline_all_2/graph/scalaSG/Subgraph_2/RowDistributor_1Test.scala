package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph.scalaSG.Subgraph_2

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.config._
import io.prophecy.libs.registerAllUDFs
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import java.nio.file.{Files, Paths}
import java.math.BigDecimal

@RunWith(classOf[JUnitRunner])
class RowDistributor_1Test extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._
  var context: Context = null

  test("Unit Test ") {

    val dfIn = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/in/schema.json",
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/in/data/unit_test_.json",
      "in"
    )
    val dfOut0 = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/out0/schema.json",
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/out0/data/unit_test_.json",
      "out0"
    )
    val dfOut1 = createDfFromResourceFiles(
      spark,
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/out1/schema.json",
      "/data/io/prophecy/pipelines/scala_e2e_existing_pipeline_all_2/graph/scalaSG/Subgraph_2/RowDistributor_1/out1/data/unit_test_.json",
      "out1"
    )

    val (dfOut0Computed, dfOut1Computed) =
      io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph.scalaSG.Subgraph_2
        .RowDistributor_1(
          io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph.scalaSG.Subgraph_2.config
            .Context(context.spark, context.config.scalaSG.Subgraph_2),
          dfIn
        )
    val resOut0 = assertDFEquals(
      dfOut0.select("customer_id",
                    "first_name",
                    "last_name",
                    "phone",
                    "email",
                    "country_code",
                    "account_open_date",
                    "account_flags"
      ),
      dfOut0Computed.select("customer_id",
                            "first_name",
                            "last_name",
                            "phone",
                            "email",
                            "country_code",
                            "account_open_date",
                            "account_flags"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msgOut0 = if (resOut0.isLeft) resOut0.left.get.getMessage else ""
    Assert.assertTrue(msgOut0, resOut0.isRight)
    val resOut1 = assertDFEquals(
      dfOut1.select("customer_id",
                    "first_name",
                    "last_name",
                    "phone",
                    "email",
                    "country_code",
                    "account_open_date",
                    "account_flags"
      ),
      dfOut1Computed.select("customer_id",
                            "first_name",
                            "last_name",
                            "phone",
                            "email",
                            "country_code",
                            "account_open_date",
                            "account_flags"
      ),
      maxUnequalRowsToShow,
      1.0
    )
    val msgOut1 = if (resOut1.isLeft) resOut1.left.get.getMessage else ""
    Assert.assertTrue(msgOut1, resOut1.isRight)
  }

  override def beforeAll() = {
    super.beforeAll()
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerAllUDFs(spark)

    val fabricName = System.getProperty("fabric", "default")
    val confFilePath = Paths
      .get(getClass.getResource(s"/config/${fabricName}.json").toURI)
      .toString

    val config =
      ConfigurationFactoryImpl.getConfig(Array("--confFile", confFilePath))

    context = Context(spark, config)
  }

}
