package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph.Subgraph_1.config.{
  Config => Subgraph_1_Config
}
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_1.graph.scalaSG.config.{
  Config => scalaSG_Config
}

case class Config(
  c_string: String = "test",
  c_int:    Int = 1,
  c_record: C_record = C_record(),
  c_array:  List[String] = List("1", "2"),
  c_databricks_secrets: DatabricksSecret =
    DatabricksSecret(scope = "qasecrets_mysql", key = "username"),
  Subgraph_1: Subgraph_1_Config = Subgraph_1_Config(),
  scalaSG:    scalaSG_Config = scalaSG_Config()
) extends ConfigBase

object C_record {

  implicit val confHint: ProductHint[C_record] =
    ProductHint[C_record](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C_record(cr_double: Double = 12.0d, cr_long: Long = 22L)

object DatabricksSecret {

  implicit val myIntReader: ConfigReader[DatabricksSecret] =
    ConfigReader[String].map { s =>
      val Array(scope, key) = s.split(":")
      DatabricksSecret(scope, key)
    }

}

case class DatabricksSecret(scope: String, key: String) {

  override def toString: String = {
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    dbutils.secrets.get(scope = scope, key = key)
  }

}
