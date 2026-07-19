package io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.scala_e2e_existing_pipeline_all_2.graph.scalaSG.config.{
  Config => scalaSG_Config
}

case class Config(
  scalaSG:  scalaSG_Config = scalaSG_Config(),
  c_string: String = "test"
) extends ConfigBase
