from python_e2e_existing_pipeline_all_2.graph.pythonSG.Subgraph_2.config.Config import (
    SubgraphConfig as Subgraph_2_Config
)
from prophecy.config import ConfigBase


class SubgraphConfig(ConfigBase):

    def __init__(self, prophecy_spark=None, Subgraph_2: dict={}, **kwargs):
        self.Subgraph_2 = self.get_config_object(
            prophecy_spark, 
            Subgraph_2_Config(prophecy_spark = prophecy_spark), 
            Subgraph_2, 
            Subgraph_2_Config
        )
        pass

    def update(self, updated_config):
        self.Subgraph_2 = updated_config.Subgraph_2
        pass

Config = SubgraphConfig()
