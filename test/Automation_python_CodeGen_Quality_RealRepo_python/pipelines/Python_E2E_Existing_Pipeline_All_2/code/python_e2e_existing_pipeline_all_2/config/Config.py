from python_e2e_existing_pipeline_all_2.graph.pythonSG.config.Config import SubgraphConfig as pythonSG_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, pythonSG: dict=None, c_string: str=None, c_int: int=None, c_float: float=None, **kwargs):
        self.spark = None
        self.update(pythonSG, c_string, c_int, c_float)

    def update(self, pythonSG: dict={}, c_string: str="test", c_int: int=2, c_float: float=3.0, **kwargs):
        prophecy_spark = self.spark
        self.pythonSG = self.get_config_object(
            prophecy_spark, 
            pythonSG_Config(prophecy_spark = prophecy_spark), 
            pythonSG, 
            pythonSG_Config
        )
        self.c_string = c_string
        self.c_int = self.get_int_value(c_int)
        self.c_float = self.get_float_value(c_float)
        pass
