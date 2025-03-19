from python_e2e_existing_pipeline_all_1.graph.pythonSG.config.Config import SubgraphConfig as pythonSG_Config
from python_e2e_existing_pipeline_all_1.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class C_record(ConfigBase):
    def __init__(self, prophecy_spark=None, cr_double: float=11.0, cr_long: int=22, **kwargs):
        self.cr_double = cr_double
        self.cr_long = cr_long
        pass


class Config(ConfigBase):

    def __init__(
            self,
            c_string: str=None,
            c_int: str=None,
            c_record: dict=None,
            c_array: list=None,
            c_databricks_secrets: dict=None,
            Subgraph_1: dict=None,
            pythonSG: dict=None,
            **kwargs
    ):
        self.spark = None
        self.update(c_string, c_int, c_record, c_array, c_databricks_secrets, Subgraph_1, pythonSG)

    def update(
            self,
            c_string: str="test",
            c_int: str="1",
            c_record: dict={},
            c_array: list=["1", "2"],
            c_databricks_secrets: dict={"providerType" : "Databricks", "secretScope" : "qasecrets_mysql", "secretKey" : "username"},
            Subgraph_1: dict={},
            pythonSG: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.c_string = c_string
        self.c_int = c_int
        self.c_record = self.get_config_object(
            prophecy_spark, 
            C_record(prophecy_spark = prophecy_spark), 
            c_record, 
            C_record
        )
        self.c_array = c_array

        if c_databricks_secrets is not None:
            self.c_databricks_secrets = self.get_secret_config_object(
                prophecy_spark, 
                ConfigBase.SecretValue(prophecy_spark = prophecy_spark), 
                c_databricks_secrets, 
                ConfigBase.SecretValue
            )

        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.pythonSG = self.get_config_object(
            prophecy_spark, 
            pythonSG_Config(prophecy_spark = prophecy_spark), 
            pythonSG, 
            pythonSG_Config
        )
        pass
