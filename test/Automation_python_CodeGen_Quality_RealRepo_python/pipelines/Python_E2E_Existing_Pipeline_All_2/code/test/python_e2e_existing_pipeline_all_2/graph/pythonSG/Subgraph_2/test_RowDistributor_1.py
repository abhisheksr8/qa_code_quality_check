from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from python_e2e_existing_pipeline_all_2.graph.pythonSG.Subgraph_2.RowDistributor_1 import *
from python_e2e_existing_pipeline_all_2.config.ConfigStore import *


class RowDistributor_1Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/in0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out0/data/test_unit_test_.json',
            'out0'
        )
        dfOut1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out1/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out1/data/test_unit_test_.json',
            'out1'
        )
        dfOut0Computed, dfOut1Computed = RowDistributor_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut0.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            dfOut0Computed.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            self.maxUnequalRowsToShow
        )
        assertDFEquals(
            dfOut1.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            dfOut1Computed.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            self.maxUnequalRowsToShow
        )

    def test_unit_test__1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/in0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/in0/data/test_unit_test__1.json',
            'in0'
        )
        dfOut1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out1/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out1/data/test_unit_test__1.json',
            'out1'
        )
        dfOut0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_2/graph/pythonSG/Subgraph_2/RowDistributor_1/out0/data/test_unit_test__1.json',
            'out0'
        )
        dfOut0Computed, dfOut1Computed = RowDistributor_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut1.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            dfOut1Computed.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            self.maxUnequalRowsToShow
        )
        assertDFEquals(
            dfOut0.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            dfOut0Computed.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
