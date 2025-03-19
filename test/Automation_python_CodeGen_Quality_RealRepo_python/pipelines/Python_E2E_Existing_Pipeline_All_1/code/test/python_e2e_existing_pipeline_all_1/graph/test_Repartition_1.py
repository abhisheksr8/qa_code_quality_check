from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from python_e2e_existing_pipeline_all_1.graph.Repartition_1 import *
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *


class Repartition_1Test(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Repartition_1/in0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Repartition_1/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Repartition_1/out/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Repartition_1/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = Repartition_1(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "customer_id",
              "first_name",
              "last_name",
              "phone",
              "email",
              "country_code",
              "account_open_date",
              "account_flags"
            ),
            dfOutComputed.select(
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
        dfgraph_Lookup_1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Lookup_1/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/Lookup_1/data.json',
            "in0"
        )
        from python_e2e_existing_pipeline_all_1.graph.Lookup_1 import Lookup_1
        Lookup_1(self.spark, dfgraph_Lookup_1)
