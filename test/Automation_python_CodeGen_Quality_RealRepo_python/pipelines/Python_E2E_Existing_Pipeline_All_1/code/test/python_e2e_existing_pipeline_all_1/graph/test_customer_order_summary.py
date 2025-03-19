from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from python_e2e_existing_pipeline_all_1.graph.customer_order_summary import *
from python_e2e_existing_pipeline_all_1.config.ConfigStore import *


class customer_order_summaryTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/customer_order_summary/in0/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/customer_order_summary/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/customer_order_summary/out/schema.json',
            'test/resources/data/python_e2e_existing_pipeline_all_1/graph/customer_order_summary/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = customer_order_summary(self.spark, dfIn0)

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
