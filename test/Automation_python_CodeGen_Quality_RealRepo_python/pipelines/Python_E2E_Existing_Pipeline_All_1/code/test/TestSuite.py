import unittest

from test.python_e2e_existing_pipeline_all_1.graph.test_Repartition_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.Subgraph_1.test_Reformat_2 import *
from test.python_e2e_existing_pipeline_all_1.graph.Subgraph_1.test_Filter_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.Subgraph_1.Subgraph_2.test_RowDistributor_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_Reformat_2 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_Repartition_1_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_Filter_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_Reformat_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_Reformat_2_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.pythonSG.test_Reformat_2 import *
from test.python_e2e_existing_pipeline_all_1.graph.pythonSG.test_Filter_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.pythonSG.Subgraph_2.test_RowDistributor_1 import *
from test.python_e2e_existing_pipeline_all_1.graph.test_customer_order_summary import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
