import unittest

from test.python_e2e_existing_pipeline_all_2.graph.pythonSG.test_Reformat_2 import *
from test.python_e2e_existing_pipeline_all_2.graph.pythonSG.test_Filter_1 import *
from test.python_e2e_existing_pipeline_all_2.graph.pythonSG.Subgraph_2.test_RowDistributor_1 import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
