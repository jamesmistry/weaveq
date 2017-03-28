from __future__ import print_function
import unittest
import os

if __name__ == "__main__":
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    testsuite = unittest.TestLoader().discover("{0}/tests/unit".format(scriptDir), pattern="*_test.py")
    unittest.TextTestRunner(verbosity=1).run(testsuite)
