# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import
import unittest
import os
import sys
import logging
import tests.system.perf_test

if __name__ == "__main__":
    print("** Test success is exclusively determined by exit code: PASS (0), FAIL (!=0) **")

    logging.basicConfig(format='%(levelname)s\t%(asctime)s\t%(filename)s:%(lineno)s] %(message)s', level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)
    scriptDir = os.path.dirname(os.path.abspath(__file__))
    testsuite = unittest.TestLoader().discover("{0}/tests/system".format(scriptDir), pattern="*_test.py")
    unittest.TextTestRunner(verbosity=1).run(testsuite)

    if (not tests.system.perf_test.run()):
        sys.exit(1)

