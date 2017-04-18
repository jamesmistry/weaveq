import sys

import tests.system.test_elastic
import tests.system.test_perf

all_tests_passed = True

if (not tests.system.test_elastic.run()):
    all_tests_passed = False

if (not tests.system.test_perf.run()):
    all_tests_passed = False

if (not all_tests_passed):
    print("One or more tests failed :-(")
    sys.exit(1)
else:
    print("All tests passed :-)")
    sys.exit(0)
