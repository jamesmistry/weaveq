# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import
import sys
import logging
import tests.system.perf_test

if __name__ == "__main__":
    if (not tests.system.perf_test.run()):
        sys.exit(1)

