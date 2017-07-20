# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import
import sys

import weaveq.application

def main():
    exit_with_error = False

    try:
        entry_point = weaveq.application.App()
        entry_point.run()
    except SystemExit:
        raise
    except:
        exit_with_error = True

    sys.exit(1 if exit_with_error else 0)

if (__name__ == "__main__"):
    main()
