from __future__ import print_function
import sys

import application

if (__name__ == "__main__"):
    exit_with_error = False

    try:
        entry_point = application.App()
        entry_point.run()
    except SystemExit:
        raise
    except:
        exit_with_error = True

    sys.exit(1 if exit_with_error else 0)
