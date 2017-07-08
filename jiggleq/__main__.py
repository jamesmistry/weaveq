from __future__ import print_function
import sys

import application

if (__name__ == "__main__"):
    unexpected_error = False

    try:
        entry_point = application.App()
        entry_point.run()
    except SystemExit:
        raise
    except:
        unexpected_error = True

    sys.exit(1 if unexpected_error else 0)
