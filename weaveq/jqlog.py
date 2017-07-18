# -*- coding: utf-8 -*-

import logging
import sys

def get(obj):
    """
    Get a logger for WeaveQ.

    @param obj: The calling object.
    """

    return logging.getLogger("weaveq")

