import logging
import sys

def get(obj):
    """
    Get a logger for JiggleQ.

    @param obj: The calling object.
    """

    return logging.getLogger("jiggleq")

