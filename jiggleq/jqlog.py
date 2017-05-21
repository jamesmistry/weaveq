import logging
import sys

def get(obj):
    """
    Get a logger based on the calling object's namespace.

    @param obj: The calling object from which the logger name will be derived
    """

    return logging.getLogger("jiggleq.{0}".format(obj.__class__.__name__))

#class JqLogger(object):
#    """!
#    Singleton for constructing the default JiggleQ logger object.
#    """

#    ## Logger instance
#    instance = None

#   @staticmethod
#    def get(obj):
#        """
#        Get a logger based on the calling object's namespace.
#
#        @param obj: The calling object from which the logger name will be derived
#        """

#        return logging.getLogger("{0}.{1}".format(obj.__module__, obj.__class__.__name__))

        #if (JqLogger.instance is None):
        #    JqLogger.instance = logging.getLogger("jiggleq")
        #    JqLogger.instance.setLevel(logging.ERROR)
        #    ch = logging.StreamHandler(sys.stdout)
        #    formatter = logging.Formatter("%(levelname)s\t%(asctime)s\t%(filename)s:%(lineno)s] %(message)s")
        #    ch.setFormatter(formatter)
            
        #    JqLogger.instance.addHandler(ch)
        
        #return JqLogger.instance
