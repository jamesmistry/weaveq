class JiggleQError(Exception):
    """!
    Superclass of all JiggleQ exceptions.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(JiggleQError, self).__init__(message)

class TextQueryCompileError(JiggleQError):
    """!
    Exception thrown when a text query fails to compile.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(TextQueryCompileError, self).__init__(message)

class ConfigurationError(JiggleQError):
    """!
    Exception thrown when a configuration can't be loaded.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(ConfigurationError, self).__init__(message)
