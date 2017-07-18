# -*- coding: utf-8 -*-

"""!
@package weaveq.wqexception Exception classes.
"""

class WeaveQError(Exception):
    """!
    Superclass of all WeaveQ exceptions.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(WeaveQError, self).__init__(message)

class TextQueryCompileError(WeaveQError):
    """!
    Exception thrown when a text query fails to compile.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(TextQueryCompileError, self).__init__(message)

class ConfigurationError(WeaveQError):
    """!
    Exception thrown when a configuration can't be loaded.
    """
    def __init__(self, message):
        """!
        Constructor.

        @param message string: Error description
        """
        super(ConfigurationError, self).__init__(message)

class DataSourceBuildError(WeaveQError):
    """!
    Exception thrown when a data source build operation encounters an error.

    @param message string: Error description
    """
    def __init__(self, message):
        """!
        Constructor.
        
        @param message string: Error description
        """
        super(DataSourceBuildError, self).__init__(message)

class DataSourceError(WeaveQError):
    """!
    Exception thrown when a data source encounters an error.

    @param message string: Error description
    """
    def __init__(self, message):
        """!
        Constructor.
        
        @param message string: Error description
        """
        super(DataSourceError, self).__init__(message)
