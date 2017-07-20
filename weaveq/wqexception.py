# -*- coding: utf-8 -*-

"""!
@package weaveq.wqexception Exception classes.
"""

# Copyright 2017 James Mistry.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
# to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
# and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.

from __future__ import print_function, absolute_import

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
