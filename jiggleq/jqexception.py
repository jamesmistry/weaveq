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
