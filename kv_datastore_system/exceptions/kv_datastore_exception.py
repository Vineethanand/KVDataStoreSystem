"""
Implements the exception classes for kv_datastore model.
"""

class KVDataDirectoryCreationError(Exception):
    """
    Exception during the data directory creation
    """
    def __init__(self, message = "Data directory creation failed"):
        super.__init__(self, message)
