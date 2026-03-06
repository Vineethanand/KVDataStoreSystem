"""
Implements the KVDataStore model class
"""
import os
import threading
import logging

from kv_datastore_system.models.psuedo_bloom_filter import PsuedoBloomFilter
from kv_datastore_system.exceptions.kv_datastore_exception import KVDataDirectoryCreationError

class KVDataStore:
    """
    KVDataStore class that has the core functionalities for
    1. Put(Key, Value)
    2. Read(Key)
    3. ReadKeyRange(StartKey, EndKey)
    4. BatchPut(..keys, ..values)
    5. Delete(key)
    """
    DATA_DIR_CREATION_RETRIES = 3

    def __init__(self, kv_data_dir="kv_data_dir", mem_threshold=1000):
        """
        Constructor
        """
        self._kv_data_dir = kv_data_dir
        self._mem_threshold = mem_threshold
        self._memtable = {}
        self._wal_path = os.path.join(self.__kv_data_dir, 'wal.log')
        self.lock = threading.Lock()


        # Save the execption in a variable for future check
        error = None
        if not os.path.exists(self._kv_data_dir):
            for attempt in range(KVDataStore.DATA_DIR_CREATION_RETRIES):
                logging.info("KV Data directory is not present, attempt {attempt} to create")
                try:
                    os.makedirs(self._kv_data_dir)
                    error=None
                except OSError as e:
                    error = e
                    logging.info("KV Data directory creation failed!")
        
        # Raise an error if there is an issue in creating the Data directory
        if error:
            raise KVDataDirectoryCreationError(str(e))
