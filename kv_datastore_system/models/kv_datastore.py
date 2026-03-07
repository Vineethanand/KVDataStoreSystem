"""
Implements the KVDataStore model class
"""
import os
import threading
import logging
import json
import time

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
    DELETE_ENTRY = "__DELETED__"
    def __init__(self, kv_data_dir="/tmp/kv_data_dir", mem_threshold=1000):
        """
        Constructor
        """
        self._kv_data_dir = kv_data_dir
        self._mem_threshold = mem_threshold
        self._memtable = {}
        self._wal_path = os.path.join(self.__kv_data_dir, 'wal.log')
        self._lock = threading.Lock()
        self._filters : dict[str, PsuedoBloomFilter]= {}
        self._range_of_disktables : dict[str, (str, str)]= {}

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

        self._recover_from_wal()

    def _recover_from_wal(self):
        """
        Method to recover from the write ahead log
        """
        if os.path.exists(self._wal_path):
            with open(self._wal_path, "r") as wfp:
                for line in wfp:
                    entry = json.loads(line)
                    if entry['op'] == 'put':
                        self._memtable[entry['key']] = entry['value']
                    elif entry['op'] == 'delete':
                        self._memtable[entry['key']] = KVDataStore.DELETE_ENTRY


    def _make_wal_entry(self, entry):
        """
        Method to create an entry in the write ahead log.
        """
        with open(self._wal_path, 'a') as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _move_to_disktable(self):
        """
        Method to move the data to disk tables which is sorted.
        The disktables will have the entries sorted w.r.t to keys 
        """
        timestamp = int(time.time()*10000)
        # Create a name for the disktable with time stamp in the name
        # This will make the sorting easy while reading the disk tables
        # in reverse order
        disktable_name = f"disk_table_{timestamp}.json"
        filter : PsuedoBloomFilter = PsuedoBloomFilter()

        # Add the filter info to the filters map
        self._filters[disktable_name] = filter
        for key in self._memtable:
            filter.add_key(key)

        with open(os.path.join(self._kv_data_dir, disktable_name), "w") as dtfp:
            sorted_entries = sorted(self._memtable.items())
            json.dump(sorted_entries, dtfp)
        # Clear the memtable once it is written to the disk table file
        self._memtable.clear()
        # Clear the entries in the WAL
        open(self._wal_path, 'w').close()


    def put(self, key, value):
        """
        Method to add a new key, value pair to the Datastore
        """
        with self._lock():
            self._make_wal_entry({'op':'put', 'key': key, 'value': value})
            self._memtable[key] = value
            if len(self._memtable) >= self._mem_threshold:
                self._move_to_disktable()

    def read(self, key):
        """
        Method to read the key from Datastore
        """
        # If the key is available in Memtable, return immediately
        if key in self._memtable:
            return self._memtable[key] if self._memtable[key] != KVDataStore.DELETE_ENTRY else None
        else:
            # Check the Disk table
            # Check the psuedo bloom filter to see whether the key is present
            # Otherwise go to the next file
            for file in sorted(self._filters.keys(), reverse=True):
                if self._filters[file].contains(key):
                    with open(os.path.join(self._kv_data_dir, file), "r") as dtfp:
                        entries = json.load(dtfp)
                        if key in entries:
                            return entries[key] if entries[key] != KVDataStore.DELETE_ENTRY else None
        
        return None


    def delete(self, key):
        """
        Method to delete a key from datastore
        """
        with self._lock:
            entry = {'op' : 'delete', 'key': key}
            self._make_wal_entry(entry)
        self._memtable[key] = KVDataStore.DELETE_ENTRY

        if len(self._memtable) >= self._mem_threshold:
            self._move_to_disktable()


    def read_key_range(self, start_key, end_key):
        """
        Method to read keys in a range starting from start_key and ending at end_key
        """


