"""
Implements the KVDataStore model class
"""
import os
import threading
import logging
import json
import time
import math

from kv_datastore_system.models.psuedo_bloom_filter import PsuedoBloomFilter
from kv_datastore_system.exceptions.kv_datastore_exception import KVDataDirectoryCreationError

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)

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
    DISKTABLE_FILENAME_PREFIX = "disk_table_"

    def __init__(self, kv_data_dir, mem_threshold=1000):
        """
        Constructor
        """
        self._kv_data_dir = kv_data_dir
        self._mem_threshold = mem_threshold
        self._memtable = {}
        self._wal_path = os.path.join(self._kv_data_dir, 'wal.log')
        self._lock = threading.Lock()
        self._filters : dict[str, PsuedoBloomFilter]= {}
        self._range_of_disktables : dict[str, (str, str)]= {}

        # Save the execption in a variable for future check
        error = None
        
        for attempt in range(KVDataStore.DATA_DIR_CREATION_RETRIES):
            logger.info(f"KV Data directory is not present, attempt {attempt} to create")
            if not os.path.exists(self._kv_data_dir):
                try:
                    os.makedirs(self._kv_data_dir)
                except OSError as e:
                    error = e
                    logger.info("KV Data directory creation failed!")
            else:
                break
        # Raise an error if there is an issue in creating the Data directory
        if error:
            raise KVDataDirectoryCreationError(str(error))

        self._recover_from_wal()
        self._recover_filters()

    def _recover_filters(self):
        """
        Method to recover filters in case of a crash.
        """
        for filename in os.listdir(self._kv_data_dir):
            if filename.startswith(KVDataStore.DISKTABLE_FILENAME_PREFIX):
                self._create_filters_for_disktable(filename)

    def _create_filters_for_disktable(self, filename):
        """
        Method to create the filter for disktable for each lookup
        This method can be used to recover the filters during a crash
        """
        filter : PsuedoBloomFilter = PsuedoBloomFilter()

        # Add the filter info to the filters map
        self._filters[filename] = filter
        for key in self._memtable:
            filter.add_key(key)

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
        # else:
        #     wfp = open(self._wal_path,"w")
        #     wfp.close()
                

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
        disktable_name = f"{KVDataStore.DISKTABLE_FILENAME_PREFIX}{timestamp}.json"
        
        self._create_filters_for_disktable(disktable_name)

        with open(os.path.join(self._kv_data_dir, disktable_name), "w") as dtfp:
            sorted_entries = sorted(self._memtable.items())
            # Find the max and min key in the current memtable
            # Save it as the range of the disktables for easy filtering
            # This will help in the easy filtering during batch read
            max_key = max(sorted_entries.keys()[-1])
            min_key = min(sorted_entries.keys())[0]
            self._range_of_disktables[disktable_name] = (min_key, max_key)
            json.dump(sorted_entries, dtfp)
        # Clear the memtable once it is written to the disk table file
        self._memtable.clear()
        # Clear the entries in the WAL
        open(self._wal_path, 'w').close()


    def put(self, key, value):
        """
        Method to add a new key, value pair to the Datastore
        """
        with self._lock:
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
            if self._memtable[key] != KVDataStore.DELETE_ENTRY:
                return self._memtable[key]
            else: None
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
        sorted_keys = sorted(self._memtable)
        required_vals = []
        for key in sorted_keys:
            if start_key <= key and end_key >= key:
                required_vals.append(self._memtable[key])
        
        for filename in sorted(os.listdir(self._kv_data_dir), reverse=True):
            if filename.startswith(KVDataStore.DISKTABLE_FILENAME_PREFIX):
                # Check whether the key is with the range of this Disk table
                if self._range_of_disktables[filename][0] <= start_key and self._range_of_disktables[filename][1] >= end_key:
                    with open(os.path.join(self._kv_data_dir, filename), "r") as dtfp:
                        entries = json.load(dtfp)
                        for key in entries:
                            if key >= start_key and key <= end_key:
                                required_vals.append(entries[key])

        return required_vals


    def batch_put(self, key_values : dict):
        """
        Method to put a batch of key value pairs
        """
        with self._lock:
            wal_new_entries = [json.dumps({"op" : "put", "key" : k, "value" : v}) + "\n" \
                                    for k,v in key_values.items()]

            with open(self._wal_path, "a") as walfp:
                walfp.writelines(wal_new_entries)
                walfp.flush()
                os.fsync(walfp.fileno())

            for k, v in key_values.items():
                self._memtable[k] = v
            
            if len(self._memtable) >= self._mem_threshold:
                self._move_to_disktable()

        
