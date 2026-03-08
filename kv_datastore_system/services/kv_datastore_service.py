"""
Implements the service for KVDataStore
"""
from kv_datastore_system.models.kv_datastore import KVDataStore

class KVDataStoreService:
    """
    Service class to provide the services of KVDataStore
    """
    def __init__(self, kv_data_dir="/home/kv_datastore_dir", kv_mem_threshold="1000"):
        # The KVDatStore object can be passed from the controller to make sure
        # that the specific DatStore model is used in the backend.
        # Through dependency injection. Since in this implementation we have
        # only one DataStore model, so initializing it insid the constructor
        self._kv_datastore : KVDataStore = KVDataStore(kv_data_dir, kv_mem_threshold)

    def put(self, key, value):
        try:
            self._kv_datastore.put(key, value)
        except Exception as e:
            raise Exception(f"Error while inserting {key}:{value}, {e}")

    def read(self, key):
        try:
            val= self._kv_datastore.read(key)
            return val
        except Exception as e:
            raise Exception(f"Error while reading {key}, {e}")

    def delete(self, key):
        try:
            self._kv_datastore.delete(key)
        except Exception as e:
            raise Exception(f"Error while deleting {key}, {e}")

    def batch_put(self, key_values):
        try:
            self._kv_datastore.batch_put(key_values)
        except Exception as e:
            raise Exception(f"Error while inserting a batch {key_values}, e")

    def read_range(self, start_key, end_key):
        try:
            values = self._kv_datastore.read_key_range(start_key, end_key)
            return values
        except Exception as e:
            raise Exception(f"Error while reading a range from {start_key} to {end_key}, e")

