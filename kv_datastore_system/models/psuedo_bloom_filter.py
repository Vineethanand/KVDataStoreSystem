"""
Implements model class PseudoBloomFilter
"""
import hashlib

class PsuedoBloomFilter:

    """
    Class that implements the functionality to check whether a data is
    present in datastore using a collection of bits.
    """
    def __init__(self, size=10000, hash_count=5):
        """
        Constructor
        :param size - Num of keys to be 
        :param hash_count - Num of hashes to use while setting the bits
        for a key
        """
        self._filter_bits : int  = 0
        self._size = size
        self._hash_count = hash_count

    def _get_bits_to_set(self,key):
        """
        Method to get the bits to set in the filter for a particular key
        """
        # Get a hashvalue for the key using md5 hashing algorithm
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        bits_to_set = []
        # Get the index consider the maximum size to which the filter
        # is initialized with using the modular operation.
        for i in range(self._hash_count):
            index = (hash_value + hash_value * i) % self._size
            bits_to_set.append(index)
        return bits_to_set

    def add_key(self, key):
        """
        Method to add a key to the filter
        """
        # Find the indices the _get_bits_to_set method
        bits_to_set = self._get_bits_to_set(key)
        # Set each bit corresponding to the key in the filter
        for bit in bits_to_set:
            self._filter_bits |= (1 << bit)

    def contains(self, key):
        """
        Method to check whether the key is present
        """
        # Check whether all bits to set corresponding to the key
        # are already set in the filter
        bits_to_set = self._get_bits_to_set(key)
        for bit in bits_to_set:
            if not (self._filter_bits | (1<<bit) ):
                return False
        return True