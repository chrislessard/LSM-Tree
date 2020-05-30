from math import log
from mmh3 import hash
from bitarray import bitarray 
  
class BloomFilter:
    # For an explanation of the math, visit 
    # https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives.

    # A condensed explanation can be found here:
    # https://stackoverflow.com/questions/658439/how-many-hash-functions-does-my-bloom-filter-need

    def __init__(self, num_items, false_positive_prob): 
        ''' (self, int, float) -> BloomFilter
        Creates a new BloomFilter. num_items represents the number of items 
        expected to be stored in the structure. fb_prob represents the 
        desired probability of a false positive, represented as a decimal value 
        between 0 and 1. 
        
        Note: A lower false positive probability comes at the expense of time 
        and computational power.
        '''
        self.false_positive_prob = false_positive_prob 
        self.bit_array_size = self.bit_array_size(num_items, false_positive_prob) 
        self.num_hash_fns = self.get_hash_count(self.bit_array_size, num_items) 

        self.bit_array = bitarray(self.bit_array_size) 
        self.bit_array.setall(0)

    def add(self, item): 
        ''' (self, str) -> None
        Add item to the BloomFilter.
        '''
        digests = [] 
        for seed in range(self.num_hash_fns):
            # each seed creates a different digest.
            digest = hash(item, seed) % self.bit_array_size 
            digests.append(digest) 
  
            self.bit_array[digest] = True

    def check(self, item): 
        ''' (self, str) -> Boolean
        Check for existence of an item in filter 
        '''
        for seed in range(self.num_hash_fns): 
            digest = hash(item, seed) % self.bit_array_size 
            if self.bit_array[digest] == False: 
                # if any bit is false, the item is not definitely present
                return False
        return True
  
    # Helpers
    def bit_array_size(self, num_items, probability):
        ''' (self, int, float) -> int
        Return the required size of the bit array, m, as a function
        of n, the number of items expect to be stored, and p,
        the desired false positive probability.
 
        m = -(n * lg(p)) / (lg(2)^2) 
        '''
        m = -(num_items * log(probability)) / (log(2)**2)
        return int(m)

    def get_hash_count(self, bit_arr_size, num_items): 
        ''' (self, int, int) -> int
        Return the required number of hash functions to be used
        by the structure, as a function of m, the size of the bit array,
        and n, the number of items expected to be stored.

        k = (m/n) * lg(2)

        Note: Many standard BloomFilter implementations draw on a nubmer of
        distinct hash functions. Since this one allows us to specify the desired
        false positive probability (which directly determines how many hash 
        fns to use) we take a different approach. A single hash function, MurmurHash3,
        is used and called with various seeds, each of which produce a different 
        digest for the same input.
        '''
        return int((bit_arr_size/num_items) * log(2))
