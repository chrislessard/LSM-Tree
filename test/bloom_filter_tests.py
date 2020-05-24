import unittest
from src.bloom_filter import BloomFilter

class BloomFilterTests(unittest.TestCase):
    def test_add_item(self):
        '''
        Tests that an item can be added to the Bloom Filter.
        '''
        bf = BloomFilter(1, 0.05)
        bf.add('christian')

        self.assertTrue(bf.check('christian'))

    def test_add_items(self):
        '''
        Tests that multiple items can be added to the Bloom Filter.
        '''
        bf = BloomFilter(4, 0.05)
        bf.add('christian')
        bf.add('daniel')
        bf.add('debra')
        bf.add('charles-adrian')

        self.assertTrue(bf.check('christian'))
        self.assertTrue(bf.check('daniel'))
        self.assertTrue(bf.check('debra'))
        self.assertTrue(bf.check('charles-adrian'))

    def test_add_item_different_prob(self):
        '''
        Tests that an item can be added to the Bloom Filter with
        different false positive probabilities.
        '''
        bf = BloomFilter(1, 0.10)
        bf.add('christian')

        self.assertTrue(bf.check('christian'))

        bf = BloomFilter(1, 0.5)
        bf.add('christian')

        self.assertTrue(bf.check('christian'))
    
        bf = BloomFilter(1, 0.9)
        bf.add('christian')

        self.assertTrue(bf.check('christian'))

    def test_add_items_different_prob(self):
        '''
        Tests that multiple items can be added to the Bloom Filter with
        different false positive probabilities.
        '''
        bf = BloomFilter(4, 0.10)
        bf.add('christian')
        bf.add('daniel')
        bf.add('debra')
        bf.add('charles-adrian')

        self.assertTrue(bf.check('christian'))
        self.assertTrue(bf.check('daniel'))
        self.assertTrue(bf.check('debra'))
        self.assertTrue(bf.check('charles-adrian'))

        bf = BloomFilter(4, 0.50)
        bf.add('christian')
        bf.add('daniel')
        bf.add('debra')
        bf.add('charles-adrian')

        self.assertTrue(bf.check('christian'))
        self.assertTrue(bf.check('daniel'))
        self.assertTrue(bf.check('debra'))
        self.assertTrue(bf.check('charles-adrian'))

        bf = BloomFilter(4, 0.9)
        bf.add('christian')
        bf.add('daniel')
        bf.add('debra')
        bf.add('charles-adrian')

        self.assertTrue(bf.check('christian'))
        self.assertTrue(bf.check('daniel'))
        self.assertTrue(bf.check('debra'))
        self.assertTrue(bf.check('charles-adrian'))

    def test_response_for_any_key(self):
        '''
        Tests that the Bloom Filter will give a response
        no matter the key. We cannot test any further, as we don't
        know with certainty whether we'll get a false positive.
        '''
        bf = BloomFilter(1, 0.05)
        bf.add('christian')

        self.assertTrue(bf.check('christian'))

        r1 = bf.check('daniel') in (True, False)
        r2 = bf.check('charles') in (True, False)

        self.assertTrue(r1)
        self.assertTrue(r2)

    def test_bf_calculate_correct_num_hases(self):
        '''
        Tests that the bloom filter calculates the correct number
        of hash functions to use.
        '''
        num_items = 20
        prob = 0.05
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.num_hash_fns, 4)

        num_items = 1000
        prob = 0.25
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.num_hash_fns, 1)

        num_items = 10000
        prob = 0.02
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.num_hash_fns, 5)

    def test_bf_calculate_correct_bit_array_size(self):
        '''
            Tests that the bloom filter calculates the correct number
            of hash functions to use.
            '''
        num_items = 20
        prob = 0.05
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.bit_array_size, 124)

        num_items = 1000
        prob = 0.25
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.bit_array_size, 2885)

        num_items = 10000
        prob = 0.02
        bf = BloomFilter(num_items, prob)
        self.assertEqual(bf.bit_array_size, 81423)
