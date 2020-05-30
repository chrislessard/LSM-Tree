import unittest
from os import remove
from src.append_log import AppendLog

FILENAME = 'testfile'

class AppendLogTests(unittest.TestCase):
    def tearDown(self):
        AppendLog.instance(FILENAME).clear()

    def test_write_value(self):
        '''
        Tests that a single value can be written to disk.
        '''
        output = 'test,test string\n'
        a = AppendLog.instance(FILENAME)
        a.write(output)

        with open(FILENAME, 'r') as s:
            input = s.readline()
        
        self.assertEqual(output, input)

    def test_write_multiple_values(self):
        '''
        Tests that mutliple values can be written to disk.
        '''
        output1 = 'test,test string\n'
        output2 = 'write,writer\n'
        a = AppendLog.instance(FILENAME)
        a.write(output1)
        a.write(output2)

        with open(FILENAME, 'r') as s:
            input = s.readlines()

        self.assertEqual(output1, input[0])
        self.assertEqual(output2, input[1])

    def test_clear(self):
        '''
        Tests that the disk contents can be cleared.
        '''
        a = AppendLog.instance(FILENAME)
        a.write('test1,test2\n')
        a.write('test3,test4\n')
        a.write('test5,test6\n')
        a.clear()

        with open(FILENAME, 'r') as s:
            input = s.readlines()

        self.assertTrue(len(input) == 0)

    def test_singleton_returns_same_instance(self):
        '''
        Tests that each instance is identitcal.
        '''
        a = AppendLog.instance(FILENAME)
        b = AppendLog.instance(FILENAME)
        c = AppendLog.instance(FILENAME)

        self.assertEqual(id(a), id(b))
        self.assertEqual(id(a), id(c))
        self.assertEqual(id(b), id(c))
