import os
import unittest
from basic_log import BasicLog

FILENAME = 'test_file.bin'

class TestDatabase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        if os.path.exists(FILENAME):
            os.remove(FILENAME)

    # db_set
    def test_db_set_stores_pair(self):
        db = BasicLog(FILENAME)
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')

        with open(FILENAME, 'r') as s:
            line1 = s.readline().strip()
            line2 = s.readline().strip()

        self.assertEqual(line1, '1,test1')
        self.assertEqual(line2, '2,test2')


    # db_get
    def test_db_get_retrieve_val(self):
        with open(FILENAME, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')

        db = BasicLog(FILENAME)

        self.assertEqual(db.db_get('1'), 'test1')
        self.assertEqual(db.db_get('2'), 'test2')
        self.assertEqual(db.db_get('3'), None)
    
    def test_db_get_retrieve_most_recent_val(self):
        with open(FILENAME, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('1,new value\n')
        
        db = BasicLog(FILENAME)
        self.assertEqual(db.db_get('1'), 'new value')

if __name__ == '__main__':
    unittest.main()