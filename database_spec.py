import os
import unittest
import database as database

FILENAME = 'test_file.bin'

class TestDatabase(unittest.TestCase):
    # db_set
    def test_db_set_stores_pair(self):
        db = database.Database(FILENAME)
        
        db.db_set(1, 'test1')
        db.db_set(2, 'test2')

        stream = open('testFile', 'rb')
        line1 = stream.readline()
        line2 = stream.readline()

        self.assertEqual(line1, '1: test1')
        self.assertEqual(line1, '2: test2')
        stream.close

        os.remove(FILENAME)

    # db_get
    def test_db_get_retrieve_val(self):
        db = database.Database(FILENAME)

        db.db_set(1, 'test1')
        db.db_set(2, 'test2')

        self.assertEqual(db.db_get(1), 'test1')
        self.assertEqual(db.db_get(2), 'test2')

        os.remove(FILENAME)

if __name__ == '__main__':
    unittest.main()