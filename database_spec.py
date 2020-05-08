import os
import unittest
import database as database

FILENAME = 'test_file.bin'

class TestDatabase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        os.remove(FILENAME)

    # db_set
    def test_db_set_stores_pair(self):
        db = database.Database(FILENAME)
        
        db.db_set(1, 'test1')
        db.db_set(2, 'test2')

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

        db = database.Database(FILENAME)

        self.assertEqual(db.db_get(1), 'test1')
        self.assertEqual(db.db_get(2), 'test2')
        self.assertEqual(db.db_get(3), None)

if __name__ == '__main__':
    unittest.main()