import unittest
import os
import pickle
from pathlib import Path
from src.ss_table import SSTable

TEST_FILENAME = 'test_file-1'
TEST_BASEPATH = 'test-segments/'
BKUP_NAME = 'test_backup'
TESTPATH = TEST_BASEPATH + TEST_FILENAME

class TestDatabase(unittest.TestCase):
    def setUp(self):
        if not (Path(TEST_BASEPATH).exists() and Path(TEST_BASEPATH).is_dir):
            Path(TEST_BASEPATH).mkdir()

    def tearDown(self):
        for filename in os.listdir(TEST_BASEPATH):
            os.remove(TEST_BASEPATH + filename)

    def tearDownClass():
        os.rmdir(TEST_BASEPATH)

    # db_set
    def test_db_set_stores_pair_in_memtable(self):
        '''
        Tests the db_set functionality.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')

        node1 = db.memtable.find_node('1')
        node2 = db.memtable.find_node('2')

        self.assertEqual(node1.value, 'test1')
        self.assertEqual(node2.value, 'test2')
    
    def test_db_set_flushes_to_disk_past_threshold(self):
        '''
        Tests the db_set functionality.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')
        db.db_set('3', 'cl')

        with open(TESTPATH, 'r') as s:
            lines = s.readlines()
        
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0].strip(), '1,test1')

        node2 = db.memtable.find_node('2')
        self.assertEqual(node2.value, 'test2')

    def test_in_order_traversal(self):
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable.add('chris', 'lessard')
        db.memtable.add('daniel', 'lessard')
        db.memtable.add('debra', 'brown')
        db.memtable.add('antony', 'merchy')

        nodes = db.memtable.in_order()
        vals = [node.key for node in nodes]

        self.assertEqual(vals, ['antony', 'chris', 'daniel', 'debra'])

    def test_flush_memtable(self):
        '''
        Tests that the memtable can be flushed to disk
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable.add('chris', 'lessard')
        db.memtable.add('daniel', 'lessard')
        db.flush_memtable(TESTPATH)

        with open(TESTPATH, 'r') as s:
            lines = s.readlines()

        expected_lines = ['chris,lessard\n', 'daniel,lessard\n']
        self.assertEqual(lines, expected_lines)

    def test_db_set_writes_to_wal(self):
        '''
        Tests that db_set invocations write the values to the write-ahead-log.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        
        # The singleton instance will persist throughout the suite.
        # This test tests its functionality directory, so we need to wipe
        # its state.
        db.memtable_bkup().clear()
        open(TEST_BASEPATH + BKUP_NAME, 'w').close()

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')

        self.assertEqual(Path(TEST_BASEPATH + BKUP_NAME).exists(), True)
        with open(TEST_BASEPATH + BKUP_NAME, 'r') as s:
            lines = s.readlines()
        
        self.assertEqual(len(lines), 2)

    # db_get
    def test_db_get_single_val_retrieval(self):
        '''
        Tests the retrieval of a single value written into the db
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.db_set('chris', 'lessard')
        val = db.db_get('chris')

        self.assertEqual(val, 'lessard')
    
    def test_db_get_low_threshold(self):
        '''
        Tests the db_get functionality when the db threshold is low.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 20
        db.db_set('chris','lessard')
        db.db_set('daniel','lessard')
        db.db_set('charles','lessard')
        db.db_set('adrian','lessard')

        self.assertEqual(db.db_get('chris'), 'lessard')
        self.assertEqual(db.db_get('daniel'), 'lessard')
        self.assertEqual(db.db_get('charles'), 'lessard')
        self.assertEqual(db.db_get('adrian'), 'lessard')

    def test_db_get_miss(self):
        '''
        Tests the db_get functionality when the key has not been stored in the db.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 20
        db.db_set('chris','lessard')
        db.db_set('daniel','lessard')
        db.db_set('charles','lessard')
        db.db_set('adrian','lessard')

        self.assertEqual(db.db_get('debra'), None)

    def test_db_get_retrieve_most_recent_val(self):
        '''
        Tests that db_get retrieves the most recent key value.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)

        db.db_set('chris', 'lessard')
        db.db_set('chris', 'martinez')
        self.assertEqual(db.db_get('chris'), 'martinez')
    
    def test_db_get_retrieve_most_recent_val_multiple_thresholds(self):
        '''
        Tests that db_get retrieves the most recent key value.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10

        db.db_set('chris', 'lessard')
        db.db_set('chris', 'martinez')
        self.assertEqual(db.db_get('chris'), 'martinez')
    
    def test_db_get_retrieve_val_multiple_segments(self):
        '''
        Tests the db_get functionality.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')

        db.db_set('chris', 'martinez')
        db.db_set('a', 'b')
        db.db_set('a', 'c')

        self.assertEqual(db.db_get('chris'), 'martinez')
        self.assertEqual(db.db_get('daniel'), 'lessard')
        self.assertEqual(db.db_get('a'), 'c')

    # segments
    def test_db_set_uses_segment(self):
        '''
        Tests that new segments are created and used when the threshold is reached.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10
        db.db_set('abc', 'cba')
        db.db_set('def', 'fed') # This will cross the threshold

        self.assertEqual(db.memtable.total_bytes, 6)
        self.assertEqual(db.current_segment, 'test_file-2')

    # # compaction
    def test_compact_single_segment(self):
        '''
        Tests that a single segment can be compacted.
        '''
        with open(TEST_BASEPATH + TEST_FILENAME, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('3,test3\n')
            s.write('1,test4\n')
            s.write('2,test5\n')
            s.write('3,test6\n')
            s.write('1,test7\n')
            s.write('2,test8\n')
            s.write('3,test9\n')

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.compact_segment(TEST_FILENAME)

        with open(TEST_BASEPATH + TEST_FILENAME, 'r') as s:
            lines = s.readlines()

        self.assertEqual(lines, ['1,test7\n', '2,test8\n', '3,test9\n'])

    def test_compact_even_number_multiple_segments(self):
        '''
        Tests that multiple segments can be compacted.
        '''
        segments = ['test_file-1', 'test_file-2']
        with open(TEST_BASEPATH + segments[0], 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('1,test3\n')
            s.write('2,test4\n')

        with open(TEST_BASEPATH + segments[1], 'w') as s:
            s.write('1,test5\n')
            s.write('2,test6\n')
            s.write('1,test7\n')
            s.write('2,test8\n')

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments
        db.compact()

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        expected_result = ['1,test7\n', '2,test8\n']

        self.assertEqual(first_segment_lines, expected_result)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), False)

    def test_compact_odd_number_multiple_segments(self):
        '''
        Tests that multiple segments can be compacted.
        '''
        segments = ['test_file-1', 'test_file-2', 'test_file-3']
        with open(TEST_BASEPATH + segments[0], 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('4,test9\n')
            s.write('2,test4\n')

        with open(TEST_BASEPATH + segments[1], 'w') as s:
            s.write('1,test5\n')
            s.write('2,test6\n')
            s.write('3,test7\n')
            s.write('2,test8\n')

        with open(TEST_BASEPATH + segments[2], 'w') as s:
            s.write('1,test9\n')
            s.write('2,testa\n')
            s.write('3,test9\n')
            s.write('2,testc\n')

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments
        db.compact()

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        expected_result = [
            '1,test9\n', 
            '2,testc\n', 
            '3,test9\n', 
            '4,test9\n', 
        ]

        self.assertEqual(first_segment_lines, expected_result)

        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), False)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[2]), False)

    def test_compact_multiple_segments_with_threshold(self):
        '''
        Tests that the compaction algorithm correctly factors in the segment
        size threshold when merging segments.
        '''
        segments = ['test_file-1', 'test_file-2', 'test_file-3']
        with open(TEST_BASEPATH + segments[0], 'w') as s:
            s.write('1,four\n')
            s.write('2,bomb\n')
            s.write('1,john\n')
            s.write('2,long\n')

        with open(TEST_BASEPATH + segments[1], 'w') as s:
            s.write('3,gone\n')
            s.write('4,girl\n')
            s.write('3,woot\n')
            s.write('4,chew\n')

        with open(TEST_BASEPATH + segments[2], 'w') as s:
            s.write('5,noob\n')
            s.write('6,fear\n')
            s.write('5,love\n')
            s.write('6,osrs\n')

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments
        db.threshold = 14 * 2
        db.compact()

        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[0]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[2]), False)

        first_seg_expected_vals = ['1,john\n', '2,long\n', '3,woot\n', '4,chew\n']
        scnd_seg_expected_vals = ['5,love\n', '6,osrs\n']

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        # Check second segment is correct
        with open(TEST_BASEPATH + segments[1], 'r') as s:
            scnd_segment_lines = s.readlines()

        self.assertEqual(first_segment_lines, first_seg_expected_vals)
        self.assertEqual(scnd_segment_lines, scnd_seg_expected_vals)

    def test_merge_two_segments(self):
        segments = ['test_file-1', 'test_file-2']
        with open(TEST_BASEPATH + segments[0], 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('4,test6\n')

        with open(TEST_BASEPATH + segments[1], 'w') as s:
            s.write('1,test5\n')
            s.write('2,test6\n')
            s.write('3,test5\n')

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments

        db.merge_segments(segments[0], segments[1])

        with open(TEST_BASEPATH + 'test_file-1', 'r') as s:
            segment_lines = s.readlines()
        
        expected_contents = ['1,test5\n', '2,test6\n', '3,test5\n', '4,test6\n']
        self.assertEqual(segment_lines, expected_contents)

        # Check that the second file was deleted
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), False)
    
    def test_rename_segments(self):
        '''
        Tests that a list of segment names with mislabled suffixes are properly
        re-labelled.
        '''
        segments = ['segment-1', 'segment-4', 'segment-6', 'segment-7']
        expected_result = ['segment-1', 'segment-2', 'segment-3', 'segment-4']
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        result = db.rename_segments(segments)
        self.assertEqual(result, expected_result)

    def test_set_threshold(self):
        '''
        Tests that the user can reset the threshold to the value they want.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 500
        db.set_threshold(1000)

        self.assertEqual(db.threshold, 1000)

    def test_save_metadata_saves_metadata(self):
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        segments = ['segment-1', 'segment-2', 'segment-3']
        db.segments = segments

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')
        db.save_metadata()

        with open(db.segments_directory + 'database_metadata', 'rb') as s:
            metadata = pickle.load(s)
        
        self.assertEqual(metadata['current_segment'], TEST_FILENAME)
        self.assertEqual(metadata['segments'], segments)

    def test_load_metadata_segments_at_init(self):
        '''
        Checks that pre-existing segments are loaded into the system at run time.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        segments = ['segment-1', 'segment-2', 'segment-3']
        db.segments = segments
        db.current_segment = segments[-1]

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')
        db.save_metadata() # pickle will be saved
        del db

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.load_metadata()

        self.assertEqual(db.segments, segments)
        self.assertEqual(db.current_segment, segments[-1])

    def test_load_memtable_from_wal(self):
        '''
        Tests that the memtable can be restored from the write-ahead-log.
        '''
        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        # The singleton instance will persist throughout the suite.
        # This test tests its functionality directory, so we need to wipe
        # its state.
        db.memtable_bkup().clear()
        db.db_set('sad', 'mad')
        db.db_set('pad', 'tad')

        del db

        db = SSTable(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.restore_memtable()
        self.assertEqual(db.memtable.contains('sad'), True)
        self.assertEqual(db.memtable.contains('pad'), True)
        self.assertEqual(db.memtable.total_bytes, 16)

if __name__ == '__main__':
    unittest.main()