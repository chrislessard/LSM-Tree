import unittest
import os
from pathlib import Path
from augmented_log import AugmentedLog

TEST_FILENAME = 'test_file-1'
TEST_BASEPATH = 'test-segments/'
TESTPATH = TEST_BASEPATH + TEST_FILENAME

class TestAugmentedLog(unittest.TestCase):
    def setUp(self):
        if not (Path(TEST_BASEPATH).exists() and Path(TEST_BASEPATH).is_dir):
            Path(TEST_BASEPATH).mkdir()

    def tearDown(self):
        for filename in os.listdir(TEST_BASEPATH):
            os.remove(TEST_BASEPATH + filename)

    # db_set
    def test_db_set_stores_pair(self):
        '''
        Tests the db_set functionality.
        '''
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')

        with open(TESTPATH, 'r') as s:
            line1 = s.readline().strip()
            line2 = s.readline().strip()

        self.assertEqual(line1, '1,test1')
        self.assertEqual(line2, '2,test2')

    def test_db_set_stores_index(self):
        '''
        Tests that indexes are stored upon db_set call.
        '''
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')

        self.assertEqual(db.index['1'], 0)
        self.assertEqual(db.index['2'], 8)

    def test_db_set_index_retrieves_correct_value(self):
        '''
        Tests that the index stores the correct value.
        '''
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        
        db.db_set('1', 'test1')
        db.db_set('2', 'test2')
        db.db_set('3', 'test3')
        db.db_set('4', 'test4')

        with open(TESTPATH, 'r') as s:
            s.seek(db.index['2'])
            self.assertEqual(s.readline(), '2,test2\n')
            s.seek(db.index['4'])
            self.assertEqual(s.readline(), '4,test4\n')

    # db_get
    def test_db_get_retrieve_val(self):
        '''
        Tests the db_get functionality.
        '''
        with open(TESTPATH, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)

        self.assertEqual(db.db_get('1'), 'test1')
        self.assertEqual(db.db_get('2'), 'test2')
        self.assertEqual(db.db_get('3'), None)
    
    def test_db_get_retrieve_most_recent_val(self):
        '''
        Tests that db_get retrieves the most recent key value.
        '''
        with open(TESTPATH, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('1,new value\n')
        
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        self.assertEqual(db.db_get('1'), 'new value')
    
    def test_db_get_retrieve_val_multiple_segments(self):
        '''
        Tests the db_get functionality.
        '''
        with open(TEST_BASEPATH + 'test_file-1', 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')

        with open(TEST_BASEPATH + 'test_file-2', 'w') as s:
            s.write('3,test3\n')
            s.write('4,test4\n')

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.segments.append('test_file-2')

        self.assertEqual(db.db_get('1'), 'test1')
        self.assertEqual(db.db_get('3'), 'test3')

    # load_index
    def test_load_index_correctly_creates_hash(self):
        '''
        Tests that load_index correctly maps keys in the current segment to offsets
        in the file on disk
        '''
        with open(TEST_BASEPATH + TEST_FILENAME, 'w') as s:
            s.write('1,test1\n')
            s.write('2,test2\n')
            s.write('3,test3\n')
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.load_index()

        expected_index = {'1': 0, '2':8, '3': 16}
        self.assertEqual(db.index, expected_index)

    # segments
    def test_db_set_uses_segment(self):
        '''
        Tests that new segments are created and used when the threshold is reached.
        '''
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.threshold = 10
        db.db_set('abc', 'cba')
        db.db_set('def', 'fed') # This will cross the threshold

        self.assertEqual(db.current_segment_size, 8)
        self.assertEqual(db.current_segment, 'test_file-2')

    # compaction
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

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
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

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.segments = segments
        db.compact()

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        expected_result = ['1,test3\n', '2,test4\n', '1,test7\n', '2,test8\n']

        self.assertEqual(first_segment_lines, expected_result)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), False)

        # Test that the db's current segment size tracker is set correctly
        result_size = len('1,test1\n') * 4
        self.assertEqual(db.current_segment_size, result_size)

        self.assertEqual(db.current_segment, segments[0])

    def test_compact_odd_number_multiple_segments(self):
        '''
        Tests that multiple segments can be compacted.
        '''
        segments = ['test_file-1', 'test_file-2', 'test_file-3']
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

        with open(TEST_BASEPATH + segments[2], 'w') as s:
            s.write('1,test9\n')
            s.write('2,testa\n')
            s.write('1,testb\n')
            s.write('2,testc\n')

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.segments = segments
        db.compact()

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        expected_result = [
            '1,test3\n', 
            '2,test4\n', 
            '1,test7\n', 
            '2,test8\n', 
            '1,testb\n', 
            '2,testc\n'
        ]

        self.assertEqual(first_segment_lines, expected_result)

        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), False)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[2]), False)

        # Test that the db's current segment size tracker is set correctly
        result_size = len('1,test1\n') * 6
        self.assertEqual(db.current_segment_size, result_size)
        self.assertEqual(db.current_segment, segments[0])

    def test_compact_multiple_segments_with_threshold(self):
        '''
        Tests that the compaction algorithm correctly factors in the segment
        size threshold when merging segments.
        '''
        segments = ['test_file-1', 'test_file-2', 'test_file-3']
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

        with open(TEST_BASEPATH + segments[2], 'w') as s:
            s.write('1,test9\n')
            s.write('2,testa\n')
            s.write('1,testb\n')
            s.write('2,testc\n')

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.segments = segments
        db.threshold = 8 * 4
        db.compact()

        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[0]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[2]), False)

        first_seg_expected_vals = ['1,test3\n', '2,test4\n', '1,test7\n', '2,test8\n']
        scnd_seg_expected_vals = ['1,testb\n', '2,testc\n']

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

        with open(TEST_BASEPATH + segments[1], 'w') as s:
            s.write('1,test5\n')
            s.write('2,test6\n')

        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.segments = segments

        db.merge_segments(segments[0], segments[1])

        with open(TEST_BASEPATH + 'test_file-1', 'r') as s:
            segment_lines = s.readlines()
        
        expected_contents = ['1,test1\n', '2,test2\n', '1,test5\n', '2,test6\n']
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
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        result = db.rename_segments(segments)
        self.assertEqual(result, expected_result)

    def test_set_threshold(self):
        '''
        Tests that the user can reset the threshold to the value they want.
        '''
        db = AugmentedLog(TEST_FILENAME, TEST_BASEPATH)
        db.threshold = 500
        db.set_threshold(1000)

        self.assertEqual(db.threshold, 1000)

if __name__ == '__main__':
    unittest.main()