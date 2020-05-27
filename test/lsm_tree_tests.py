import unittest
import os
import pickle
from pathlib import Path
from src.lsm_tree import LSMTree
from src.red_black_tree import RedBlackTree

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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        
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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable.add('chris', 'lessard')
        db.memtable.add('daniel', 'lessard')
        db.flush_memtable_to_disk(TESTPATH)

        with open(TESTPATH, 'r') as s:
            lines = s.readlines()

        expected_lines = ['chris,lessard\n', 'daniel,lessard\n']
        self.assertEqual(lines, expected_lines)

    def test_db_set_writes_to_wal(self):
        '''
        Tests that db_set invocations write the values to the write-ahead-log.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        
        # The singleton instance will persist throughout the suite.
        # We need to clear the instance explicitely in order to make sure that values from old tests don't persist.
        db.memtable_wal().clear()
        open(TEST_BASEPATH + BKUP_NAME, 'w').close()

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')

        self.assertEqual(Path(TEST_BASEPATH + BKUP_NAME).exists(), True)
        with open(TEST_BASEPATH + BKUP_NAME, 'r') as s:
            lines = s.readlines()
        
        self.assertEqual(len(lines), 2)

    def test_key_update_does_not_increment_memtable_total_bytes(self):
        '''
        Tests that adding a new value for a key that already exists in the
        memtable does not change the value of the threshold.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.db_set('mr', 'bean')
        self.assertEqual(db.memtable.total_bytes, 6)
        db.db_set('mr', 'toast')
        self.assertEqual(db.memtable.total_bytes, 6)

    # db_get
    def test_db_get_single_val_retrieval(self):
        '''
        Tests the retrieval of a single value written into the db
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.db_set('chris', 'lessard')
        val = db.db_get('chris')

        self.assertEqual(val, 'lessard')
    
    def test_db_get_low_threshold(self):
        '''
        Tests the db_get functionality when the db threshold is low.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)

        db.db_set('chris', 'lessard')
        db.db_set('chris', 'martinez')
        self.assertEqual(db.db_get('chris'), 'martinez')
    
    def test_db_get_retrieve_most_recent_val_multiple_thresholds(self):
        '''
        Tests that db_get retrieves the most recent key value.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10

        db.db_set('chris', 'lessard')
        db.db_set('chris', 'martinez')
        self.assertEqual(db.db_get('chris'), 'martinez')
    
    def test_db_get_retrieve_val_multiple_segments(self):
        '''
        Tests the db_get functionality.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')

        db.db_set('chris', 'martinez')
        db.db_set('a', 'b')
        db.db_set('a', 'c')

        self.assertEqual(db.db_get('chris'), 'martinez')
        self.assertEqual(db.db_get('daniel'), 'lessard')
        self.assertEqual(db.db_get('a'), 'c')

    def test_get_segment_path(self):
        ''' 
        Tests that the segment path can be retrieved for any segment
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        self.assertEqual(db.segment_path('segment1'), TEST_BASEPATH + 'segment1')
        self.assertEqual(db.segment_path('segment5'), TEST_BASEPATH + 'segment5')

    # segments
    def test_db_set_uses_segment(self):
        '''
        Tests that new segments are created and used when the threshold is reached.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 10
        db.db_set('abc', 'cba')
        db.db_set('def', 'fed') # This will cross the threshold

        self.assertEqual(db.memtable.total_bytes, 6)
        self.assertEqual(db.current_segment, 'test_file-2')

    # Merging algorithm
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

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments

        db.merge_into_first_segment(segments[0], segments[1])

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
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        result = db.rename_segments(segments)
        self.assertEqual(result, expected_result)

    def test_set_threshold(self):
        '''
        Tests that the user can reset the threshold to the value they want.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.threshold = 500
        db.set_threshold(1000)

        self.assertEqual(db.threshold, 1000)

    def test_save_metadata_saves_metadata(self):
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        segments = ['segment-1', 'segment-2', 'segment-3']
        db.segments = segments

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')
        db.bf_active = True
        db.bf_false_pos_prob = 0.5
        db.bf_num_items = 100
        db.save_metadata()

        with open(db.segments_directory + 'database_metadata', 'rb') as s:
            metadata = pickle.load(s)
        
        self.assertEqual(metadata['current_segment'], TEST_FILENAME)
        self.assertEqual(metadata['segments'], segments)
        self.assertTrue(metadata['bf_active'])
        self.assertEqual(metadata['bf_false_pos'], 0.5)
        self.assertEqual(metadata['bf_num_items'], 100)

    def test_load_metadata_segments_at_init(self):
        '''
        Checks that pre-existing segments are loaded into the system at run time.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        segments = ['segment-1', 'segment-2', 'segment-3']
        db.segments = segments
        db.current_segment = segments[-1]

        db.db_set('chris', 'lessard')
        db.db_set('daniel', 'lessard')
        db.bf_active = True
        db.bf_false_pos_prob = 0.5
        db.bf_num_items = 100
        db.save_metadata() # pickle will be saved
        del db

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.load_metadata()

        self.assertEqual(db.segments, segments)
        self.assertEqual(db.current_segment, segments[-1])
        self.assertTrue(db.bf_active)
        self.assertEqual(db.bf_false_pos_prob, 0.5)
        self.assertEqual(db.bf_num_items, 100)

    def test_load_memtable_from_wal(self):
        '''
        Tests that the memtable can be restored from the write-ahead-log.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)

        # The singleton instance will persist throughout the suite.
        # We need to clear the instance explicitely in order to make sure that values from old tests don't persist.
        db.memtable_wal().clear()
        db.db_set('sad', 'mad')
        db.db_set('pad', 'tad')

        del db
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable = RedBlackTree()

        db.restore_memtable()
        self.assertEqual(db.memtable.contains('sad'), True)
        self.assertEqual(db.memtable.contains('pad'), True)
        self.assertEqual(db.memtable.total_bytes, 12)

    def test_load_memtable_from_wal_keeps_update(self):
        '''
        Tests that updates persist when reloading the memtable 
        from the write-ahead-log.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)

        # The singleton instance will persist throughout the suite.
        # We need to clear the instance explicitely in order to make sure that values from old tests don't persist.
        db.memtable_wal().clear()
        db.db_set('chris', 'lessard')
        db.db_set('chris', 'hemsworth')

        with open(db.memtable_wal_path(), 'r') as s:
            lines = s.readlines()
        
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0], 'chris,lessard\n')
        self.assertEqual(lines[1], 'chris,hemsworth\n')

        del db
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable = RedBlackTree()

        db.restore_memtable()
        self.assertTrue(db.memtable.contains('chris'), True)
        self.assertEqual(db.db_get('chris'), 'hemsworth')


    def test_initializing_db_loads_metadata_and_memtable(self):
        '''
        Tests that initializing an new instance of the database loads
        metadata and memtable info.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.memtable_wal().clear()

        segments = ['segment1', 'segment2', 'segment3', 'segment4', 'segment5']
        db.segments = segments
        db.current_segment = 'segment5'

        db.db_set('french', 'francais')
        db.db_set('english', 'anglais')
        db.db_set('spanish', 'espagnole')

        db.save_metadata()

        del db

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)

        self.assertEqual(db.segments, segments)
        self.assertEqual(db.current_segment, 'segment5')
        self.assertTrue(db.memtable.contains('french'))
        self.assertTrue(db.memtable.contains('english'))
        self.assertTrue(db.memtable.contains('spanish'))

    # Index
    def test_set_db_sparsity_factor(self):
        '''
        Tests that the sparsity of the database's index can be retrieved.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_sparsity_factor(10)
        self.assertEqual(db.sparsity_factor, 10)

        db.set_sparsity_factor(1000)
        self.assertEqual(db.sparsity_factor, 1000)

    def test_fetch_db_sparsity(self):
        '''
        Tests that the sparsity of the database's index can be retrieved.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(1000000)
        db.set_sparsity_factor(100)
        self.assertEqual(db.sparsity(), 10000)

        db.set_threshold(100)
        db.set_sparsity_factor(4)
        self.assertEqual(db.sparsity(), 25)

        db.set_threshold(100)
        db.set_sparsity_factor(7)
        self.assertEqual(db.sparsity(), 14)

    # Flush memtable
    def test_flush_memtable_populates_index(self):
        '''
        Tests that flushing the memtable to disk populates the index.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)
        db.set_sparsity_factor(25)

        db.db_set('abc', '123')
        db.db_set('def', '456')
        db.db_set('ghi', '789')
        db.db_set('jkl', '012')
        db.db_set('mno', '345')
        db.db_set('pqr', '678')
        db.db_set('stu', '901')
        db.db_set('vwx', '234')

        db.flush_memtable_to_disk(TESTPATH)

        in_order = db.index.in_order()
        self.assertEqual(len(in_order), 2)
        self.assertTrue(db.index.contains('jkl'))
        self.assertTrue(db.index.contains('vwx'))

    def test_memtable_writes_most_recent_keys(self):
        '''
        Tests that the memtable only flushes the most recent values of 
        keys to disk.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)

        db.db_set('abc', '123')
        db.db_set('abc', 'ABC')
        db.db_set('def', '345')
        db.db_set('def', 'DEF')
        db.db_set('ghi', '567')
        db.db_set('ghi', 'GHI')

        db.flush_memtable_to_disk(TESTPATH)

        with open(TESTPATH, 'r') as s:
            lines = s.readlines()
        
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0], 'abc,ABC\n')
        self.assertEqual(lines[1], 'def,DEF\n')
        self.assertEqual(lines[2], 'ghi,GHI\n')

    def test_flush_memtable_stores_segment_in_index(self):
        '''
        Tests that flushing the memtable to disk populates the index and stores
        the current segments within each node.s
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)
        db.set_sparsity_factor(25)

        db.db_set('abc', '123')
        db.db_set('def', '456')
        db.db_set('ghi', '789')
        db.db_set('jkl', '012')

        db.flush_memtable_to_disk(TESTPATH)

        db.db_set('mno', '345')
        db.db_set('pqr', '678')
        db.db_set('stu', '901')
        db.db_set('vwx', '234')

        # Simulate crossing threshold
        db.segments = ['test_file-1', 'test_file-2']
        db.current_segment = 'test_file-2'
        db.flush_memtable_to_disk(TESTPATH)

        segment1 = db.index.find_node('jkl').segment
        segment2 = db.index.find_node('vwx').segment
        
        self.assertEqual(segment1, 'test_file-1')
        self.assertEqual(segment2, 'test_file-2')

    def test_flush_memtable_stores_correct_index_offsets(self):
        '''
        Tests that flushing the memtable to disk stores the correct
        offsets into disk in the index.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)
        db.set_sparsity_factor(25)

        db.db_set('abc', '123')
        db.db_set('def', '456')
        db.db_set('ghi', '789')
        db.db_set('jkl', '012')
        db.db_set('mno', '345')
        db.db_set('pqr', '678')
        db.db_set('stu', '901')
        db.db_set('vwx', '234')

        db.flush_memtable_to_disk(TESTPATH)

        offset1 = db.index.find_node('jkl').offset
        offset2 = db.index.find_node('vwx').offset

        self.assertEqual(offset1, 24)
        self.assertEqual(offset2, 56)

        with open(TESTPATH, 'r') as s:
            s.seek(offset1)
            line1 = s.readline()
            s.seek(offset2)
            line2 = s.readline()

        self.assertEqual(line1, 'jkl,012\n')
        self.assertEqual(line2, 'vwx,234\n')

    # Index
    def test_retrieve_value_from_index(self):
        '''
        Tests that indexed values can be retrieved appropriately
        from disk when there is one segment.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)
        db.set_sparsity_factor(25)

        db.db_set('abc', '123')
        db.db_set('def', '456')
        db.db_set('ghi', '789')
        db.db_set('jkl', '012')

        db.flush_memtable_to_disk(TESTPATH)

        offset1 = db.index.find_node('jkl').offset

        with open(TESTPATH, 'r') as s:
            s.seek(offset1)
            line1 = s.readline()

        key, value = line1.strip().split(',')
        self.assertEqual(key, 'jkl')
        self.assertEqual(value, '012')

    def test_retrieve_values_from_index(self):
        '''
        Tests that indexed values can be retrieved appropriately
        from disk when there are multiple segments.
        '''
        TESTPATH = TEST_BASEPATH + TEST_FILENAME

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.set_threshold(100)
        db.set_sparsity_factor(25)

        db.db_set('abc', '123')
        db.db_set('def', '456')
        db.db_set('ghi', '789')
        db.db_set('jkl', '012')

        db.flush_memtable_to_disk(TESTPATH)

        db.db_set('mno', '345')
        db.db_set('pqr', '678')
        db.db_set('stu', '901')
        db.db_set('vwx', '234')

        db.flush_memtable_to_disk(TESTPATH)
        'test_file-2'

        # First segment
        segment1 = db.index.find_node('jkl').segment
        offset1 = db.index.find_node('jkl').offset

        with open(TEST_BASEPATH + segment1, 'r') as s:
            s.seek(offset1)
            line1 = s.readline()

        segment1 = db.index.find_node('jkl').segment
        key, value = line1.strip().split(',')
        self.assertEqual(key, 'jkl')
        self.assertEqual(value, '012')

        # Second segment
        segment2 = db.index.find_node('vwx').segment
        offset2 = db.index.find_node('vwx').offset

        with open(TEST_BASEPATH + segment2, 'r') as s:
            s.seek(offset2)
            line2 = s.readline()

        key, value = line2.strip().split(',')
        self.assertEqual(key, 'vwx')
        self.assertEqual(value, '234')

    def test_db_get_uses_index(self):
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        with open(TEST_BASEPATH + 'segment2', 'w') as s:
            s.write('chris,lessard\n')

        db.index.add('chris', 'lessard', offset=0, segment='segment2')

        self.assertEqual(db.db_get('chris'), 'lessard')

    def test_db_get_uses_index_with_floor(self):
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        with open(TEST_BASEPATH + 'segment2', 'w') as s:
            s.write('chris,lessard\n')
            s.write('christian,dior\n')
            s.write('daniel,lessard\n')

        db.index.add('chris', 'lessard', offset=0, segment='segment2')

        self.assertEqual(db.db_get('christian'), 'dior')
        self.assertEqual(db.db_get('daniel'), 'lessard')

    def test_repopulate_index_stores_correst_offsets(self):
        '''
        Tests that the repopulate_index method correctly stores
        offsets to locations of the records on disk.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.index.add('chris', offset=10)
        db.index.add('lessard', offset=52)
        db.segments = ['segment1', 'segment2']

        # Write every two records
        db.threshold = 10
        db.set_sparsity_factor(5) 
        
        with open(TEST_BASEPATH + 'segment1', 'w') as s:
            s.write('red,1\n')
            s.write('blue,2\n')
            s.write('green,3\n')
            s.write('purple,4\n')

        with open(TEST_BASEPATH + 'segment2', 'w') as s:
            s.write('cyan,5\n')
            s.write('magenta,6\n')
            s.write('yellow,7\n')
            s.write('black,8\n')

        db.repopulate_index()

        blue_node = db.index.find_node('blue')
        self.assertEqual(blue_node.offset, 6)

        with open(TEST_BASEPATH + blue_node.segment, 'r') as s:
            s.seek(blue_node.offset)
            line = s.readline()

        self.assertEqual(line, 'blue,2\n')

        magenta_node = db.index.find_node('magenta')
        self.assertEqual(magenta_node.offset, 7)

        with open(TEST_BASEPATH + magenta_node.segment, 'r') as s:
            s.seek(magenta_node.offset)
            line = s.readline()

        self.assertEqual(line, 'magenta,6\n')

    def test_repopulate_index(self):
        '''
        Tests that the index is cleared and repopulated by
        calling the db's repopulate_index method.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.index.add('chris', offset=10)
        db.index.add('lessard', offset=52)
        db.segments = ['segment1', 'segment2']

        # Write every two records
        db.threshold = 10
        db.set_sparsity_factor(5) 

        with open(TEST_BASEPATH + 'segment1', 'w') as s:
            s.write('red,1\n')
            s.write('blue,2\n')
            s.write('green,3\n')
            s.write('purple,4\n')

        with open(TEST_BASEPATH + 'segment2', 'w') as s:
            s.write('cyan,5\n')
            s.write('magenta,6\n')
            s.write('yellow,7\n')
            s.write('black,8\n')

        db.repopulate_index()

        blue_node = db.index.find_node('blue')
        self.assertIsNotNone(blue_node)
        self.assertEqual(blue_node.key, 'blue')

        magenta_node = db.index.find_node('magenta')
        self.assertIsNotNone(magenta_node)
        self.assertEqual(magenta_node.key, 'magenta')

        self.assertFalse(db.index.contains('red'))
        self.assertFalse(db.index.contains('green'))
        self.assertFalse(db.index.contains('cyan'))
        self.assertFalse(db.index.contains('yellow'))

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

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
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

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = segments
        db.threshold = 14 * 2
        db.compact()

        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[0]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[1]), True)
        self.assertEqual(os.path.exists(TEST_BASEPATH + segments[2]), False)

        first_seg_expected_vals = ['1,john\n',
                                   '2,long\n', '3,woot\n', '4,chew\n']
        scnd_seg_expected_vals = ['5,love\n', '6,osrs\n']

        # Check first segment is correct
        with open(TEST_BASEPATH + segments[0], 'r') as s:
            first_segment_lines = s.readlines()

        # Check second segment is correct
        with open(TEST_BASEPATH + segments[1], 'r') as s:
            scnd_segment_lines = s.readlines()

        self.assertEqual(first_segment_lines, first_seg_expected_vals)
        self.assertEqual(scnd_segment_lines, scnd_seg_expected_vals)

    def test_compaction_repopulates_index(self):
        '''
        Tests that the compaction algorithm repopulates the index.
        '''
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = ['segment1', 'segment2', 'segment3']
        # Write every record to index
        db.threshold = 35
        db.sparsity_factor = 35

        with open(TEST_BASEPATH + 'segment1', 'w') as s:
            s.write('chris,lessard\n')
            s.write('john,burman\n')
            s.write('alex,rodriguez\n')

        with open(TEST_BASEPATH + 'segment2', 'w') as s:
            s.write('chris,manson\n')
            s.write('john,alexander\n')
            s.write('alex,rodriguez\n')

        with open(TEST_BASEPATH + 'segment3', 'w') as s:
            s.write('chris,manson\n')
            s.write('john,tidal\n')
            s.write('alex,rodriguez\n')

        db.index.add('chris,lessard', offset='0', segment='segment1')
        db.index.add('john,tidal', offset='13', segment='segment2')
        db.index.add('alex,rodriguez', offset='24', segment='segment3')

        db.compact()

        self.assertEqual(db.index.find_node('chris').value, 'manson')
        self.assertEqual(db.index.find_node('alex').value, 'rodriguez')
        self.assertEqual(db.index.find_node('john').value, 'tidal')

    # New compaction tests
    def test_delete_one_key_from_file(self):
        '''
        Tests that individual keys can be deleted from a given segment file.
        '''
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n',]
        keys = set(['green'])
        file = TEST_BASEPATH + 'test_file-1'

        with open(file, 'w') as s:
            for line in lines:
                s.write(line)

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.delete_keys_from_segment(keys, file)

        with open(file, 'r') as s:
            altered_lines = s.readlines()

        self.assertTrue(altered_lines == ['red,1\n', 'blue,2\n', 'yellow,4\n', ])

    def test_delete_multiple_keys_from_file(self):
        '''
        Tests that individual keys can be deleted from a given segment file.
        '''
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n', ]
        keys = set(['green', 'blue'])
        file = TEST_BASEPATH + 'test_file-1'

        with open(file, 'w') as s:
            for line in lines:
                s.write(line)

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.delete_keys_from_segment(keys, file)

        with open(file, 'r') as s:
            altered_lines = s.readlines()

        self.assertTrue(altered_lines == ['red,1\n', 'yellow,4\n', ])

    def test_delete_keys_from_segments_one_key(self):
        '''
        Tests that delete_keys_from_segments deletes a single key 
        from multiple segments.
        '''
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n', ]
        files = ['test_file-1', 'test_file-2', 'test_file-3']
        keys = set(['green'])

        for file in files:
            with open(TEST_BASEPATH + file, 'w') as s:
                for line in lines:
                    s.write(line)

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.delete_keys_from_segments(keys, files)

        expected_lines = [
            'red,1\n',
            'blue,2\n',
            'yellow,4\n',
        ]
        for file in files:
            with open(TEST_BASEPATH + file, 'r') as s:
                l = s.readlines()
                self.assertEqual(l, expected_lines)

    def test_delete_keys_from_segments_many_keys(self):
        '''
            Tests that delete_keys_from_segments deletes a single key
            from multiple segments.
            '''
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n', ]
        files = ['test_file-1', 'test_file-2', 'test_file-3']
        keys = set(['red', 'green'])

        for file in files:
            with open(TEST_BASEPATH + file, 'w') as s:
                for line in lines:
                    s.write(line)

        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.delete_keys_from_segments(keys, files)

        expected_lines = [
            'blue,2\n',
            'yellow,4\n',
        ]
        for file in files:
            with open(TEST_BASEPATH + file, 'r') as s:
                l = s.readlines()
                self.assertEqual(l, expected_lines)

    def test_new_compact_one_key_to_drop(self):
        '''
        Tets the new compaction algorithm
        '''
        # Prepare the values on disk
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n',]
        files = ['test_file-1', 'test_file-2', 'test_file-3']

        for file in files:
            with open(TEST_BASEPATH + file, 'w') as s:
                for line in lines:
                    s.write(line)

        # Mock the database instance
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = files[:]
        db.activate_bloom_filter()

        for line in lines:
            key, val = line.split(',')
            db.bloom_filter.add(key)

        db.memtable.add('green', '5')

        # Test
        db.new_compaction()
        expected_lines = ['red,1\n', 'blue,2\n', 'yellow,4\n']
        for file in files:
            with open(TEST_BASEPATH + file, 'r') as s:
                l = s.readlines()
                self.assertEqual(l, expected_lines)

    def test_new_compact_multiple_keys_to_drop(self):
        '''
        Tets the new compaction algorithm
        '''
        # Prepare the values on disk
        lines = ['red,1\n', 'blue,2\n', 'green,3\n', 'yellow,4\n', ]
        files = ['test_file-1', 'test_file-2', 'test_file-3']

        for file in files:
            with open(TEST_BASEPATH + file, 'w') as s:
                for line in lines:
                    s.write(line)

        # Mock the database instance
        db = LSMTree(TEST_FILENAME, TEST_BASEPATH, BKUP_NAME)
        db.segments = files[:]
        db.activate_bloom_filter()

        for line in lines:
            key, val = line.split(',')
            db.bloom_filter.add(key)

        db.memtable.add('green', '5')
        db.memtable.add('blue', '5')
        db.memtable.add('red', '5')

        # Test
        db.new_compaction()
        expected_lines = ['yellow,4\n']
        for file in files:
            with open(TEST_BASEPATH + file, 'r') as s:
                l = s.readlines()
                self.assertEqual(l, expected_lines)

if __name__ == '__main__':
    unittest.main()
