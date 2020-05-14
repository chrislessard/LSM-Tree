from pathlib import Path
from os import remove as remove_file, rename as rename_file
from red_black_tree import RedBlackTree
from append_log import AppendLog
import pickle

class SSTable():
    def __init__(self, database_name, segments_directory, memtable_log):
        ''' (self, str, str, str) -> SSTable

        Initialize a new SSTable with:

        - A first segment called database_name
        - A segments directory called segments_directory
        - A memtable log called memtable_log
        '''
        self.segments_directory = segments_directory
        self.memtable_log = memtable_log
        self.current_segment = database_name
        self.segments = []

        # Default threshold is 1mb
        self.threshold = 1000000
        self.memtable = RedBlackTree()

        if not (Path(segments_directory).exists() and Path(segments_directory).is_dir):
            Path(segments_directory).mkdir()

    def db_set(self, key, value):
        ''' (self, str, str) -> None
        Stores a new key value pair in the DB
        '''
        # Check if we need a new segment
        additional_size = len(key) + len(value)
        if self.memtable.total_bytes + additional_size > self.threshold:
            # Flush memtable to disk
            self.flush_memtable(self.full_path())
            self.memtable = RedBlackTree()

            # Clear the log file
            self.memtable_bkup().clear()

            # Update bookkeeping metadata
            self.segments.append(self.current_segment)
            new_seg_name = self.new_segment_name()
            self.current_segment = new_seg_name
            self.memtable.total_bytes = 0

        # Write to memtable backup
        log = self.as_log_entry(key, value)
        self.memtable_bkup().write(log)

        # Write to memtable
        self.memtable.add(key, value)
        self.memtable.total_bytes += additional_size

    def db_get(self, key):
        ''' (self, str) -> None
        Retrieve the value associated with key in the db
        '''
        # Attempts to find the key in the memtable first
        memtable_result = self.memtable.find_node(key)
        
        if memtable_result:
            return memtable_result.value

        segments = self.segments[:]
        while len(segments):
            segment = segments.pop()
            segment_path = self.segments_directory + segment

            val = None
            with open(segment_path, 'r') as s:
                for line in s:
                    k, v = line.split(',')
                    if k == key:
                        return v.strip()

    def compact(self):
        ''' (self) -> None
        Performs the compaction algorithm on the database segments.
        '''
        for segment in self.segments:
            self.compact_segment(segment)

        # Reduce segments
        compacted = []
        segments = self.segments[:]

        while len(segments) > 1:
            # pop two segs from segments
            seg1, seg2 = segments.pop(0), segments.pop(0)

            # check if their total size exceeds the threshold
            seg1_size = Path(self.segments_directory + seg1).stat().st_size
            seg2_size = Path(self.segments_directory + seg2).stat().st_size
            total = seg1_size + seg2_size

            if total > self.threshold:
                # add seg1 segment to compacted and seg2 back to start of segments
                compacted.append(seg1)
                segments = [seg2] + segments
            else:
                # merge the two segments, conserving the name of the seg1 one
                merged_segment = self.merge_segments(seg1, seg2)
                segments = [merged_segment] + segments

        result = compacted + segments

        # the leftover segments wont be ordered properly by name
        self.segments = self.rename_segment_files(result)

    def set_threshold(self, threshold):
        ''' (self, int) -> None
        Sets the threshold - the point at which a new segment is created
        for the database. The argument, threshold, is measured in bytes.
        '''
        self.threshold = threshold

    def merge_segments(self, segment1, segment2):
        ''' (self, str, str) -> str
        Concatenates the contents of the files represented byt segment1 and
        segment 2, erases the second segment file and returns the name of the
        first segment. 
        '''
        path1 = self.segments_directory + segment1
        path2 = self.segments_directory + segment2
        new_path = self.segments_directory + 'temp'

        with open(new_path, 'w') as s0:
            with open(path1, 'r') as s1:
                with open(path2, 'r') as s2:
                    line1, line2 = s1.readline(), s2.readline()
                    while not (line1 == '' and line2 == ''):
                        # At the end of the file stream we'll get the empty str
                        key1, key2 = line1.split(',')[0], line2.split(',')[0]

                        if key1 == '' or key1 == key2:
                            s0.write(line2)
                            line1 = s1.readline()
                            line2 = s2.readline()
                        elif key2 == '' or key1 < key2:
                            s0.write(line1)
                            line1 = s1.readline()
                        else:
                            s0.write(line2)
                            line2 = s2.readline()

        # Remove old segments and replaced first segment with the new one
        remove_file(path1)
        remove_file(path2)
        rename_file(new_path, path1)

        return segment1

    def save_memtable_snapshot(self, name):
        ''' (self, str) -> None
        Saves a pickle dump of the current memtable to disk, calling the file name.
        '''
        with open(self.full_path(), 'wb') as snapshot_file_stream:
            pickle.dump(self.memtable, snapshot_file_stream)

    def load_memtable_snapshot(self, name):
        ''' (self, str) -> None
        Loads a snapshot of the memtable from disk to memory, using file name.
        '''
        with open(self.full_path(), 'rb') as snapshot_file_stream:
            self.memtable = pickle.load(snapshot_file_stream)

    def flush_memtable(self, path):
        ''' (self, str) -> None
        Writes the contents of the current memtable to disk and wipes the current memtable.
        '''
        traversal = self.memtable.in_order()
        with open(path, 'w') as s:
            for node in traversal:
                log = self.as_log_entry(node.key, node.value)
                s.write(log)

    # Helper methods
    def full_path(self):
        return self.segments_directory + self.current_segment

    def new_segment_name(self):
        ''' (self) -> None
        Calculate the name of incrementing the current segment.
        '''
        name, number = self.current_segment.split('-')
        new_number = str(int(number) + 1)

        return '-'.join([name, new_number])

    def rename_segments(self, seg_list):
        ''' (self, [str]) -> [str]
        Renames the segments to make sure that their suffixes are in proper
        ascending order.
        '''
        result = []

        for i in range(len(seg_list)):
            name = seg_list[i]
            name_decomp = name.split('-')
            name_decomp[-1] = str(i + 1)
            result.append('-'.join(name_decomp))

        return result

    def rename_segment_files(self, result):
        ''' (self) -> [str]
        Renames the segment files on disk to make sure that their suffixes are 
        in proper ascending order.
        '''
        corrected_names = self.rename_segments(result)
        for idx, segment in enumerate(result):
            old_path = self.segments_directory + segment
            new_path = self.segments_directory + corrected_names[idx]
            rename_file(old_path, new_path)

        return corrected_names

    def as_log_entry(self, key, value):
        '''(str, str) -> str
        Converts a key value pair into a comma seperated newline delimited
        log entry.
        '''
        return str(key) + ',' + (value) + '\n'

    def compact_segment(self, segment_name):
        ''' (self, str) -> None
        Compacts the single segment named segment_name.
        '''
        keys = {}
        with open(self.segments_directory + segment_name, 'r') as s:
            for line in s:
                k, v = line.split(',')
                keys[k] = v

        with open(self.segments_directory + segment_name, 'w') as s:
            for key, val in keys.items():
                log = self.as_log_entry(key, val.strip())
                s.write(log)

    def memtable_bkup(self):
        ''' (self) -> str
        Returns the full path to the memtable backup.
        '''
        file_path = self.segments_directory + self.memtable_log
        return AppendLog.instance(file_path)

# Basic benchmarking code

if __name__ == "__main__":
    import timeit

    setup = """
from __main__ import SSTable
db = SSTable('test_file-1', 'segments/', 'bkup')
"""
    def benchmark_store(db):
        for i in range(100000):
            db.db_set('chris', 'lessard')

    def benchmark_get(db):
        db.db_get('daniel')
        
    benchmark_store_str = """benchmark_store(db)"""
    benchmark_get_str = """
db.db_set('daniel', 'lessard')
benchmark_get(db)
"""

    print('Store benchmark:', timeit.timeit(benchmark_store_str, setup=setup, number=1))
    print('Get benchmark:', timeit.timeit(benchmark_get_str, setup=setup, number=1))
