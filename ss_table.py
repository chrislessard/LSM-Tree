from pathlib import Path
from os import remove as remove_file
from os import rename as rename_file

import pickle

class SSTable():
    current_segment = None
    current_segment_size = None
    segments = None
    threshold = None
    index = None

    def __init__(self, database_name, segments_dir_name):
        self.segments_dir_name = segments_dir_name
        self.current_segment = database_name
        self.segments = [self.current_segment]
        self.current_segment_size = 0

        self.threshold = 1000000
        self.index = {}

        if not (Path(segments_dir_name).exists() and Path(segments_dir_name).is_dir):
            Path(segments_dir_name).mkdir()
        else:
            pass

    def db_set(self, key, value):
        ''' (self, str, str) -> None
        Stores a new key value pair in the DB
        '''
        log = self.log_entry(key, value)
        # Check if we need a new segment
        log_size = len(log)
        if self.current_segment_size + log_size > self.threshold:
            new_seg_name = self.new_segment_name()
            self.current_segment = new_seg_name
            self.current_segment_size = 0
            self.segments.append(self.current_segment)

        with open(self.full_path(), 'a') as s:
            # Offset is fetched as current number of bytes in file up until this write
            offset = Path(self.full_path()).stat().st_size
            self.index[key] = offset
            s.write(log)

        self.current_segment_size += log_size

    def db_get(self, key):
        ''' (self, str) -> None
        Retrieve the value associated with key in the db
        '''
        offset = self.is_indexed(key)
        segments = self.segments[:]

        while len(segments):
            segment = segments.pop()
            segment_path = self.segments_dir_name + segment

            val = None
            with open(segment_path, 'r') as s:
                # Todo: generalize offset across all segments
                if offset and segment == self.current_segment:
                    s.seek(offset)
                    line = s.readline()
                    k, v = line.split(',')
                    if k == key:
                        val = v.strip()
                else:
                    for line in s:
                        k, v = line.split(',')
                        if k == key:
                            val = v.strip()
            if val:
                return val

    def is_indexed(self, key):
        ''' (self, int) -> Bool
        Checks whether key is stored in the DB's index
        '''
        return self.index[key] if key in self.index.keys() else None

    def load_index(self):
        ''' (self) -> None
        Parses the database file and loads keys into the index. 
        Only retrieves values for the current segment. Warning, this is really slow!
        '''
        byte_count = 0
        with open(self.full_path(), 'r') as s:
            for line in s:
                k, v = line.split(',')
                self.index[k] = byte_count
                byte_count += len(line)

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
            seg1_size = Path(self.segments_dir_name + seg1).stat().st_size
            seg2_size = Path(self.segments_dir_name + seg2).stat().st_size

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

        self.current_segment = self.segments[-1]
        self.current_segment_size = Path(self.segments_dir_name + self.current_segment).stat().st_size

    def rename_segment_files(self, result):
        ''' (self) -> [str]
        Renames the segment files on disk to make sure that their suffixes are 
        in proper ascending order.
        '''
        corrected_names = self.rename_segments(result)
        for idx, segment in enumerate(result):
            old_path = self.segments_dir_name + segment
            new_path = self.segments_dir_name + corrected_names[idx]
            rename_file(old_path, new_path)

        return corrected_names

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

    def merge_segments(self, segment1, segment2):
        ''' (self, str, str) -> str
        Concatenates the contents of the files represented byt segment1 and
        segment 2, erases the second segment file and returns the name of the
        first segment. 
        '''
        path1 = self.segments_dir_name + segment1
        path2 = self.segments_dir_name + segment2

        with open(path1, 'a') as s1:
            with open(path2, 'r') as s2:
                for line in s2:
                    s1.write(line)

        remove_file(path2)
        return segment1

    def save_index_snapshot(self, name):
        ''' (self, str) -> None
        Saves a pickle dump of the current index to disk, calling the file name.
        '''
        with open(self.full_path(), 'wb') as snapshot_file_stream:
            pickle.dump(self.index, snapshot_file_stream)

    def load_index_snapshot(self, name):
        ''' (self, str) -> None
        Loads a snapshot of the index from disk to memory, using file name.
        '''
        with open(self.full_path(), 'rb') as snapshot_file_stream:
            self.index = pickle.load(snapshot_file_stream)

    # Helper methods
    def full_path(self):
        return self.segments_dir_name + self.current_segment

    def new_segment_name(self):
        ''' (self) -> None
        Calculate the name of incrementing the current segment.
        '''
        name, number = self.current_segment.split('-')
        new_number = str(int(number) + 1)

        return '-'.join([name, new_number])
    
    def log_entry(self, key, value):
        '''(str, str) -> str
        Converts a key value pair into a comma seperated newline delimited
        log entry.
        '''
        return str(key) + ',' + (value) + '\n'

    def compact_segment(self, segment_name):
        keys = {}
        with open(self.segments_dir_name + segment_name, 'r') as s:
            for line in s:
                k, v = line.split(',')
                keys[k] = v

        with open(self.segments_dir_name + segment_name, 'w') as s:
            for key, val in keys.items():
                log = self.log_entry(key, val.strip())
                s.write(log)
