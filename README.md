# SSTable-POC

To read the article associated with this codebase, visit [the link here](https://www.notion.so/Implementing-a-basic-SSTable-363b7bbd98674291ba80edd1d61d8a0a)

## Dependencies

You'll need python3 in order to run this code.

## Project use and description

The project contains three implementations of key-values stores. To use one of them, cd into its directory and invoke `python3 main.py`. If you would like to run the test suite, cd into the directory and invoke `python3 *_tests.py`.

All versions of the store will create csv files on disk. These represent the persistent versions of they key value store. The AugmentedLog and SSTable will additionally place these .csv files in a directory called segments.

### basic_log

This is an extremely key value store, supporting two commands:

1. store {key} {data} : store the pair (key, value) in the database
2. get {key} : retrieve the most recent value on disk associated with key

### augmented_log

This is an improved version of the basic_log, supporting some functionality common to SSTables, namely the creation, merging and compaction of segments. The interface is as follows:

Which will provide an interface that is ready to receive the following commands:

1. store {key} {data} : store the pair (key, value) in the database
2. get {key} : retrieve the most recent value on disk associated with key
3. compact_segments : run the compact and merge algorithm on the segments on disk.
4. load_index : scan the current segment, and load its values into the index. this requires a linear parse of the file and is slow!
5. save_index_snapshot {filename} : save a snapshot of the index to disk, as a pickle dump named {filename}
6. load_index_snapshot {filename} : load an aforementioned snapshot on the index to disks
7. set_threshold {number of bytes} : set the new segment threshold for the db in bytes

The main improvement over the basic_log comes in the use of an index to perform fast lookups on key values pairs, and the introduction of segments and the compaction algorithm to reclaim diskspace and speed up reads.

### ss_table

This is a basic implementation of a a Sorted String Table (SSTable). It uses the augmented log as its basis, making one key change: it guarantees that segments are sorted by key.

It also introduces a memtable into memory, which is a RedBlack tree of nodes containing key-value pairs. Thanks to [stanislavkozlovski](https://github.com/stanislavkozlovski/Red-Black-Tree/blob/master/rb_tree.py) for the implementation. I've augmented it to support keys and values, as well as support the update operation and provide an inorder traversal of the nodes for the purpose of writing the memtable to disk.

You'll notice when you use the SSTable that segments aren't actively being created like they were for the other two implementations. This is because the memtable is only flushed to disk when the total size of the key/value pairs stored exceeds the segment treshold.

Since the memtable isn't persistent, we back it up to disk on each operation using the standard append-only log, which serves solely the purpose of re-populating the memtable in the event of a system failure.

The merging algorithm implementation is different, since it can leverage the fact that segments are sorted.

## Notes

Here are some places where this proof of concept implementation of an SSTable could be improved:

### Segment file contents

Its not the best idea to store the contents of the file as a CSV, this has just been done for simplicity's sake. In practice, a more appropriate approach is to store the length of the key-value string in bytes, followed by the raw string. This would allow us to drop the newlines from the stores, and would probably be a nicer approach in more low-level languages (like C) that don't handle escaping for us.  

Also, under the current implementation, values can't contain a comma since it would require an extra amount of application logic to deal with. In other words, a real implementation uses character escaping.

### Memory

Since we are hopping between disk and memory throughout the project, we need be careful with the later. In python3, the following is considered to be "safe" and pythonic from a memory-perspective:

``` python
with open("file") as stream:
    for line in stream:
        print(line)
```

Which is why it is used throughout.

We need to be careful not to overload the system memory when implementing the key-value store. *Designing Data Intensive Applications.* recommends that each segment be a few megabytes in size. The default segment size in the project is 1MB. In general, it's a good idea to set the max segment size to be below the limitations of your system's memory. If not, in a worst case scenario, an algorithm like this one:

``` python
def load_index(self):
    byte_count = 0
    with open(self.full_path(), 'r') as s:
        for line in s:
            k, v = line.split(',')
            self.index[k] = byte_count
            byte_count += len(line)
```

could run out of memory.

You might remark that the db_get implementation is extremly malperformant when the key is not present in the index. This is due to the fact that the algorithm scans through an entire segment to try and find the value, which fails to leverage the fact that new values will always be at the end of the file. Therefore, it would be smarter to parse the file in reverse. Unfortunately, we cannot the "obvious" python solution:

``` python
for line in reversed(list(open("filename"))):
    print(line.rstrip())
```

This doesn't scale to large file sizes. Since we limit the segment size then, in theory, there is no harm in adding it to the project. I've left it out choosing to instead discuss the point here. Another solution using generators was [proposed online as well](https://stackoverflow.com/a/23646049).

### Parallelism

Among some of the nit-picks, the main thing that seperates this implementation from a real SSTable is the lack of parallelism in this version. Normally, while the database runs, seperate threads tend to reading and writing new key-value pairs, while background threads periodically run the compaction algorithm to save disk space. The merging and compaction algorithms don't support asynchronicity. Under this simplified implementation, the user has to invoke the compact algorithm themself.

In order to properly support asynchronicity, the implementation would need to be changed. Under the current implementation, merging involves compacting all the segments individually, then concatenating the segments in pairs of two until it is no longer possible. This process is slow, and prevents and reading or writing from happening until it is finished. Even if it were to occur in the background, it would pose a problem for the reader/writer thread trying to access the segments being merged.

To solve this problem, we would borrow a notion that is common in the world of database implementations: we would duplicate the segments to-be-compacted into redundant "outdated" versions that the readers and writers would continue to use. We would run the compaction, and then address the discrepencies between the newly compacted segments and the obsolete ones. As you might imagine, syncing the old and new versions is easier for reader threads than for writer threads, and the correct approach to solving this problem is valid grounds for a project of its own.

Configuring the compaction threads to run at the most opportune times (when volumes of reads and writes are low) is also a non-trivial problem.

More generally, all disk operations would benefit from parallelism. Since disk access time is very slow when compared, it may be a good idea to enqueue a seperate worker thread to perform the write while the main thread continue to look for new reads.

## Miscellaneous improvements

### Bug: Infer the number of segments already on disk

The functionality currently works fine for situations where the user has not left the interface. If they do, then load it up again, the AugmentedTable and SSTable should scan the segments directory to infer how many there are and which one it should use.

### Testing framework

Unnittest is a bit basic and the tests contain a lot of redundancy. In a proper implementation, it would be better to port the tests to a more complete testing framework. It would also be nice to see if there isn't a better way to test disk operations.

### Testing by using class methods

A lot of the tests were set up by manually creating files and calling functions. For instance, this was done in a case where a segment would be manually written to disk, rather than by calling db_set several times over. Generally speaking, I think it's ok to use internal methods in other unittests as long as those method's contracts don't change (i.e. they allow for refactoring but not full on redefinition).

### How often we write to disk

In the basic implementation of the key value store, we wrote to disk as soon as we could - every time a new value came in. In the SSTable approach, we prefer periodic flushes to disk to as to avoid regularly incurring the lag of connecting to the disk. The caveat in this strategy is that should our system crash, we lose the memtable.

### Adding a sparse index to the memory table

We can further improve the SSTable by adding a sparse index. It would only store some keys. If we cant find a value we're searching for in the index, we could leverage it to determine the appropriate range ('Christina' would be stored between 'Chris' and 'Daniel', for instance). Using the lower part of this range, we could read into disk and begin scanning through to the other boundary, since we know that the keys are sorted.
