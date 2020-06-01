# LSMTree-POC

To read an old article that goes into the process behind developing this project, visit [the link here](https://www.notion.so/Implementing-a-basic-LSMTree-363b7bbd98674291ba80edd1d61d8a0a). I also wrote an article about compaction in LSM-Trees [here](https://medium.com/@chris.lessard.96/lsm-trees-technical-optimizations-for-compaction-and-disk-reclamation-66960631714e).

## Dependencies

You'll need python3 in order to run this code.

## Project use and description

This project is a simple key-value store database, implemented as a single-threaded LSM-Tree.

You can run the database on your system by checking out the code and invoking `python main.py`. The program will create csv files on disk in a directory called 'segments'. Metadata and a write-ahead-log are maintained in this directory on top of Sorted String Tables. These contain the persisted records. Tests and benchmark code use their own directories.

Similarly, you can port the project into your own code and create an instance of the LSMTree. The index sparsity, memtable threshold and bloom filter parameters can be configured through the folling methods:

```
    set_threshold(threshold)
    set_sparsity_factor(factor)
    def set_bloom_filter_num_items(num_items)
    def set_bloom_filter_false_pos_prob(probability)
```

Once those are configured, you can interface the DB with the following two commands:

```
    db_set(key, value)
    db_get(key)
```

The details are explained below.

## About

The DB is an implementation of a Log Structured Merge Tree (LSMTree). It uses the [augmented log](https://github.com/chrislessard/augmented_log) as its basis, making one key change: it guarantees that segments are sorted by key. It also provides various optimizations to help with reads.

### Main operations

The two primary operations are those used for the storing and retrieval of key value pairs: `db_set` and `db_get`. They store and retrieve key values pairs. If a key's value is overwritten, the most recent value is always read back. They have been modified to make reads quicker and to make the application more resilient in the face of system failure.

### Memtable

One of the main data structures in the LSM-Tree is the Memtable. This is an in-memory RedBlack tree (i.e. an balanced binary search tree) of nodes containing key-value pairs. When writes or reads come in, they always start with the memtable. When it grows past a threshold number of bytes in size, the keys are written in order to a new Sorted String Table (SSTable) segment on disk. This is done to avoid overloading memory. By default, the threshold is set to a megabyte.

Thanks to [stanislavkozlovski](https://github.com/stanislavkozlovski/Red-Black-Tree/blob/master/rb_tree.py) for the tree implementation. I've augmented it and the test suite to support:

- The storage of keys and values (for writes/reads), offsets and segment names (for the index) in nodes
- The ability to update nodes in the tree in place, rather than storing duplicate keys
- An inorder traversal algorithm

### AppendLog

Since the memtable isn't persistent, each write that comes in is also written to an append log on disk. If the system crashes, the log is used to restore the memtable. The AppendLog is a singleton pattern than returns a single filestream pointer to this file, to avoid the expense of opening a new pointer on each write. This has some implications for the tests, since the same instance will persist through each one. The clear() method has been introduced to address this, and to allow the same filestream to be used when the WAL needs to be re-initialized.

### Index

Another RedBlack tree is kept in memory and used as a sparse index. The DB's sparsity factor can be chose by the user, and is used to decide how often a record should be written to the index when they are flushed to disk. The calculation is `frequency = threshold / sparsity_factor`, a higher value of sparsity_factor leads to a denser index. Since the RedBlack tree support floor and ciel lookups, the index can be leveraged to find regions of disk where a key is likely to be found.

### Bloom Filter

Misses on reads are very expensive: the system has to check the memtable, then check every segment on disk only to find that the key isn't there. As the number of segments grows, these misses become too expensive. 

To avoid this pain, a Bloom Filter has been added to the DB. Keys are written to it when they are flushed to disk. This in turn allows the system to check if an incoming read key definitely isn't stored, at which point None is immediately returned. This can make the cost of a miss extremely cheap. There is a chance of developing a false positive, but if the system's expected load and desired false-positive probability is known ahead of time, this will happen infrequently. 

Two methods are used for the Bloom Filter's configuration:

- `set_bloom_filter_num_items`: Set the expected load for the system
- `set_bloom_filter_false_pos_prob`: Set the desired probability of generating a false positive

Note that a low probability will affect write performace, since it makes the BloomFilter's add() operation more expensive. Each of these parameters are needed to generate the bloom filter, so when one changes the entire structure is re-initialized.

### Compaction algorithm

This project does not use the standard compaction algorithm for LSM-Trees. This is mainly due to the fact that the memtable in this project is updated in place, so when it is flushed to disk there is already the guarantee every line in the SSTable will contain a unique key. 

This makes the compaction of individual SSTables on disk redundant. With that said, it is still possible for the same key to be written to multiple different SSTables. To deal with this, the system introduces a new compaction algorithm to reclaim disk space:

1. The compaction algorithm runs right before the memtable is flushed to disk.
2. The algorithm traverses the memtable, using the bloom filter to calculate a set of nodes that are probably already on disk*
3. It parses each segment on disk line by line, dropping records who's that appear in the set

\* Note that keys in the memtable are only written to the bloom filter when they are written to disk.

Once this is over, the memtable can be flushed to a new segment on disk and the keys added to the bloom filter.

By doing this, we largely avoid spending unecessary time doing disk IO for keys that definitely aren't on disk. If the Bloom Filter is properly configured, the probability of a false positive is kept fairly low.

## Testing

Invoke any of:

- `python3 -m unittest test.lsm_tree_tests` (the main test suite)
- `python3 -m unittest test.red_black_tree_tests`
- `python3 -m unittest test.bloom_filter_tests`

## Benchmarking

Invoke `python3 benchmarks/write_benchmark.py` and `python3 benchmarks/read_benchmark.py`

## Notes

Here are some places where this database could be improved:

### Multithreading

The last big hurdle for this project is to parellalize it. The main goal here would be to create dedicated threads for reading, writing and flushing/compaction. 

The first good move would be to trigger the compaction algorithm in a background thread so that writes aren't blocked waiting for it. This problem is especially pressing since the time needed to perform compaction scales with the number of bytes stored on disk. By moving flushing to a background process, the write benchmarks could see a speedup as well, however we always flush a constant amount of data to disk, the net benefit is slightly less and so the optimization should be made after.

The main challenge in parallelizing the app would lie in protecting shared resources. This would include the memtable (we need to be able to read from it while it is being flushed to disk) and the AppendLog (only one writer thread should be able to use it at a time) among other things. For flushing and compaction, we would need to take great care to make sure that reads and writes can still access segments as needed even while they are being modified.

### Testing framework

Unnittest is a bit basic and the tests contain a lot of redundancy. It would be better to spend more time improving code reuse in the tests, or to find a more complete testing **framework**. It would also be nice to see if there isn't a better way to test disk operations. A quick fix would be to extract common logic for groups of tests into functions that can be run as more granular setup/teardown.

### IO connections

The append only log requires that every write to the memtable automatically be added to a write-ahead-log on disk. In a naive approach, this involves opening a new file IO stream for each write, which is extremely expensive and seriously detracts from the performance of the database. Even when using a single file stream pointer, I've noticed that the write benchmarks take a significant hit. It would be good to see if there isn't any other way to speed this operation up, or at least take the load of a writes by spawning another thread.