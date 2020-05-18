# SSTable-POC

To read the full article associated with this codebase, visit [the link here](https://www.notion.so/Implementing-a-basic-SSTable-363b7bbd98674291ba80edd1d61d8a0a).

## Dependencies

You'll need python3 in order to run this code.

## Project use and description

To use the project, cd into the ss_table directory and invoke `python main.py`. The program will create csv files on disk. These contain the persisted records. Metadata and a write-ahead-log will also be created in this directory. Tests and benchmark code use their own directories.

## About

This is a basic implementation of a a Sorted String Table (SSTable). It uses the augmented log as its basis, making one key change: it guarantees that segments are sorted by key. The interface is the same as the AugmentedLog's.

It also introduces a memtable into memory, which is a RedBlack tree of nodes containing key-value pairs. Thanks to [stanislavkozlovski](https://github.com/stanislavkozlovski/Red-Black-Tree/blob/master/rb_tree.py) for the implementation. I've augmented it to support keys and values, as well as support the update operation and provide an inorder traversal of the nodes for the purpose of writing the memtable to disk.

You'll notice when you use the SSTable that segments aren't actively being created like they were for the other two implementations. This is because the memtable is only flushed to disk when the total size of the key/value pairs stored exceeds the segment treshold.

Since the memtable isn't persistent, we back it up to disk on each operation using the standard append-only log, which serves solely the purpose of re-populating the memtable in the event of a system failure.

The merging algorithm implementation is different, since it can leverage the fact that segments are sorted.

## Testing

`cd` into the ss_table directory and invoke `python3 -m unittest test.ss_table_tests`.

## Benchmarking

`cd` into the ss_table directory and invoke `python3 benchmarks/benchmark.py`

## Notes

Here are some places where this proof of concept implementation of an SSTable could be improved:

### Testing framework

Unnittest is a bit basic and the tests contain a lot of redundancy. It would be better to port the tests to a more complete testing framework. It would also be nice to see if there isn't a better way to test disk operations. A quick fix would be to extract common logic for groups of tests into functions that can be run as more granular setup/teardown.

### IO connections

The append only log requires that every write to the memtable automatically be added to a write-ahead-log on disk. In a naive approach, this involves opening a new file IO stream for each write, which is extremely expensive and seriously detracts from the performance of the database. This was the motivation for the singleton class, which can be leveraged to write and flush to disk without incurring the same expense.

The class could also store an open filestream as a class variable. This would avoid some issues around testing, in which the singleton persists throughout different test cases and needs to be cleared manually in setup. I didn't opt for this since it felt a little dirty.

### Adding a bloom filter

A bloom filter is a probabilistic data structure that can efficiently inform us if a key is not in the database. Introducing one would allow the system to quickly determine if a key wasn't present, instead of searching through the memtable and every segment on disk.
