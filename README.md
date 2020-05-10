# SSTable-POC

A basic implementation of an SSTable for learning purposes.

## Use

With python3 installed on your system, simply invoke:

``` bash
$ python3 main.py
```

Which will provide an interface that is ready to receive the following commands:

**store {key} {data}** - store the key value pair (key, data) as strings in the current segment

**get {key}** - find the most recent value associated with key

**load_index** - parse the current segment, loading all found keys into the in-memory index

**save_index_snapshot {filename}** - save a snapshot of the in-memory index file to disk, under "filename"

**load_index_snapshot {filename}** - load a snapshot of the on-disk index into memory, under "filename"

**help** - print the available commands

**exit** - quit the program

## Testing

To invoke the test suite for the databse, run:

``` bash
$ python3 database_spec.py
```

## Notes

Here are some places where this proof of concept implementation of an SSTable could be improved:

### Segment file contents

Its not the best idea to store the contents of the file as a CSV, this has just been done for simplicity's sake. In practice, a more appropriate approach is to store the length of the key-value string in bytes, followed by the raw string. This would allow us to drop the newlines from the stores, and would probably be a nicer approach in more low-level languages (like C) that don't handle escaping for us.  

Also, under the current implementation, values can't contain a comma since it would require an extra amount of application logic to deal with.

### Memory

Since we are hopping between disk and memory throughout the project, we need be careful with the later. In Python3, the following is considered to be "safe" from a memory-perspective:

``` python
with open("file") as stream:
    for line in stream:
        print(line)
```

We need to be careful not to overload the system memory when implementing the key-value store. *Designing Data Intensive Applications.* recommends that each segment be a few megabytes in size. In general, its a good idea to set the max segment size to be below the limitations of your system's memory. If not, in a worst case scenario, an algorithm liek this one:x

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

since this does not scale if we can't fit the entire segment into memory (which would also be at odds with the index in memory, among other things).

## Miscellaneous improvements

### Testing framework

Unnittest is a bit basic and the tests contain a lot of redundancy. In a proper implementation, it would be better to port the tests to a more complete testing framework. It would also be nice to see if there isn't a better way to test disk operations.

### Parallelism

The merging and compaction algorithms don't support asynchronicity. Under this simplified implementation, the user has to invoke the compact algorithm themself. In the real world, the system would be configured to automatically run this process in the background on a seperate thread. In order to properly support this asynchronicity, the implementation would need to be changed. Under the current implementation, merging involves compacting all the segments, then concatenating the segments in pairs of two until it is no longer possible. This process is slow, and prevents and reading or writing from happening until it is finished. Even if it were to occur in the background, it would pose a problem for the reader/writer thread trying to access the segments being merged. To solve this problem, the merging algorithm would need to write a new segment, allowing the old segments to exist on the disk for use by the reader/writer. Periodically, the new segments would replace the old ones, minimizing waiting time for the reader/writer thread.

More generally, all disk operations would benefit from parallelism. Since disk access time is very slow when compared, it may be a good idea to enqueue a seperate worker thread to perform the write while the main thread continue to look for new reads or writes.