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

Its not a good idea to store the contents of the file as a csv, this has just been done for simplicity's sake. Under the current implementation it means that values can't contain a comma, and it would require an annoying amount of application logic to deal with this.

In practice, a more appropriate approach is to

### Memory

Since we are hoping between disk and memory throughout the project, we need be careful with the later. In Python3, the following is considered to be "safe" from a memory-perspective:

``` python
with open("file") as stream:
    for line in stream:
        print(line)
```

We need to be careful not to overload the system memory when implementing the key-value store. In general, its a good idea to set the max segment size to be below the limitations of your system's memory. If not, in a worst case scenario, an algorithm liek this one:

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
