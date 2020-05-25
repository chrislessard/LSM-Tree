import sys, os, timeit, random, string

# ffs python
file_directory = sys.path[0]
sys.path.insert(1, os.path.dirname(file_directory))
from src import lsm_tree as s

path = file_directory + '/benchmark_segments/'

def random_string(stringLength):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))

def benchmark_store(db):
    for i in range(100000):
        db.db_set('chris', 'lessard')

def benchmark_get(db):
    db.db_get('daniel')

def benchmark_random_writes(db, pairs):
    for key, value in pairs:
        db.db_set(key, value)

def benchmark_random_reads(db, strs):
    for key in strs:
        db.db_get(key)

setup = """
from __main__ import s, benchmark_store, benchmark_get, benchmark_random_writes, path
db = s.LSMTree('test_file-1', path, 'bkup')
"""

##
## WRITE BENCHMARKS
##

print("Write benchmarks")

# 01. Write the same key value pair to the DB 100k times
benchmark_store_str = """benchmark_store(db)"""
print('Same key 100k times:', timeit.timeit(benchmark_store_str, setup=setup, number=1))

# 02. Write 100k random key value pairs
random_writes_setup = setup + """
from __main__ import random_string
import string, random
num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_random_writes_str = """
benchmark_random_writes(db, pairs)
"""
print('Diff keys 100k times', timeit.timeit(
    benchmark_random_writes_str, setup=random_writes_setup, number=1))

# 03. Write 100k random key value pairs, with the bloom filter turned on
random_writes_setup = setup + """
from __main__ import random_string
import string, random

db.bloom_filter_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_random_writes_str = """
benchmark_random_writes(db, pairs)
"""
print('Diff keys 100k times - bloom filter on', timeit.timeit(
    benchmark_random_writes_str, setup=random_writes_setup, number=1))


##
## READ BENCHMARKS
##

print()
print("Read benchmarks")

# 01. Get a single key
benchmark_get_str = """
db.db_set('daniel', 'lessard')
benchmark_get(db)
"""
print('Get Single key:', timeit.timeit(benchmark_get_str, setup=setup, number=1))

# 02. Get 100k keys
random_reads_setup = setup + """
from __main__ import random_string, benchmark_random_reads
import string, random
num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
for k, v in pairs:
    db.db_set(k, v)
strs = [random.choice(strings) for i in range(num_writes)]
"""
benchmark_random_reads_str = """
benchmark_random_reads(db, strs)
"""
print('100k random reads', timeit.timeit(benchmark_random_writes_str, setup=random_reads_setup, number=1))

# 03. Get 100k keys, bloom filter on
random_reads_setup_with_bloomfilter = setup + """
from __main__ import random_string, benchmark_random_reads
import string, random

db.bloom_filter_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
strings = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

strs = [random.choice(strings) for i in range(num_writes)]
"""
benchmark_random_reads_with_bloomfilter_str = """
benchmark_random_reads(db, strs)
"""
print('100k random reads, bloom filter on', timeit.timeit(
    benchmark_random_reads_with_bloomfilter_str, 
    setup=random_reads_setup_with_bloomfilter, 
    number=1)
    )

# 04. Get 100k keys, half aren't present, bloom filter off
random_reads_setup_with_bloomfilter = setup + """
from __main__ import random_string, benchmark_random_reads
import string, random

num_writes = 100000
present_strs = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

# absent strings have shorter length 
absent_strs =  [random_string(5) for i in range(50)]

strs = [random.choice(present_strs) for i in range(num_writes // 2)]
strs += [random.choice(absent_strs) for i in range(num_writes // 2)]
"""
benchmark_random_reads_with_bloomfilter_str = """
benchmark_random_reads(db, strs)
"""
print('100k random reads, half missing, bloom filter off', timeit.timeit(
    benchmark_random_reads_with_bloomfilter_str,
    setup=random_reads_setup_with_bloomfilter,
    number=1)
)

# 05. Get 100k keys, half aren't present, bloom filter on
random_reads_setup_with_bloomfilter = setup + """
from __main__ import random_string, benchmark_random_reads
import string, random

db.bloom_filter_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
present_strs = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

# absent strings have shorter length 
absent_strs =  [random_string(5) for i in range(50)]

strs = [random.choice(present_strs) for i in range(num_writes // 2)]
strs += [random.choice(absent_strs) for i in range(num_writes // 2)]
"""
benchmark_random_reads_with_bloomfilter_str = """
benchmark_random_reads(db, strs)
"""
print('100k random reads, half missing, bloom filter on', timeit.timeit(
    benchmark_random_reads_with_bloomfilter_str,
    setup=random_reads_setup_with_bloomfilter,
    number=1)
)



# Cleanup
for filename in os.listdir(path):
    os.remove(path + filename)

os.rmdir(path)
