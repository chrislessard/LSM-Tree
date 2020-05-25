import sys, os, timeit, random, string

# ffs python
file_directory = sys.path[0]
sys.path.insert(1, os.path.dirname(file_directory))
from src import lsm_tree as s

path = file_directory + '/benchmark_segments/'

# Helpers
def random_string(stringLength):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))

setup = """
from __main__ import s, path
db = s.LSMTree('test_file-1', path, 'bkup')
"""

##
## WRITE BENCHMARKS
##

print("Write benchmarks")

# 01. Write the same key value pair to the DB 100k times
benchmark_execute = """
for i in range(100000):
    db.db_set('chris', 'lessard')
"""
print('Same key 100k times:', timeit.timeit(
    benchmark_execute, setup=setup, number=1))

# 02. Write 100k random key value pairs
benchmark_setup = setup + """
from __main__ import random_string
import string, random
num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print('Diff keys 100k times', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))

# 03. Write 100k random key value pairs, with the bloom filter turned on
benchmark_setup = setup + """
from __main__ import random_string
import string, random

db.bf_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
c = """
for key, value in pairs:
    db.db_set(key, value)
"""
print('Diff keys 100k times - bloom filter on', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))

##
## READ BENCHMARKS
##

print()
print("Read benchmarks")

# 01. Get a single key
benchmark_setup = setup + """
db.db_set('daniel', 'lessard')
"""
benchmark_execute = """
db.db_get('daniel')
"""
print('Get Single key:', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))

# 02. Get 100k keys
benchmark_setup = setup + """
from __main__ import random_string
import string, random
num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
for k, v in pairs:
    db.db_set(k, v)
strs = [random.choice(strings) for i in range(num_writes)]
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))

# 03. Get 100k keys, bloom filter on
benchmark_setup = setup + """
from __main__ import random_string
import string, random

db.bf_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
strings = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

strs = [random.choice(strings) for i in range(num_writes)]
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads, bloom filter on', timeit.timeit(
    benchmark_execute, 
    setup=benchmark_setup, 
    number=1))

# 04. Get 100k keys, half aren't present, bloom filter off
benchmark_setup = setup + """
from __main__ import random_string
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
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads, half missing, bloom filter off', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

# 05. Get 100k keys, half aren't present, bloom filter on
benchmark_setup = setup + """
from __main__ import random_string
import string, random

db.bf_active = True
db.set_bloom_filter_num_items(100000)
db.bf_false_pos_prob = 0.2

num_writes = 100000
present_strs = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

# absent strings have shorter length 
absent_strs =  [random_string(5) for i in range(50)]

present = [random.choice(present_strs) for i in range(num_writes // 2)]
absent = [random.choice(absent_strs) for i in range(num_writes // 2)]
strs = present + absent
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads, half missing, bloom filter on', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

# 06. Lots of misses, bloom filter off
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 1000000
present_strs = [random_string(5) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

# absent strings have shorter length 
absent_strs =  [random_string(5) for i in range(50)]

l1 = [random.choice(present_strs) for i in range(num_writes//2)]
l2 = [random.choice(absent_strs) for i in range(num_writes//2)]
strs = l1 + l2
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k misses', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

# 06. Lots of misses, bloom filter on
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 1000000
db.bf_active = True
db.set_bloom_filter_num_items(num_writes)
db.bf_false_pos_prob = 0.1

present_strs = [random_string(5) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

# absent strings have shorter length 
absent_strs = [random_string(5) for i in range(50)]

l1 = [random.choice(present_strs) for i in range(num_writes//2)]
l2 = [random.choice(absent_strs) for i in range(num_writes//2)]
strs = l1 + l2
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k misses, bloom filter on', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

# Cleanup
for filename in os.listdir(path):
    os.remove(path + filename)

os.rmdir(path)
