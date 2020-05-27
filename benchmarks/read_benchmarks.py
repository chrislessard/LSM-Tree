import sys, os, timeit, random, string

file_directory = sys.path[0]
sys.path.insert(1, os.path.dirname(file_directory))
from src import lsm_tree as s

# SETUP
path = file_directory + '/benchmark_segments/'

# Helpers
def random_string(stringLength):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


setup = """
from __main__ import s, path, random_string
import string, random
db = s.LSMTree('test_file-1', path, 'bkup')
"""

# Benchmarks
print("Read benchmarks")

#
#
# Get a single key
#
#
benchmark_setup = setup + """
db.db_set('daniel', 'lessard')
"""
benchmark_execute = """
db.db_get('daniel')
"""
print('Get Single key:', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))

# 
#
# Get 100k random keys
#
#
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

#
#
# Get 100k keys, half aren't present
#
#
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 100000
present_strs = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

absent_strs =  [random_string(5) for i in range(50)]

strs = [random.choice(present_strs) for i in range(num_writes // 2)]
strs += [random.choice(absent_strs) for i in range(num_writes // 2)]
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads, half missing', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

#
#
# Lots of misses, many segments
#
#
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 10000
db.set_threshold(num_writes // 2)
for k in range(num_writes):
    db.db_set(str(k), str(k))

absent_str = 'chris'
"""
benchmark_execute = """
for i in range(10000):
    db.db_get(absent_str)
"""
print('10k misses, lots of segments on disk', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)








print("\nThe same benchmarks with the Bloom Filter turned on:")


#
#
# Get a single key
#
#
benchmark_setup = setup + """
db.set_bloom_filter_num_items(1)
db.set_bloom_filter_false_pos_prob(0.2)

db.db_set('daniel', 'lessard')
"""
benchmark_execute = """
db.db_get('daniel')
"""
print('Get Single key:', timeit.timeit(
    benchmark_execute, setup=benchmark_setup, number=1))


#
#
# Get 100k random keys
#
#
benchmark_setup = setup + """
from __main__ import random_string
import string, random
num_writes = 100000

db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

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

#
#
# Get 100k keys, half aren't present
#
#
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 100000

db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

present_strs = [random_string(10) for i in range(50)]
write_pairs = [(random.choice(present_strs), random.choice(present_strs)) for i in range(num_writes)]
for k, v in write_pairs:
    db.db_set(k, v)

absent_strs =  [random_string(5) for i in range(50)]

strs = [random.choice(present_strs) for i in range(num_writes // 2)]
strs += [random.choice(absent_strs) for i in range(num_writes // 2)]
"""
benchmark_execute = """
for key in strs:
    db.db_get(key)
"""
print('100k random reads, half missing', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

#
#
# Lots of misses, many segments, bloom filter on
#
#
benchmark_setup = setup + """
from __main__ import random_string
import string, random

num_writes = 10000
db.set_threshold(num_writes // 2)

db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

for k in range(num_writes):
    db.db_set(str(k), str(k))

absent_str = 'chris'
"""
benchmark_execute = """
for i in range(10000):
    db.db_get(absent_str)
"""
print('10k misses, lots of segments on disk', timeit.timeit(
    benchmark_execute,
    setup=benchmark_setup,
    number=1)
)

# Cleanup
for filename in os.listdir(path):
    os.remove(path + filename)

os.rmdir(path)
