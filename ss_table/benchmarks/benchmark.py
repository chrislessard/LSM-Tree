import sys, os, timeit, random, string

# ffs python
file_directory = sys.path[0] 
sys.path.insert(1, os.path.dirname(file_directory))
from src import ss_table as s

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

def benchmark_random_reads(db, pairs):
    for key, value in pairs:
        db.db_get(key)

setup = """
from __main__ import s, benchmark_store, benchmark_get, benchmark_random_writes, path
db = s.SSTable('test_file-1', path, 'bkup')
"""

# Benchmark lots of continuous writes
benchmark_store_str = """benchmark_store(db)"""
print('Store benchmark:', timeit.timeit(benchmark_store_str, setup=setup, number=1))

# Benchmark lots of continuous reads
benchmark_get_str = """
db.db_set('daniel', 'lessard')
benchmark_get(db)
"""
print('Get benchmark:', timeit.timeit(benchmark_get_str, setup=setup, number=1))

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
print('Random writes benchmark:', timeit.timeit(benchmark_random_writes_str, setup=random_writes_setup, number=1))

random_reads_setup = setup + """
from __main__ import random_string, benchmark_random_reads
import string, random
num_writes = 100000
strings = [random_string(10) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
for k, v in pairs:
    db.db_set(k, v)
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_random_reads_str = """
benchmark_random_reads(db, pairs)
"""
print('Random reads benchmark:', timeit.timeit(benchmark_random_writes_str, setup=random_reads_setup, number=1))


# Cleanup
for filename in os.listdir(path):
    os.remove(path + filename)

os.rmdir(path)
