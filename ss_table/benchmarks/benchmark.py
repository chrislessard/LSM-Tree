import sys, os, timeit

# ffs python
file_directory = sys.path[0] 
sys.path.insert(1, os.path.dirname(file_directory))
from src import ss_table as s

path = file_directory + '/benchmark_segments/'

def benchmark_store(db):
    for i in range(100000):
        db.db_set('chris', 'lessard')

def benchmark_get(db):
    db.db_get('daniel')

setup = """
from __main__ import s, benchmark_store, benchmark_get, path
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

