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

print("Write benchmarks")

### BENCHMARKS

#
#
# Write a single key
#
#
benchmark_execute = """
db.db_set('chris', 'lessard')
"""
print(
    'Write a single key: {}'.format(
        timeit.timeit(benchmark_execute, setup=setup, number=1))
)

#
#
# Write the same key value pair to the DB 100k times
#
#
benchmark_execute = """
for i in range(100000):
    db.db_set('chris', 'lessard')
"""
print(
    'Same key 100k times: {}'.format(
        timeit.timeit(benchmark_execute, setup=setup, number=1))
    )

print('\nUNIQUE KEYS')
print('New segments created:')
#
#
# Write 100k unique key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(100000)
num_writes = 100000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 100K bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, medium threshold
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 100000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

print('\nNo new segments created:')
#
#
# Write 100k random key value pairs, high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(2000000)
num_writes = 100000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 2mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, very high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(4000000)
num_writes = 100000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 4mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)


print('\nNew segments created:')
#
#
# Write 1mil random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 1000000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 1mil unique keys, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

print('\nNo new segments created:')
#
#
# Write 1mil random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(6000000)
num_writes = 1000000
pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 1mil unique keys, threshold 4mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)


print()
print("RANDOM KEY VALUE PAIRS")
#
#
# Write 100k random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(100000)
num_writes = 100000
strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 100K bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
    )

#
#
# Write 100k random key value pairs, threshold = num writes
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 100000
strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(2000000)
num_writes = 100000
strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 2mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, very high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(4000000)
num_writes = 100000
strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 4mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)









print("\nThe same benchmarks, with the Bloom Filter activated")

### BENCHMARKS

#
#
# Write a single key
#
#
benchmark_setup = setup + """
db.activate_bloom_filter()
db.set_bloom_filter_num_items(1)
db.set_bloom_filter_false_pos_prob(0.2)
"""
benchmark_execute = """
db.db_set('chris', 'lessard')
"""
print(
    'Write a single key: {}'.format(
        timeit.timeit(benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write the same key value pair to the DB 100k times
#
#
benchmark_setup= setup + """
db.activate_bloom_filter()
db.set_bloom_filter_num_items(10000)
db.set_bloom_filter_false_pos_prob(0.2)
"""

benchmark_execute = """
for i in range(100000):
    db.db_set('chris', 'lessard')
"""
print(
    'Same key 100k times: {}'.format(
        timeit.timeit(benchmark_execute, setup=benchmark_setup, number=1))
)

print('\nUNIQUE KEYS')
print('New segments created:')
#
#
# Write 100k unique key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(100000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 100K bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, medium threshold
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 100000


db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

print('\nNo new segments created:')
#
#
# Write 100k random key value pairs, high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(2000000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 2mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, very high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(4000000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 100k unique keys, threshold 4mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)


print('\nNew segments created:')
#
#
# Write 1mil random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 1000000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 1mil unique keys, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

print('\nNo new segments created:')
#
#
# Write 1mil random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(6000000)
num_writes = 1000000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

pairs = [(str(k), str(k)) for k in range(100000)]
"""
benchmark_execute = """
for key, val in pairs:
    db.db_set(key, val)
"""
print(
    'Write 1mil unique keys, threshold 4mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)


print()
print("RANDOM KEY VALUE PAIRS")
#
#
# Write 100k random key value pairs, low threshold
#
#
benchmark_setup = setup + """
db.set_threshold(100000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 100K bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, threshold = num writes
#
#
benchmark_setup = setup + """
db.set_threshold(1000000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 1mil bytes: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(2000000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 2mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

#
#
# Write 100k random key value pairs, very high threshold
#
#
benchmark_setup = setup + """
db.set_threshold(4000000)
num_writes = 100000

db.activate_bloom_filter()
db.set_bloom_filter_num_items(num_writes)
db.set_bloom_filter_false_pos_prob(0.2)

strings = [random_string(5) for i in range(50)]
pairs = [(random.choice(strings), random.choice(strings)) for i in range(num_writes)]
"""
benchmark_execute = """
for key, value in pairs:
    db.db_set(key, value)
"""
print(
    'Random pairs 100k times, threshold 4mil: {}'.format(timeit.timeit(
        benchmark_execute, setup=benchmark_setup, number=1))
)

# Cleanup
for filename in os.listdir(path):
    os.remove(path + filename)

os.rmdir(path)
