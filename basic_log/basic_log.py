from pathlib import Path
import pickle

FILENAME = 'database.bin'

class BasicLog():
    db_name = None

    def __init__(self, database_name):
        self.db_name = database_name

    def db_set(self, key, value):
        ''' (self, str, str) => None
        Stores a new key value pair in the DB
        '''
        log = str(key) + ',' + (value) + '\n'
        with open(self.db_name, 'a') as s:
            s.write(log)

    def db_get(self, key):
        ''' (self, str) => None
        Retrieve the value associated with key in the db
        '''
        val = None
        with open(self.db_name, 'r') as s:
            for line in s:
                k, v = line.split(',')
                if k == key:
                    val = v.strip()
        return val

def benchmark_store(db):
    for i in range(10000):
        db.db_set('chris', 'lessard')

def benchmark_get(db):
    db.db_get('chris')

if __name__ == "__main__":
    # main()

    import timeit
    db = BasicLog(FILENAME)

    for i in range(500000):
        db.db_set('chris', 'lessard')

    print(timeit.timeit("benchmark_store(db)", setup="from __main__ import benchmark_store", number=1))

    db.db_set('charles', 'lessard')

    print(timeit.timeit("benchmark_get(db)", setup="from __main__ import benchmark_store", number=1))
