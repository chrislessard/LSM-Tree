from pathlib import Path
import pickle

FILENAME = 'database.bin'

class Database():
    db_name = None
    index = None

    def __init__(self, database_name):
        self.db_name = database_name
        self.index = {}

    def db_set(self, key, value):
        ''' (self, str, str) => None
        Stores a new key value pair in the DB
        '''
        log = str(key) + ',' + (value) + '\n'
        with open(self.db_name, 'a') as s:
            offset = Path(self.db_name).stat().st_size
            self.index[key] = offset
            s.write(log)

    def db_get(self, key):
        ''' (self, str) => None
        Retrieve the value associated with key in the db
        '''
        offset = self.is_indexed(key)

        val = None
        with open(self.db_name, 'r') as s:
            if offset:
                s.seek(offset)
                k, v = line.split(',')
                val = v
            else:
                for line in s:
                    k, v = line.split(',')
                    if k == key:
                        val = v.strip()
        return val

    def is_indexed(self, key):
        ''' (self, int) => Bool
        Checks whether key is stored in the DB's index
        '''
        return self.index[key] if key in self.index.keys() else None

    def load_index(self):
        ''' (self) => None
        Parses the database file and loads keys into the index. Warning, this is really slow!
        '''
        with open(self.db_name, 'r') as s:
            for line in s:
                k, v = line.split(',')
                self.index[k] = v

    def save_index_snapshot(self, name):
        ''' (self, str) => None
        Saves a pickle dump of the current index to disk, calling the file name.
        '''
        with open(name, 'wb') as snapshot_file_stream:
            pickle.dump(self.index, snapshot_file_stream)

    def load_index_snapshot(self, name):
        ''' (self, str) => None
        Loads a snapshot of the index from disk to memory, using file name.
        '''
        with open(name, 'rb') as snapshot_file_stream:
            self.index = pickle.load(snapshot_file_stream)

def main():
    '''
    Run the database interface.
    '''
    usage_msg = [
        'Please select an option: ',
        'store {key} {data}',
        'get {key}',
        'load_index',
        'save_index_snapshot {filename}',
        'load_index_snapshot {filename}',
        'exit'
    ]

    db = Database(FILENAME)

    while True:
        print('\n\t'.join(usage_msg))
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            key, val = cmd[1], cmd[2]
            db.db_set(key, val)
        elif cmd[0] == 'get':
            key = cmd[1]
            db.db_get(key)
        elif cmd[0] == 'load_index':
            db.load_index()
        elif cmd[0] == 'save_index_snapshot':
            name = cmd[1]
            db.save_index_snapshot(name)
        elif cmd[0] == 'load_index_snapshot':
            name = cmd[1]
            db.load_index_snapshot(name)
        elif cmd[0] == 'exit':
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()