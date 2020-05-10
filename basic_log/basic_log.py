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

def main():
    '''
    Run the database interface.
    '''
    usage_msg = [
        'Please select an option: ',
        'store {key} {data}',
        'get {key}',
        'exit'
    ]

    db = BasicLog(FILENAME)

    while True:
        print('\n\t'.join(usage_msg))
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            key, val = cmd[1], ' '.join(cmd[2:])
            db.db_set(key, val)
            print('Stored "', key, '" with value "', val, '"\n')
        elif cmd[0] == 'get':
            key = cmd[1]
            print(db.db_get(key))
        elif cmd[0] == 'exit':
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()
