from pathlib import Path

FILENAME = 'database.bin'

class Database():
    db_name = None
    index = None

    def __init__(self, database_name):
        self.db_name = database_name
        self.index = {}

    def db_set(self, key, value):
        '''
        Stores a new key value pair in the DB
        '''
        log = str(key) + ',' + (value) + '\n'
        with open(self.db_name, 'a') as s:
            offset = Path(self.db_name).stat().st_size
            self.index[key] = offset
            s.write(log)

    def db_get(self, key):
        '''
        Retrieve the value associated with key in the db
        '''
        offset = self.is_indexed(key)

        with open(self.db_name, 'r') as s:
            if offset:
                s.seek(offset)
                k, v = line.split(',')
                return v
            else:
                for line in s:
                    k, v = line.split(',')
                    if int(k) == key:
                        return v.strip()

    
    def is_indexed(self, key):
        return self.index[key] if key in self.index.keys() else None

def main():
    '''
    Run the database interface.
    '''
    usage_msg = 'Please select an option: \n\tstore {key} {data}\n\tget {key}\n\texit'

    db = Database(FILENAME)

    while True:
        print(usage_msg)
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            db.db_set(cmd[1], cmd[2])
        elif cmd[0] == 'get':
            db.db_get(cmd[1])
        elif cmd[0] == 'exit':
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()