FILENAME = 'database.bin'

class Database():
    db_name = None

    def __init__(self, database_name):
        self.db_name = database_name

    def db_set(self, key, value):
        '''
        Stores a new key value pair in the DB
        '''
        log = str(key) + ',' + (value) + '\n'
        with open(self.db_name, 'a') as s:
            s.write(log)

    def db_get(self, key):
        '''
        Retrieve the value associated with key in the db
        '''
        with open(self.db_name, 'r') as s:
            for line in s:
                k, v = line.split(',')
                if int(k) == key:
                    return v.strip()

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