FILENAME = 'database.bin'

def db_set(key, value):
    '''
    Stores a new key value pair in the DB
    '''

    pass

def db_get(key):
    '''
    Retrieve the value associated with key in the db
    '''
    pass

def main():
    '''
    Run the database interface.
    '''
    usage_msg = 'Please select an option: \n\tstore {key} {data}\n\tget {key}\n\texit'
    while True:
        print(usage_msg)
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            db_set(cmd[1], cmd[2])
        elif cmd[0] == 'get':
            db_get(cmd[1])
        elif cmd[0] == 'exit':
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()