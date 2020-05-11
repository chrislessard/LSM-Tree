from basic_log import BasicLog

BASEPATH = 'segments/'
FILENAME = 'database-1'

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