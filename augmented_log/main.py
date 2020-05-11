from augmented_log import AugmentedLog

BASEPATH = 'segments/'
FILENAME = 'database-1'

def main():
    '''
    Run the database interface.
    '''
    usage_msg = [
        'Commands: ',
        'store {key} {data}',
        'get {key}',
        'compact_segments',
        'load_index',
        'save_index_snapshot {filename}',
        'load_index_snapshot {filename}',
        'set_threshold {number of bytes}',
        'help',
        'exit'
    ]

    db = AugmentedLog(FILENAME, BASEPATH)

    print('\n\t'.join(usage_msg))

    while True:
        print('Enter a command below. Type "help" to see a list of commands.')
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            key, val = cmd[1], ' '.join(cmd[2:])
            db.db_set(key, val)
            print('Stored', key, 'with value', val, '\n')
        elif cmd[0] == 'get':
            key = cmd[1]
            print(db.db_get(key), '\n')
        elif cmd[0] == 'compact_segments':
            print('\nCompacting segments on disk ...')
            db.compact()
            print('Finished compacting segments.')
        elif cmd[0] == 'load_index':
            db.load_index()
        elif cmd[0] == 'save_index_snapshot':
            name = cmd[1]
            db.save_index_snapshot(name)
        elif cmd[0] == 'load_index_snapshot':
            name = cmd[1]
            db.load_index_snapshot(name)
        elif cmd[0] == 'set_threshold':
            threshold = int(cmd[1])
            print('\nSetting new threshold ...')
            db.set_threshold(threshold)
            print('Done\n')
        elif cmd[0] == 'help':
            print('\n\t'.join(usage_msg))
        elif cmd[0] == 'exit':
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()