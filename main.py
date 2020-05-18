from src.lsm_tree import LSMTree

SEGMENTS_DIRECTORY = 'segments/'
SEGMENT_BASENAME = 'LSMTree-1'
WAL_BASENAME = 'memtable_bkup'

def main():
    '''
    Run the LSMTree interface.
    '''
    usage_msg = [
        'Commands: ',
        'store {key} {data}',
        'get {key}',
        'compact_segments',
        'set_threshold {number of bytes}',
        'help',
        'exit'
    ]

    db = LSMTree(SEGMENT_BASENAME, SEGMENTS_DIRECTORY, WAL_BASENAME)

    print('\n\t'.join(usage_msg))

    while True:
        print('\nEnter a command below. Type "help" to see a list of commands.')
        cmd = input('$ ').lower().split(' ')

        if cmd[0] == 'store':
            key, val = cmd[1], ' '.join(cmd[2:])
            db.db_set(key, val)
            print('Stored', key, 'with value', val)
        elif cmd[0] == 'get':
            key = cmd[1]
            print('Key "' + key + '" has value:', db.db_get(key))
        elif cmd[0] == 'compact_segments':
            print('\nCompacting segments on disk ...')
            db.compact()
            print('Finished compacting segments.')
        elif cmd[0] == 'set_threshold':
            threshold = int(cmd[1])
            print('\nSetting new threshold ...')
            db.set_threshold(threshold)
            print('Done\n')
        elif cmd[0] == 'help':
            print('\n\t'.join(usage_msg))
        elif cmd[0] == 'exit':
            db.save_metadata()
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()