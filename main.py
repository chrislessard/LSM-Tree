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
        'set_sparsity {value}',
        'activate_bloomfilter',
        'deactivate_bloomfilter',
        'set_bf_num_items {items}',
        'set_bf_false_pos_prob {probability}',
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
            print('Done\n')
        elif cmd[0] == 'set_threshold':
            threshold = int(cmd[1])
            print('\nSetting new threshold ...')
            db.set_threshold(threshold)
            print('Done\n')
        elif cmd[0] == 'set_sparsity':
            sparsity = int(cmd[1])
            db.set_sparsity_factor(sparsity)
            print('\nSet new sparsity factor.\n')
        elif cmd[0] == 'activate_bloomfilter':
            db.activate_bloom_filter()
            print('BloomFilter activated')
        elif cmd[0] == 'deactivate_bloomfilter':
            db.deactivate_bloom_filter()
            print('BloomFilter deactivated')
        elif cmd[0] == 'set_bf_num_items':
            arg = int(cmd[1])
            if arg <= 0:
                print("Invalid option, plase choose a value greater than 0")
            else:
                db.set_bloom_filter_num_items(arg)
                print('Set the BloomFilters expected num items to', arg)
        elif cmd[0] == 'set_bf_false_pos_prob':
            arg = float(cmd[1])
            if arg < 0 or arg > 1:
                print('Invalid option. Please choose a probability between 0 and 1.')
            else:
                db.set_bloom_filter_false_pos_prob()
                print(
                    'Set the BloomFilters desired probability of a false positive to', float(cmd[1]))
        elif cmd[0] == 'help':
            print('\n\t'.join(usage_msg))
        elif cmd[0] == 'exit':
            db.save_metadata()
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()
