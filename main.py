from src.lsm_tree import LSMTree

SEGMENTS_DIRECTORY = 'segments/'
SEGMENT_BASENAME = 'LSMTree-1'
WAL_BASENAME = 'memtable_bkup'

def main():
    '''
    Run the LSMTree interface.
    '''
    usage_msg = [
        ['Commands: ', 'Explanation'],
        ['store {key} {data}', 'Store the key value vair in the DB'],
        ['get {key}', 'Retrieve the value for key. Returns None if it doesnt exist'],
        ['\n\tConfiguration:', ''],
        ['set_threshold {number of bytes}', 'Set the threshold for the size of the memtable in bytes'],
        ['set_sparsity {value}', 'Set the sparsity factor for the DBs index'],
        ['set_bf_num_items {items}', 'Set the number of expected items to be stored in the Bloom Filter. Warning: this overrides it.'],
        ['set_bf_false_pos_prob {probability}', 'Set the desired false positive probability for the Bloom Filter. Warning: this overrides it.'],
        ['', ''],
        ['help', 'Print the usage message'],
        ['exit', 'Quit the program. Your instance will be saved to disk.']
    ]

    db = LSMTree(SEGMENT_BASENAME, SEGMENTS_DIRECTORY, WAL_BASENAME)

    for row in usage_msg:
        print("\t{: <40}{: <10}".format(*row))

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
        elif cmd[0] == 'set_threshold':
            arg = int(cmd[1])

            print('\nSet new threshold to {}'.format(arg))
            db.set_threshold(arg)
        elif cmd[0] == 'set_sparsity':
            arg = int(cmd[1])

            db.set_sparsity_factor(arg)
            print('\nSet new sparsity factor to {}'.format(cmd[1]))
        elif cmd[0] == 'set_bf_num_items':
            arg = int(cmd[1])

            if arg <= 0:
                print("Invalid option, plase choose a value greater than 0")
            else:
                db.set_bloom_filter_num_items(arg)
                print('Set the BloomFilters expected num items to {}'.format(arg))
        elif cmd[0] == 'set_bf_false_pos_prob':
            arg = float(cmd[1])

            if arg < 0 or arg > 1:
                print('Invalid option. Please choose a probability between 0 and 1.')
            else:
                db.set_bloom_filter_false_pos_prob(arg)
                print('Set the BloomFilters desired probability of a false positive to', arg)
        elif cmd[0] == 'help':
            for row in usage_msg:
                print("\t{: <40} {: <10}".format(*row))
        elif cmd[0] == 'exit':
            db.save_metadata()
            break
        else:
            print('Invalid command.')

if __name__ == "__main__":
    main()
