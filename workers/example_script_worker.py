__author__ = 'Bohdan Mushkevych'

import time


def main(*args):
    secs = 1
    print('Example Script: entered main function body')
    print('Example Script: called with the arguments {0}'.format(args))
    print('Example Script: falling asleep for {0} seconds'.format(secs))
    time.sleep(secs)

# if __name__ == '__main__':
#     main()
