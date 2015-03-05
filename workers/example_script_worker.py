__author__ = 'Bohdan Mushkevych'

import time


def main(*args):
    secs = 1
    print('Example Script: entered main function body')
    print('Example Script: called with the arguments %r' % args)
    print('Example Script: falling asleep for %r seconds' % secs)
    time.sleep(secs)

#if __name__ == '__main__':
#    main()
