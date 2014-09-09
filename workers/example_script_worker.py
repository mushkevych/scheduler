"""
@author: Bohdan Mushkevych
"""
import time


def main(*args):
    secs = 1
    print ('in the main function body')
    print ('called with the arguments %r' % args)
    print ('will sleep now for %r seconds' % secs)
    time.sleep(secs)

#if __name__ == '__main__':
#    main()
