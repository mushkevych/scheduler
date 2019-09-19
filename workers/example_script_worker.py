__author__ = 'Bohdan Mushkevych'

import time


def main(*args):
    secs = 1
    print('Example Script: entered main function body')
    print(f'Example Script: called with the arguments {args}')
    print(f'Example Script: falling asleep for {secs} seconds')
    time.sleep(secs)

# if __name__ == '__main__':
#     main()
