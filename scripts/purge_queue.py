"""
 @author: Kalpesh Patel, Bohdan Mushkevych
 @date: April 6, 2010
 http://neopatel.blogspot.com/2010/04/rabbitmq-purging-queue.html
"""

import sys
from flopsy.flopsy import Connection

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage python26 purge_queue.py queue_name"
        sys.exit(0)

    mq_queue_name = sys.argv[1]
    conn = Connection()
    chan = conn.connection.channel()
    try:
        n = chan.queue_purge(mq_queue_name)
        print "Purged %s messages from %s queue" % (n, mq_queue_name)
    except Exception as e:
        print "Unable to purge %s" % mq_queue_name

    chan.close()
    conn.close()