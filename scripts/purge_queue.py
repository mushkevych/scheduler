"""
 @author: Kalpesh Patel, Bohdan Mushkevych
 @date: April 6, 2010
 http://neopatel.blogspot.com/2010/04/rabbitmq-purging-queue.html
"""

import sys
from mq.flopsy import Connection
from system import process_context
from system.process_context import ProcessContext
from tests.base_fixtures import get_field_starting_with


def purge_mq_queue(mq_queue_name):
    conn = Connection()
    chan = conn.connection.channel()
    try:
        n = chan.queue_purge(mq_queue_name)
        print "Purged %s messages from %s queue" % (n, mq_queue_name)
    except Exception:
        print "Unable to purge %s" % mq_queue_name

    chan.close()
    conn.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage 1 - to purge specific queue:\n\tpython purge_queue.py queue_name\n"
        print "Usage 2 - to purge all Synergy queues:\n\tpython purge_queue.py all\n"
        sys.exit(0)

    if sys.argv[1] == 'all':
        processes = get_field_starting_with('PROCESS_', process_context)
        for process_name in processes:
            mq_queue_name = ProcessContext.get_queue(process_name)
            purge_mq_queue(mq_queue_name)
    else:
        mq_queue_name = sys.argv[1]
        purge_mq_queue(mq_queue_name)

