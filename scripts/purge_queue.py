"""
 @author: Kalpesh Patel, Bohdan Mushkevych
 @date: April 6, 2010
 http://neopatel.blogspot.com/2010/04/rabbitmq-purging-queue.html
"""

import sys
from synergy.conf.mq_queue_context import MqQueueContext

from synergy.mq.flopsy import purge_mq_queue
from synergy.conf.process_context import ProcessContext


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage 1 - to purge specific queue:\n\tpython purge_queue.py queue_name\n"
        print "Usage 2 - to purge all Synergy queues:\n\tpython purge_queue.py all\n"
        sys.exit(0)

    if sys.argv[1] == 'all':
        print "Purging process-derived queues..."
        for process_name in ProcessContext.CONTEXT:
            queue_name = ProcessContext.get_queue(process_name)
            if not queue_name:
                continue
            purge_mq_queue(queue_name)

        print "Purging custom queues..."
        for queue_name in MqQueueContext.CONTEXT:
            if not queue_name:
                continue
            purge_mq_queue(queue_name)
    else:
        queue_name = sys.argv[1]
        purge_mq_queue(queue_name)
