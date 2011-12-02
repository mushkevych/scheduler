"""
Created on 2011-10-19

@author: Bohdan Mushkevych
"""
from workers.abstract_hadoop_worker import AbstractHadoopWorker

class HadoopAggregatorDriver(AbstractHadoopWorker):
    """Python process that starts Hadoop map/reduce job, supervises its execution and updated unit_of_work"""

    def __init__(self, process_name):
        super(HadoopAggregatorDriver, self).__init__(process_name)
