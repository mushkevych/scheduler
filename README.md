Synergy Scheduler
=========

Production-grade scheduler, supervising jobs completion and track dependencies between job's time-tables

Here, **job** term can correspond to any system process (for example: Python process, Hadoop map-reduce job, etc) that is started and monitored by Synergy Scheduler means.

Synergy Scheduler is in fact a **state-machine**, that supervises job's life-cycle: from *STATE_EMBRYO* thru *STATE_IN_PROGRESS* and *STATE_FINAL_RUN* to *STATE_PROCESSED* (or *STATE_SKIPPED* in case of a failure). Most likely that multiple **tasks** (or unit_of_work) will be issued during job's life-span. They also have multiple states and cover task life-cycle: from state *STATE_REQUESTED* thru *STATE_IN_PROGRESS* to either *STATE_PROCESSED* or *STATE_CANCELED* or *STATE_INVALID*.

Synergy Scheduler hides all of this complexity from the user under its hood. In most cases, user would have to set "synergy_start_timperiod", write actual job and register it in the process_context to be able to run the job and the Synergy Scheduler.


Wiki Links
=========
[Wiki Home Page](https://github.com/mushkevych/scheduler/wiki)


Os-Level Dependencies
=========
1. linux/unix  
1. python 2.7+  
1. kernel dev headers  


Installation Instructions
=========
1. install required dev libraries  
in ubuntu terms: apt-get install python-setuptools python-dev build-essential checkinstall  
1. mongodb: [linux installation instructions](http://docs.mongodb.org/manual/administration/install-on-linux/)  
1. rabbitmq  [linux installation instructions](http://www.rabbitmq.com/download.html)  


