Synergy Scheduler
=========

Production-grade scheduler, supervising jobs completion and track dependencies between job's time-tables

Here, **job** term can correspond to any system process (for example: Python process, Hadoop map-reduce job, etc) that is started and monitored by Synergy Scheduler means.

Synergy Scheduler is in fact a **state-machine**, that supervises job's life-cycle: from *STATE_EMBRYO* thru *STATE_IN_PROGRESS* and *STATE_FINAL_RUN* to *STATE_PROCESSED* (or *STATE_SKIPPED* in case of a failure). Most likely that multiple **tasks** (or unit_of_work) will be issued during job's life-span. They also have multiple states and cover task life-cycle: from state *STATE_REQUESTED* thru *STATE_IN_PROGRESS* to either *STATE_PROCESSED* or *STATE_CANCELED* or *STATE_INVALID*.

Synergy Scheduler hides all of this complexity from the user under its hood. In most cases, user would have to set "synergy_start_timperiod", write actual job and register it in the process_context to be able to run the job and the Synergy Scheduler.


License:
---------
BSD license. Refer to LICENSE for details.


Git repository:
---------
[GitHub project page](https://github.com/mushkevych/scheduler)


Metafile:
---------

    /launch.py            launcher file
    /process_starter.py   utility to start worker in daemon mode  
    /settings.py          configuration management  
    /scripts/             folder contains shell scripts  
    /system/              folder contains system-level modules  
    /tests/               folder contains unit test  
    /vendors/             folder contains Python libraries required for the project and installed in Python Virtual Environment  
    /worker/              folder of actual project's code  
    /db/                  root folder for database components - data source manager, data access objects, schema, etc
    /mq/                  module provides RabbitMq Connection, Consumer, Publisher, etc functionality
    /mx/                  module provides HTML front-end for the Synergy Scheduler
    /scheduler/           folder contains Synergy Scheduler and related components
    /supervisor/          folder contains module that starts/stops Scheduler processes
    /event_stream_generator/  legacy tool. folder contains test stream generator.


Wiki Links
---------
[Wiki Home Page](https://github.com/mushkevych/scheduler/wiki)


Os-Level Dependencies
---------
1. linux/unix  
1. python 2.7+  
1. kernel dev headers  
