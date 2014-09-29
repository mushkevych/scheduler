Synergy Scheduler
=========

Synergy Scheduler is supervising triggering and life-cycle of two types of jobs:

- cron-like jobs govern by timer. They are known to the system as *free-run*
- *managed* jobs that are govern by state machine. Such jobs could have multiple dependencies on other jobs

Here, **job** corresponds to any system process (for example: Python process, Hadoop map-reduce job, etc) that is started and monitored by Synergy Scheduler means.

Synergy Scheduler hosts multiple **state-machines**. Each process is allowed to chose its governing state machine, that supervises job's life-cycle: from *STATE_EMBRYO* thru *STATE_IN_PROGRESS* and *STATE_FINAL_RUN* to *STATE_PROCESSED* (or *STATE_SKIPPED* in case of a failure). Most likely that multiple **tasks** (or unit_of_work) will be issued during job's life-span. They also have multiple states and cover task life-cycle: from state *STATE_REQUESTED* thru *STATE_IN_PROGRESS* to either *STATE_PROCESSED* or *STATE_CANCELED* or *STATE_INVALID*.

Synergy Scheduler hides all of this complexity from the user under its hood. In most cases, user would have to write an actual job, register it in the context, define its triggering frequency to be able to run the job under the Synergy Scheduler.


License:
---------
Modified BSD License. Refer to LICENSE for details.


Git repository:
---------
[GitHub project page](https://github.com/mushkevych/scheduler)


Metafile:
---------

    /launch.py            launcher file
    /process_starter.py   utility to start a process in a daemon mode
    /constants.py         configuration management - constants
    /context.py           configuration management - registrar of all known processes
    /settings.py          configuration management - environment-specific settings
    /setup.py             Distutils setup script
    /scripts/             folder contains helper shell scripts
    /synergy/             folder contains Synergy Scheduler egg
    /tests/               folder contains unit test
    /vendors/             folder contains Python libraries required by the project and installed in Python Virtual Environment
    /worker/              folder with actual project's processes (job runners)
    /db/                  root folder for database components - data source manager, data access objects, schema, etc


Wiki Links
---------
[Wiki Home Page](https://github.com/mushkevych/scheduler/wiki)


Os-Level Dependencies
---------
1. linux/unix  
1. python 2.7+  
1. kernel dev headers  
