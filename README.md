Synergy Scheduler
=========

Synergy Scheduler tracks its history to 2011. It is a production-grade Job Scheduling System, used for triggering simple jobs and orchestrate execution of complex interdependent data processing clusters.

Synergy Scheduler utilizes Rabbit MQ to communicate with its subsidiaries, so it is a good choice for deployments where part of the system  is remote or machine/location dependent.

Synergy Scheduler is supervising triggering and life-cycle of two types of jobs:

- cron-like jobs govern by timer. They are known to the system as *free-run*
- *managed* jobs that are govern by state machine. Such jobs could have multiple dependencies on other jobs

Here, "job" corresponds to an execution of any system process (for example: Python process, Hadoop map-reduce job, etc) with a set of particular arguments, started and monitored by the Scheduler means.

Each process registered within the Synergy Scheduler could chose state machine to govern its execution.

To start using Synergy Scheduler user would have to write an actual job, register it in the context and define its triggering frequency.


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
1. db, rabbit mq, etc 
