Synergy Scheduler
=========

[![PyPI version](https://img.shields.io/pypi/v/synergy_scheduler.svg)](https://pypi.python.org/pypi/synergy_scheduler)
[![Build Status](https://travis-ci.org/mushkevych/scheduler.svg?branch=master)](https://travis-ci.org/mushkevych/scheduler)

Synergy Scheduler is a production-grade Job Scheduling System. It breaks time into intervals, 
associates every interval with a job and makes sure that no job is missed and that each job is completed in the right order.  

Common use-cases:

- wait with starting the **daily** jobs until all nested **hourly** are successfully finished
- mark **daily** and other dependant jobs for reprocessing, should an **hourly** be manually triggered for reprocessing
- run a job every 30 minutes *or* two times per day on Tue, Thu and Fri

Synergy Scheduler use of Rabbit MQ for communication with subsidiaries makes it a good choice for deployments where part of the system is remote or machine/location dependent.

Synergy Scheduler is supervising triggering and life-cycle for two types of jobs:

- cron-like jobs govern by timer. They are known to the system as **free-run**
- **managed** jobs that are govern by state machine. Such jobs could have multiple dependencies on other jobs

Here, term **job** corresponds to an execution of any system process (for example: Python process, Hadoop map-reduce job, etc) with a set of particular arguments, started and monitored by the Synergy Scheduler means.

Each process registered within the Synergy Scheduler could chose state machine to govern its execution.

To start using Synergy Scheduler user would have to write an actual job and register it within the context.


License:
---------

[BSD 3-Clause License.](http://en.wikipedia.org/wiki/BSD_licenses#3-clause_license_.28.22Revised_BSD_License.22.2C_.22New_BSD_License.22.2C_or_.22Modified_BSD_License.22.29)
Refer to LICENSE for details.


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
    /scripts/             folder contains shell helper scripts
    /synergy/             folder contains Synergy Scheduler egg
    /tests/               folder contains unit test
    /vendors/             folder contains Python libraries required by the project and installed in Python Virtual Environment
    /worker/              folder contains illustration suite workers (job runners)  
    /db/                  root folder for illustration suite database components - models, dao, schema


Wiki Links
---------
[Wiki Home Page](https://github.com/mushkevych/scheduler/wiki)


Os-Level Dependencies
---------
1. linux/unix  
1. python 2.7+ / 3.4+
1. mongo db, rabbit mq
