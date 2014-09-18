//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CLEAN ***
db.box_configuration.drop();
db.scheduler_managed_entry.drop();
//db.scheduler_freerun_entries.drop();

// *** CREATE ***
db.createCollection('box_configuration');
db.box_configuration.ensureIndex( { box_id : 1}, {unique: true} );

db.createCollection('scheduler_managed_entry');
db.scheduler_managed_entry.ensureIndex( {process_name : 1}, {unique: true} );

db.createCollection('scheduler_freerun_entry');
db.scheduler_freerun_entry.ensureIndex( { process_name : 1, entry_name : 1}, {unique: true} );

// *** Supervisor Settings ***
// Development environment
db.box_configuration.insert({'box_id': 'DEV',
    'process_list' : {
        'SiteHourlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteDailyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SingleSessionWorker_00' : {'state' : 'state_on', 'pid' : null},
        'EventStreamGenerator' : {'state' : 'state_on', 'pid' : null},
        'GarbageCollectorWorker' : {'state' : 'state_on', 'pid' : null},
        'Scheduler' : {'state' : 'state_on', 'pid' : null}
    }});

db.box_configuration.insert({'box_id': 'HADOOP',
    'process_list' : {
        'EventStreamGenerator' : {'state' : 'state_on', 'pid' : null},
        'SiteHourlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteDailyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteMonthlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteYearlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'GarbageCollectorWorker' : {'state' : 'state_on', 'pid' : null},
        'Scheduler' : {'state' : 'state_on', 'pid' : null},
        'SingleSessionWorker_00' : {'state' : 'state_on', 'pid' : null}
    }});


// *** Scheduler Settings ***
db.scheduler_managed_entry.insert({'process_name': 'SiteHourlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 20', 'pipeline_name': 'continuous', 'process_type': 'type_managed'});
db.scheduler_managed_entry.insert({'process_name': 'SiteDailyAggregator', 'state' : 'state_on', 'trigger_time': 'every 40', 'pipeline_name': 'continuous', 'process_type': 'type_blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'SiteMonthlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 10800', 'pipeline_name': 'discrete', 'process_type': 'type_blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'SiteYearlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 21600', 'pipeline_name': 'discrete', 'process_type': 'type_blocking_children'});

db.scheduler_managed_entry.insert({'process_name': 'ClientDailyAggregator', 'state' : 'state_on', 'trigger_time': 'every 10800', 'pipeline_name': 'simplified_discrete', 'process_type': 'type_blocking_dependencies'});
db.scheduler_managed_entry.insert({'process_name': 'ClientMonthlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 21600', 'pipeline_name': 'simplified_discrete', 'process_type': 'type_blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'ClientYearlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 43200', 'pipeline_name': 'simplified_discrete', 'process_type': 'type_blocking_children'});

db.scheduler_managed_entry.insert({'process_name': 'AlertDailyWorker', 'state' : 'state_on', 'trigger_time': 'every 10800', 'pipeline_name': 'simplified_discrete', 'process_type': 'type_blocking_dependencies'});
db.scheduler_managed_entry.insert({'process_name': 'GarbageCollectorWorker', 'state' : 'state_on', 'trigger_time': 'every 900', 'pipeline_name': 'simplified_discrete', 'process_type': 'type_gc'});

