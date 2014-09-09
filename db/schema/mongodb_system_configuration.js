//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CLEAN ***
db.box_configuration.drop();
db.scheduler_configuration.drop();

// *** CREATE ***
db.createCollection('box_configuration');
db.box_configuration.ensureIndex( { box_id : 1}, {unique: true} );

db.createCollection('scheduler_configuration');
db.scheduler_configuration.ensureIndex( {process_name : 1}, {unique: true} );

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
db.scheduler_configuration.insert({'process_name': 'SiteHourlyAggregator', 'state' : 'state_on', 'interval_seconds': 20, 'pipeline_name': '', 'process_type': ''});
db.scheduler_configuration.insert({'process_name': 'SiteDailyAggregator', 'state' : 'state_on', 'interval_seconds': 40});
db.scheduler_configuration.insert({'process_name': 'SiteMonthlyAggregator', 'state' : 'state_on', 'interval_seconds': 10800});
db.scheduler_configuration.insert({'process_name': 'SiteYearlyAggregator', 'state' : 'state_on', 'interval_seconds': 21600});

db.scheduler_configuration.insert({'process_name': 'ClientDailyAggregator', 'state' : 'state_on', 'interval_seconds': 10800});
db.scheduler_configuration.insert({'process_name': 'ClientMonthlyAggregator', 'state' : 'state_on', 'interval_seconds': 21600});
db.scheduler_configuration.insert({'process_name': 'ClientYearlyAggregator', 'state' : 'state_on', 'interval_seconds': 43200});

db.scheduler_configuration.insert({'process_name': 'AlertDailyWorker', 'state' : 'state_on', 'interval_seconds': 10800});
db.scheduler_configuration.insert({'process_name': 'GarbageCollectorWorker', 'state' : 'state_on', 'interval_seconds': 60});

