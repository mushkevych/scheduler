//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CLEAN ***
db.box_configuration.drop();

// *** CREATE ***
db.createCollection('box_configuration');
db.box_configuration.ensureIndex( { box_id : 1}, {unique: true} );

// *** Supervisor Settings ***
db.box_configuration.insert({'box_id': 'DEV',
    'process_list' : {
        'EventStreamGenerator' : {'state' : 'state_on', 'pid' : null},
        'SingleSessionWorker_00' : {'state' : 'state_on', 'pid' : null},
        'SiteHourlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteDailyAggregator' : {'state' : 'state_on', 'pid' : null},
        'GarbageCollectorWorker' : {'state' : 'state_on', 'pid' : null},
        'Scheduler' : {'state' : 'state_on', 'pid' : null}
    }});

db.box_configuration.insert({'box_id': 'QA',
    'process_list' : {
        'EventStreamGenerator' : {'state' : 'state_on', 'pid' : null},
        'SingleSessionWorker_00' : {'state' : 'state_on', 'pid' : null},
        'SiteHourlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteDailyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteMonthlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteYearlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'GarbageCollectorWorker' : {'state' : 'state_on', 'pid' : null},
        'Scheduler' : {'state' : 'state_on', 'pid' : null}
    }});
