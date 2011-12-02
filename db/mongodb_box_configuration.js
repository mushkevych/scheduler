//Created on 2011-06-14
//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('synergy_hadoop');

// *** CLEAN ***
db.box_configuration_collection.drop();

// *** SYSTEM ***
db.createCollection('box_configuration_collection');
db.box_configuration_collection.ensureIndex( { box_id : 1}, {unique: true} );

// Development environment
db.box_configuration_collection.insert({'box_id': 'DEV',
    'process_list' : {
        'SiteHourlyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SiteDailyAggregator' : {'state' : 'state_on', 'pid' : null},
        'SingleSessionWorker_00' : {'state' : 'state_on', 'pid' : null},
        'EventStreamGenerator' : {'state' : 'state_on', 'pid' : null},
        'GarbageCollectorWorker' : {'state' : 'state_on', 'pid' : null},
        'Scheduler' : {'state' : 'state_on', 'pid' : null}
    }});

db.box_configuration_collection.insert({'box_id': 'HADOOP',
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
