//Created on 2011-02-25
//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('synergy_hadoop');

// *** CLEAN ***
db.scheduler_configuration_collection.drop();

// *** SYSTEM ***
db.createCollection('scheduler_configuration_collection');
db.scheduler_configuration_collection.ensureIndex( {process_name : 1}, {unique: true} );

// *** VERTICAL AGGREGATORS ***
db.scheduler_configuration_collection.insert({'process_name': 'SiteHourlyAggregator', 'interval_seconds': 20});
db.scheduler_configuration_collection.insert({'process_name': 'SiteDailyAggregator', 'interval_seconds': 40});
db.scheduler_configuration_collection.insert({'process_name': 'SiteMonthlyAggregator', 'interval_seconds': 10800});
db.scheduler_configuration_collection.insert({'process_name': 'SiteYearlyAggregator', 'interval_seconds': 21600});

db.scheduler_configuration_collection.insert({'process_name': 'ClientMonthlyAggregator', 'interval_seconds': 10800});
db.scheduler_configuration_collection.insert({'process_name': 'ClientMonthlyAggregator', 'interval_seconds': 21600});
db.scheduler_configuration_collection.insert({'process_name': 'ClientYearlyAggregator', 'interval_seconds': 43200});

db.scheduler_configuration_collection.insert({'process_name': 'AlertDailyWorker', 'interval_seconds': 10800});
db.scheduler_configuration_collection.insert({'process_name': 'GarbageCollectorWorker', 'interval_seconds': 60});

