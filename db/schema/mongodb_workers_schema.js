//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CREATE ***
db.createCollection('single_session');
db.single_session.ensureIndex( { domain_name : 1, "user_profile.session_id" : 1}, {unique: true} );
db.single_session.ensureIndex( { timeperiod : 1 });

db.createCollection('site_hourly');
db.site_hourly.ensureIndex( { domain_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_daily');
db.site_daily.ensureIndex( { domain_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_monthly');
db.site_monthly.ensureIndex( { domain_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_yearly');
db.site_yearly.ensureIndex( { domain_name : 1, timeperiod : 1}, {unique: true} );


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

