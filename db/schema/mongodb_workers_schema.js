//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CREATE ***
db.createCollection('single_session');
db.single_session.ensureIndex( { domain : 1, "user_profile.session_id" : 1}, {unique: true} );
db.single_session.ensureIndex( { timeperiod : 1 });

db.createCollection('alert_daily');
db.alert_daily.ensureIndex( { domain : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_hourly');
db.site_hourly.ensureIndex( { domain : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_daily');
db.site_daily.ensureIndex( { domain : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_monthly');
db.site_monthly.ensureIndex( { domain : 1, timeperiod : 1}, {unique: true} );

db.createCollection('site_yearly');
db.site_yearly.ensureIndex( { domain : 1, timeperiod : 1}, {unique: true} );

db.createCollection('client_daily');
db.client_daily.ensureIndex( { client_id : 1, timeperiod : 1}, {unique: true} );

db.createCollection('client_monthly');
db.client_monthly.ensureIndex( { client_id : 1, timeperiod : 1}, {unique: true} );

db.createCollection('client_yearly');
db.client_yearly.ensureIndex( { client_id : 1, timeperiod : 1}, {unique: true} );


// *** Scheduler Settings ***
db.scheduler_managed_entry.insert({'process_name': 'SiteHourlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 20', 'state_machine_name': 'continuous', 'blocking_type': 'blocking_normal'});
db.scheduler_managed_entry.insert({'process_name': 'SiteDailyAggregator', 'state' : 'state_on', 'trigger_time': 'every 40', 'state_machine_name': 'continuous', 'blocking_type': 'blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'SiteMonthlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 10800', 'state_machine_name': 'discrete', 'blocking_type': 'blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'SiteYearlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 21600', 'state_machine_name': 'discrete', 'blocking_type': 'blocking_children'});

db.scheduler_managed_entry.insert({'process_name': 'ClientDailyAggregator', 'state' : 'state_on', 'trigger_time': 'every 10800', 'state_machine_name': 'simple_discrete', 'blocking_type': 'blocking_dependencies'});
db.scheduler_managed_entry.insert({'process_name': 'ClientMonthlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 21600', 'state_machine_name': 'simple_discrete', 'blocking_type': 'blocking_children'});
db.scheduler_managed_entry.insert({'process_name': 'ClientYearlyAggregator', 'state' : 'state_on', 'trigger_time': 'every 43200', 'state_machine_name': 'simple_discrete', 'blocking_type': 'blocking_children'});

db.scheduler_managed_entry.insert({'process_name': 'AlertDailyWorker', 'state' : 'state_on', 'trigger_time': 'every 10800', 'state_machine_name': 'simple_discrete', 'blocking_type': 'blocking_dependencies'});
db.scheduler_managed_entry.insert({'process_name': 'GarbageCollectorWorker', 'state' : 'state_on', 'trigger_time': 'every 900', 'state_machine_name': 'simple_discrete', 'blocking_type': 'blocking_normal'});

