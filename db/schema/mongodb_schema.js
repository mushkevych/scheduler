//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CREATION ***
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

// *** SYSTEM ***
db.createCollection('scheduler_managed_entry');
db.scheduler_managed_entry.ensureIndex( { process_name : 1}, {unique: true} );

db.createCollection('scheduler_freerun_entry');
db.scheduler_freerun_entry.ensureIndex( { process_name : 1, entry_name : 1}, {unique: true} );

db.createCollection('box_configuration');
db.box_configuration.ensureIndex( { box_id : 1}, {unique: true} );

db.createCollection('unit_of_work');
db.unit_of_work.ensureIndex( { process_name : 1, timeperiod : 1, start_obj_id : 1, end_obj_id : 1}, {unique: true} );

// *** TIME_TABLE COLLECTION ***
db.createCollection('job_hourly');
db.job_hourly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('job_daily');
db.job_daily.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('job_monthly');
db.job_monthly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('job_yearly');
db.job_yearly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );
