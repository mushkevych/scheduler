//Created on 2011-02-25
//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CREATION ***
db.createCollection('single_session');
db.single_session.ensureIndex( { domain_name : 1, "user_profile.session_id" : 1}, {unique: true} );
db.single_session.ensureIndex( { timeperiod : 1 });

// *** SYSTEM ***
db.createCollection('scheduler_configuration');
db.scheduler_configuration.ensureIndex( { process_name : 1}, {unique: true} );

db.createCollection('units_of_work');
db.units_of_work.ensureIndex( { process_name : 1, timeperiod : 1, start_obj_id : 1, end_obj_id : 1}, {unique: true} );

// *** TIME_TABLE COLLECTION ***
db.createCollection('timetable_hourly');
db.timetable_hourly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('timetable_daily');
db.timetable_daily.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('timetable_monthly');
db.timetable_monthly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );

db.createCollection('timetable_yearly');
db.timetable_yearly.ensureIndex( { process_name : 1, timeperiod : 1}, {unique: true} );
