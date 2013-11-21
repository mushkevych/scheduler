//Created on 2011-02-25
//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('synergy_hadoop');

// *** CREATION ***
db.createCollection('single_session_collection');
db.single_session_collection.ensureIndex( { domain_name : 1, "user_profile.session_id" : 1}, {unique: true} );
db.single_session_collection.ensureIndex( { timestamp : 1 });

// *** SYSTEM ***
db.createCollection('scheduler_configuration_collection');
db.scheduler_configuration_collection.ensureIndex( { process_name : 1}, {unique: true} );

db.createCollection('units_of_work_collection');
db.units_of_work_collection.ensureIndex( { process_name : 1, timestamp : 1, start_obj_id : 1, end_obj_id : 1}, {unique: true} );

// *** TIME_TABLE COLLECTION ***
db.createCollection('timetable_hourly_collection');
db.timetable_hourly_collection.ensureIndex( { process_name : 1, timestamp : 1}, {unique: true} );

db.createCollection('timetable_daily_collection');
db.timetable_daily_collection.ensureIndex( { process_name : 1, timestamp : 1}, {unique: true} );

db.createCollection('timetable_monthly_collection');
db.timetable_monthly_collection.ensureIndex( { process_name : 1, timestamp : 1}, {unique: true} );

db.createCollection('timetable_yearly_collection');
db.timetable_yearly_collection.ensureIndex( { process_name : 1, timestamp : 1}, {unique: true} );
