//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('scheduler');

// *** CREATE ***
db.createCollection('single_session');
db.single_session.ensureIndex( { domain : 1, session_id : 1}, {unique: true} );
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
