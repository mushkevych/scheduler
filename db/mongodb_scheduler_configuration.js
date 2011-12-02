//Created on 2011-02-25
//@author: Bohdan Mushkevych

conn = new Mongo('localhost:27017');
db = conn.getDB('synergy_hadoop');

// *** CLEAN ***
db.scheduler_configuration_collection.drop();

// *** SYSTEM ***
db.createCollection('scheduler_configuration_collection');
db.scheduler_configuration_collection.ensureIndex( {entry_name : 1}, {unique: true} );

// *** VERTICAL AGGREGATORS ***
db.scheduler_configuration_collection.insert({'entry_name': 'site_hourly_agg', 'interval_seconds': 20, 'type': 'type_vertical_aggregator', 'process_name': 'SiteHourlyAggregator'});
db.scheduler_configuration_collection.insert({'entry_name': 'site_daily_agg', 'interval_seconds': 40, 'type': 'type_vertical_aggregator', 'process_name': 'SiteDailyAggregator'});
db.scheduler_configuration_collection.insert({'entry_name': 'site_monthly_agg', 'interval_seconds': 10800, 'type': 'type_vertical_aggregator', 'process_name': 'SiteMonthlyAggregator'});
db.scheduler_configuration_collection.insert({'entry_name': 'site_yearly_agg', 'interval_seconds': 21600, 'type': 'type_vertical_aggregator', 'process_name': 'SiteYearlyAggregator'});

db.scheduler_configuration_collection.insert({'entry_name': 'client_daily_agg', 'interval_seconds': 10800, 'type': 'type_horizontal_aggregator', 'process_name': 'ClientDailyAggregator'});
db.scheduler_configuration_collection.insert({'entry_name': 'client_monthly_agg', 'interval_seconds': 21600, 'type': 'type_horizontal_aggregator', 'process_name': 'ClientMonthlyAggregator'});
db.scheduler_configuration_collection.insert({'entry_name': 'client_yearly_agg', 'interval_seconds': 43200, 'type': 'type_horizontal_aggregator', 'process_name': 'ClientYearlyAggregator'});

db.scheduler_configuration_collection.insert({'entry_name': 'alert_daily_worker', 'interval_seconds': 10800, 'type': 'type_alert', 'process_name': 'AlertDailyWorker'});
db.scheduler_configuration_collection.insert({'entry_name': 'garbage_collector', 'interval_seconds': 60, 'type': 'type_garbage_collector', 'process_name': 'GarbageCollectorWorker'});

