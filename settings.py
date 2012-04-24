ENVIRONMENT='%ENVIRONMENT%'

# folder locations, connection properties etc
settings = dict(
    process_prefix = 'Synergy', # global prefix that is added to every process name started for synergy-data
    process_cwd = '/mnt/tmp',   # daemonized process working directory, where it can create .cache and other folders
    config_file= '/etc/synergy-data.conf',

    log_directory='/mnt/log/synergy-data/', 
    pid_directory='/mnt/log/synergy-data/',

    mq_insist=False,
    mq_queue='default_queue',
    mq_routing_key='default_routing_key',
    mq_exchange='default_exchange',
    mq_durable=True,
    mq_exclusive=False,
    mq_auto_delete=False,
    mq_delivery_mode=2,
    mq_no_ack=False,

    mongo_db_name='synergy_hadoop',
    hadoop_command='/usr/bin/hadoop',
    hadoop_jar='/home/bmushkevych/git/synergy-hadoop/dist/synergy-hadoop-02.jar',

    tunnel_host='***SURUS_HOST***',
    tunnel_site_port=9988, # SURUS PORTS

    bulk_threshold=1024,
    mx_host='0.0.0.0',                              # management extension host (0.0.0.0 opens all interfaces)
    mx_port=5000,                                   # management extension port
    perf_ticker_interval=30,                        # seconds between performance ticker messages
    debug=False,                                    # if True - logger is given additional "concole" adapter
    under_test=False
)

# For now just two level... we can have configs for all deployments
# Need to have a better way for switching these
try:
    overrides = __import__('settings_' + ENVIRONMENT)
except:
    overrides = __import__('settings_dev')
settings.update(overrides.settings)


# Modules to test and verify (pylint/pep8)
testable_modules = [
        'model',
        'event_stream_generator',
        'flopsy',
        'supervisor',
        'scheduler',
        'system',
        'workers',
        ]

test_cases = [
    'tests.test_system_collections',
    'tests.test_raw_data',
    'tests.test_site_statistics',
    'tests.test_single_session_statistics',
    'tests.test_time_helper',
    'tests.test_repeat_timer',
    'tests.test_site_hourly_aggregator',
]

def enable_test_mode():
    test_settings = dict(
            mongo_db_name=settings['mongo_db_name'] + '_test',
            mq_vhost='/unit_test',
            debug=True,
            under_test=True,
            bulk_threshold=512
            )
    settings.update(test_settings)
