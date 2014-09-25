ENVIRONMENT = '%ENVIRONMENT%'

from datetime import datetime

# folder locations, connection properties etc
settings = dict(
    process_prefix='Synergy',  # global prefix that is added to every process name started for synergy-scheduler
    process_cwd='/mnt/tmp',    # daemonized process working directory, where it can create .cache and other folders
    config_file='/etc/synergy-scheduler.conf',
    version='%BUILD_NUMBER%-%SVN_REVISION%',

    log_directory='/mnt/logs/synergy-scheduler/', 
    pid_directory='/mnt/logs/synergy-scheduler/',

    remote_source_host_list=['user@f.q.h.n'],
    remote_source_password={'user@f.q.h.n': '***SSH_PASSWORD***'},
    remote_source_folder='/mnt/remote_folder/',
    bash_runnable_count=5,

    ds_type='mongo_db',
    mongo_db_name='scheduler',
    hadoop_command='/usr/bin/hadoop',
    pig_command='/usr/bin/pig',
    bash_command='/bin/bash',

    debug=False,                                    # if True - logger is given additional "console" adapter
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
    'mq',
    'supervisor',
    'scheduler',
    'system',
    'workers',
]

test_cases = [
    'tests.test_regular_pipeline',
    #'tests.test_decorator',
    'tests.test_publishers_pool',
    'tests.test_discrete_pipeline',
    'tests.test_garbage_collector',
    'tests.test_system_collections',
    'tests.test_system_utils',
    'tests.test_two_level_tree',
    'tests.test_three_level_tree',
    'tests.test_four_level_tree',
    'tests.test_raw_data',
    'tests.test_site_statistics',
    'tests.test_single_session',
    'tests.test_time_helper',
    'tests.test_repeat_timer',
    'tests.test_process_context',
    'tests.test_site_hourly_aggregator',
    'tests.test_tree_node',
    'tests.test_event_clock',
]


def enable_test_mode():
    if settings['under_test']:
        # test mode is already enabled
        return

    test_settings = dict(
        mongo_db_name=settings['mongo_db_name'] + '_test',
        mq_vhost='/unit_test',
        debug=True,
        under_test=True,
        synergy_start_timeperiod=datetime.utcnow().strftime('%Y%m%d%H'),

        # legacy settings
        hadoop_jar='/home/bmushkevych/git/synergy-hadoop/dist/synergy-hadoop-02.jar',
        construction_hosts=['https://***REST_INTERFACE_URL***'],            # production access
        construction_login='***REST_INTERFACE_LOGIN***',                    # production access ONLY
        construction_password='***REST_INTERFACE_PWD***',                   # production access ONLY
        tunnel_host='***SURUS_HOST***',
        tunnel_site_port=9988,  # SURUS PORTS
        bulk_threshold=1024,
    )
    settings.update(test_settings)

    from tests.ut_context import register_processes
    register_processes()
