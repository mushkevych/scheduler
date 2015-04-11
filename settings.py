ENVIRONMENT = '%ENVIRONMENT%'

from datetime import datetime

# folder locations, connection properties etc
settings = dict(
    process_prefix='Synergy',  # global prefix that is added to every process name started for synergy-scheduler
    process_cwd='/mnt/tmp',    # daemonized process working directory, where it can create .cache and other folders
    config_file='/etc/synergy.conf',
    version='%BUILD_NUMBER%',

    log_directory='/mnt/logs/synergy-scheduler/', 
    pid_directory='/mnt/logs/synergy-scheduler/',

    remote_source_host_list=['user@f.q.h.n'],
    remote_source_password={'user@f.q.h.n': '***SSH_PASSWORD***'},
    remote_source_folder='/mnt/remote_folder/',
    compute_gzip_md5=True,     # True, if AbstractFileCollector should compute MD5 for every file it processes
    bash_runnable_count=5,

    ds_type='mongo_db',
    mongo_db_name='scheduler',
    hadoop_command='/usr/bin/hadoop',
    pig_command='/usr/bin/pig',
    bash_command='/bin/bash',

    debug=False,                                    # if True - logger is given additional "console" adapter
    under_test=False
)

# Update current dict with the environment-specific settings
try:
    overrides = __import__('settings_' + ENVIRONMENT)
except:
    overrides = __import__('settings_dev')
settings.update(overrides.settings)


# Modules to test and verify (pylint/pep8)
testable_modules = [
    'synergy',
    'db',
    'workers',
]

test_cases = [
    'tests.test_process_hierarchy',
    'tests.test_abstract_state_machine',
    'tests.test_state_machine_continuous',
    'tests.test_state_machine_discrete',
    # 'tests.test_state_machine_simple_discrete',
    'tests.test_publishers_pool',
    'tests.test_garbage_collector',
    'tests.test_system_utils',
    'tests.test_multi_level_tree',
    'tests.test_raw_data',
    'tests.test_site_statistics',
    'tests.test_single_session',
    'tests.test_time_helper',
    'tests.test_repeat_timer',
    'tests.test_process_starter',
    'tests.test_site_hourly_aggregator',
    'tests.test_site_daily_aggregator',
    'tests.test_site_monthly_aggregator',
    'tests.test_site_yearly_aggregator',
    'tests.test_tree_node',
    'tests.test_event_clock',
]


def enable_test_mode():
    if settings['under_test']:
        # test mode is already enabled
        return

    test_settings = dict(
        mongo_db_name=settings['mongo_db_name'] + '_test',
        # mq_vhost='/unit_test',
        debug=True,
        under_test=True,
        synergy_start_timeperiod=datetime.utcnow().strftime('%Y%m%d%H'),

        # test suite settings
        hadoop_jar='/home/bmushkevych/git/synergy-hadoop/dist/synergy-hadoop-02.jar',
        construction_hosts=['https://***REST_INTERFACE_URL***'],            # production access
        construction_login='***REST_INTERFACE_LOGIN***',                    # production access ONLY
        construction_password='***REST_INTERFACE_PWD***',                   # production access ONLY
        tunnel_host='***SURUS_HOST***',
        tunnel_site_port=9988,  # SURUS PORTS
        bulk_threshold=1024,
    )
    settings.update(test_settings)

    # it is safe to import global settings at this point
    # since the environment-specific settings has been loaded above
    from synergy.conf import settings as global_settings
    global_settings.settings.update(test_settings)

    from tests.ut_context import register_processes
    register_processes()
