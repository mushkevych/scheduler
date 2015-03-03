from datetime import datetime

settings = dict(
    process_cwd='/tmp',   # daemonized process working directory, where it can create .cache and other folders

    # created with: sudo rabbitmqctl add_vhost unit_test
    # set permissions with: sudo rabbitmqctl set_permissions -p unit_test guest ".*" ".*" ".*"
    mq_host='127.0.0.1',
    mq_user_id='guest',
    mq_password='guest',
    mq_vhost='/',
    mq_port=5672,
    mq_timeout_sec=None,

    mongodb_host_list=['mongodb://127.0.0.1:27017'],

    perf_ticker_interval=10,                                            # seconds between performance ticker messages
    # synergy_start_timeperiod='2015021400',    # precision is process dependent
    synergy_start_timeperiod=datetime.utcnow().strftime('%Y%m%d%H'),    # precision is process dependent
    debug=True
)
