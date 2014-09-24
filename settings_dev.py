from datetime import datetime

settings = dict(
    process_cwd='/tmp',   # daemonized process working directory, where it can create .cache and other folders

    # created with: sudo rabbitmqctl add_vhost /hadoop
    # set permissions with: sudo rabbitmqctl set_permissions -p /hadoop guest ".*" ".*" ".*"
    mq_host='ci-node-1.mobidiadomain.com',
    mq_user_id='richmond',
    mq_password='richmond',
    # mq_host='127.0.0.1',
    # mq_user_id='guest',
    # mq_password='guest',
    mq_vhost='/',
    mq_port=5672,

    mongodb_host_list=['mongodb://127.0.0.1:27017'],

    perf_ticker_interval=10,                                            # seconds between performance ticker messages
    synergy_start_timeperiod='2014091400',    # precision is process dependent
    # synergy_start_timeperiod=datetime.utcnow().strftime('%Y%m%d%H'),    # precision is process dependent
    debug=True
)
