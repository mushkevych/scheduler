settings = dict(
    mq_timeout_sec=300.0,
    mq_durable=True,
    mq_exclusive=False,
    mq_auto_delete=False,
    mq_delivery_mode=2,
    mq_no_ack=False,

    mx_host='0.0.0.0',                              # management extension host (0.0.0.0 opens all interfaces)
    mx_port=5000,                                   # management extension port
    mx_children_limit=168,                          # maximum number of children at any given level returned by MX
    perf_ticker_interval=60,                        # seconds between performance ticker messages
)
